"""FastAPI server for the multi-engine trading system.

Runs the Momentum (1-minute ORB) and Swing (4-hour trendline) engines as two
independent `asyncio` background workers over the mock data pipeline, so
neither engine's processing blocks the other. Both engines share one
`ConfigStore` (live strategy parameters), one `RiskManager` (capital
allocation + the cross-engine daily drawdown circuit breaker), and one
`Database` (trade/equity persistence) — all wired up here in `create_app`,
the composition root. Signals broadcast live to WebSocket clients and are
also fanned out through the `Notifier` pipeline; trades and equity curves are
read back from the database rather than kept in memory, so they survive a
restart.

`create_app()` is a factory (not a single module-level singleton) so tests
can spin up independent apps, each with its own in-memory database and fresh
risk/config state, without leaking between test runs.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from backend.config import ConfigStore, ConfigValidationError
from backend.data_pipeline import stream_1m_bars, stream_4h_bars
from backend.db import Database
from backend.models import TradeSignal
from backend.notifier import ConsoleNotifier, Notifier, WebhookNotifier
from backend.risk_manager import RiskManager
from backend.schemas import MomentumConfigUpdate, SwingConfigUpdate
from backend.strategies.momentum_engine import MomentumEngine
from backend.strategies.swing_engine import SwingEngine

SYMBOL = "MOCK"
MOMENTUM_TICK_SECONDS = 1.0
SWING_TICK_SECONDS = 2.0
MAX_SIGNALS_RETAINED = 500


def _iso_utc(timestamp: datetime) -> str:
    """Formats a timestamp as UTC ISO-8601. SQLite round-trips datetimes as
    naive; every timestamp this system stores is already UTC, so a naive
    value is reattached that tzinfo before formatting rather than being
    misread as local time by API clients."""
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return timestamp.isoformat()


def _signal_to_json(signal: TradeSignal) -> dict[str, Any]:
    payload = asdict(signal)
    payload["action"] = signal.action.value
    payload["timestamp"] = _iso_utc(signal.timestamp)
    return payload


def _build_notifier() -> Notifier:
    sinks: list[Any] = [ConsoleNotifier()]
    webhook_url = os.environ.get("NOTIFIER_WEBHOOK_URL")
    if webhook_url:
        sinks.append(WebhookNotifier(webhook_url))
    return Notifier(sinks)


class ConnectionManager:
    """Tracks connected WebSocket clients for a single broadcast channel."""

    def __init__(self) -> None:
        self._connections: set[WebSocket] = set()

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        self._connections.add(websocket)

    def disconnect(self, websocket: WebSocket) -> None:
        self._connections.discard(websocket)

    async def broadcast(self, message: dict[str, Any]) -> None:
        stale: list[WebSocket] = []
        for connection in self._connections:
            try:
                await connection.send_json(message)
            except Exception:
                stale.append(connection)
        for connection in stale:
            self.disconnect(connection)


class EngineState:
    """In-memory live signal log for one engine, plus its WS broadcast channel.

    Trades and equity curves are *not* kept here — they're persisted (see
    `Database`) and read back on demand, so they survive a restart.
    """

    def __init__(self) -> None:
        self.signals: list[dict[str, Any]] = []
        self.connections = ConnectionManager()

    def record(self, signal: TradeSignal) -> dict[str, Any]:
        payload = _signal_to_json(signal)
        self.signals.append(payload)
        self.signals[:] = self.signals[-MAX_SIGNALS_RETAINED:]
        return payload


def create_app(
    database_url: str | None = None,
    *,
    momentum_interval_seconds: float = MOMENTUM_TICK_SECONDS,
    swing_interval_seconds: float = SWING_TICK_SECONDS,
) -> FastAPI:
    database = Database(database_url)
    config_store = ConfigStore()
    risk_manager = RiskManager()
    notifier = _build_notifier()
    momentum_state = EngineState()
    swing_state = EngineState()

    async def _run_engine_worker(engine: MomentumEngine | SwingEngine, bar_stream: Any, state: EngineState) -> None:
        async for signal in engine.run(bar_stream):
            payload = state.record(signal)
            await state.connections.broadcast(payload)
            await notifier.notify_signal(signal)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        await database.init()

        momentum_starting_equity = await database.get_latest_equity(MomentumEngine.ENGINE_TYPE)
        swing_starting_equity = await database.get_latest_equity(SwingEngine.ENGINE_TYPE)

        momentum_engine = MomentumEngine(
            SYMBOL,
            config_store=config_store,
            risk_manager=risk_manager,
            persistence=database,
            starting_equity=momentum_starting_equity,
        )
        swing_engine = SwingEngine(
            SYMBOL,
            config_store=config_store,
            risk_manager=risk_manager,
            persistence=database,
            starting_equity=swing_starting_equity,
        )

        momentum_task = asyncio.create_task(
            _run_engine_worker(
                momentum_engine, stream_1m_bars(SYMBOL, interval_seconds=momentum_interval_seconds), momentum_state
            )
        )
        swing_task = asyncio.create_task(
            _run_engine_worker(swing_engine, stream_4h_bars(SYMBOL, interval_seconds=swing_interval_seconds), swing_state)
        )
        try:
            yield
        finally:
            momentum_task.cancel()
            swing_task.cancel()
            for task in (momentum_task, swing_task):
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            await database.dispose()

    app = FastAPI(title="Traderz Multi-Engine Trading System", lifespan=lifespan)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.get("/api/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/api/momentum/signals")
    async def momentum_signals() -> list[dict[str, Any]]:
        return momentum_state.signals

    @app.get("/api/swing/signals")
    async def swing_signals() -> list[dict[str, Any]]:
        return swing_state.signals

    @app.get("/api/momentum/equity")
    async def momentum_equity() -> list[dict[str, Any]]:
        rows = await database.get_equity_curve(MomentumEngine.ENGINE_TYPE)
        return [{"timestamp": _iso_utc(row.timestamp), "equity": row.equity} for row in rows]

    @app.get("/api/swing/equity")
    async def swing_equity() -> list[dict[str, Any]]:
        rows = await database.get_equity_curve(SwingEngine.ENGINE_TYPE)
        return [{"timestamp": _iso_utc(row.timestamp), "equity": row.equity} for row in rows]

    @app.get("/api/momentum/trades")
    async def momentum_trades() -> list[dict[str, Any]]:
        rows = await database.get_trades(MomentumEngine.ENGINE_TYPE)
        return [_trade_to_json(row) for row in rows]

    @app.get("/api/swing/trades")
    async def swing_trades() -> list[dict[str, Any]]:
        rows = await database.get_trades(SwingEngine.ENGINE_TYPE)
        return [_trade_to_json(row) for row in rows]

    @app.get("/api/config")
    async def get_config() -> dict[str, Any]:
        return {"momentum": asdict(config_store.momentum), "swing": asdict(config_store.swing)}

    @app.put("/api/config/momentum")
    async def update_momentum_config(update: MomentumConfigUpdate) -> dict[str, Any]:
        try:
            updated = await config_store.update_momentum(**update.model_dump(exclude_none=True))
        except ConfigValidationError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return asdict(updated)

    @app.put("/api/config/swing")
    async def update_swing_config(update: SwingConfigUpdate) -> dict[str, Any]:
        try:
            updated = await config_store.update_swing(**update.model_dump(exclude_none=True))
        except ConfigValidationError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return asdict(updated)

    @app.get("/api/risk/status")
    async def risk_status() -> dict[str, Any]:
        return risk_manager.status()

    @app.post("/api/system/pause")
    async def pause_system() -> dict[str, Any]:
        risk_manager.pause()
        return risk_manager.status()

    @app.post("/api/system/resume")
    async def resume_system() -> dict[str, Any]:
        risk_manager.resume()
        return risk_manager.status()

    @app.websocket("/ws/momentum")
    async def ws_momentum(websocket: WebSocket) -> None:
        await momentum_state.connections.connect(websocket)
        try:
            while True:
                await websocket.receive_text()
        except WebSocketDisconnect:
            momentum_state.connections.disconnect(websocket)

    @app.websocket("/ws/swing")
    async def ws_swing(websocket: WebSocket) -> None:
        await swing_state.connections.connect(websocket)
        try:
            while True:
                await websocket.receive_text()
        except WebSocketDisconnect:
            swing_state.connections.disconnect(websocket)

    return app


def _trade_to_json(trade: Any) -> dict[str, Any]:
    return {
        "id": trade.id,
        "engine_type": trade.engine_type,
        "asset_ticker": trade.asset_ticker,
        "entry_timestamp": _iso_utc(trade.entry_timestamp),
        "exit_timestamp": _iso_utc(trade.exit_timestamp),
        "entry_price": trade.entry_price,
        "exit_price": trade.exit_price,
        "position_size": trade.position_size,
        "fees": trade.fees,
        "net_profit": trade.net_profit,
    }


app = create_app()
