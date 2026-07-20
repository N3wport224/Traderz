"""FastAPI server for the multi-engine trading system.

Runs the Momentum (1-minute ORB) and Swing (4-hour trendline) engines as two
independent `asyncio` background workers over the mock data pipeline, so
neither engine's processing blocks the other. Signals are broadcast live to
WebSocket clients and also retained in memory for REST snapshot polling.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import asdict
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from backend.data_pipeline import stream_1m_bars, stream_4h_bars
from backend.models import TradeSignal
from backend.strategies.momentum_engine import MomentumEngine
from backend.strategies.swing_engine import SwingEngine

SYMBOL = "MOCK"
MOMENTUM_TICK_SECONDS = 1.0
SWING_TICK_SECONDS = 2.0
MAX_SIGNALS_RETAINED = 500


def _signal_to_json(signal: TradeSignal) -> dict[str, Any]:
    payload = asdict(signal)
    payload["action"] = signal.action.value
    payload["timestamp"] = signal.timestamp.isoformat()
    return payload


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
    """In-memory signal/equity history for one engine, plus its WS broadcast channel."""

    def __init__(self) -> None:
        self.signals: list[dict[str, Any]] = []
        self.equity_curve: list[dict[str, Any]] = []
        self.connections = ConnectionManager()

    def record(self, signal: TradeSignal, equity_curve: list[tuple[Any, float]]) -> dict[str, Any]:
        payload = _signal_to_json(signal)
        self.signals.append(payload)
        self.signals[:] = self.signals[-MAX_SIGNALS_RETAINED:]
        self.equity_curve = [{"timestamp": ts.isoformat(), "equity": round(eq, 4)} for ts, eq in equity_curve]
        return payload


momentum_state = EngineState()
swing_state = EngineState()


async def _run_momentum_worker() -> None:
    engine = MomentumEngine(SYMBOL)
    bar_stream = stream_1m_bars(SYMBOL, interval_seconds=MOMENTUM_TICK_SECONDS)
    async for signal in engine.run(bar_stream):
        payload = momentum_state.record(signal, engine.equity_curve)
        await momentum_state.connections.broadcast(payload)


async def _run_swing_worker() -> None:
    engine = SwingEngine(SYMBOL)
    bar_stream = stream_4h_bars(SYMBOL, interval_seconds=SWING_TICK_SECONDS)
    async for signal in engine.run(bar_stream):
        payload = swing_state.record(signal, engine.equity_curve)
        await swing_state.connections.broadcast(payload)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    momentum_task = asyncio.create_task(_run_momentum_worker())
    swing_task = asyncio.create_task(_run_swing_worker())
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


@app.get("/api/momentum/equity")
async def momentum_equity() -> list[dict[str, Any]]:
    return momentum_state.equity_curve


@app.get("/api/swing/signals")
async def swing_signals() -> list[dict[str, Any]]:
    return swing_state.signals


@app.get("/api/swing/equity")
async def swing_equity() -> list[dict[str, Any]]:
    return swing_state.equity_curve


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
