"""Event-driven historical backtesting engine.

Replays historical OHLCV candles through the *exact same* strategy engines,
execution gateway, and bracket monitor used live — nothing is reimplemented,
so intrabar SL/TP touches, same-candle pessimistic resolution, gap fills at
the open, slippage, fees, and the operational risk guard all behave
identically to paper/live trading. The only substitution is the transport:
`HistoricalTransport` reads candles from a local CSV file or a cached JSON
payload instead of a live network stream and feeds them sequentially,
mimicking a real clock.

`run_backtest` is the composition helper: it wires a fresh engine (momentum or
swing), an in-memory trade recorder, a deterministic mock gateway, and an
optional `RiskGuard` together, replays the window, and folds everything into a
`BacktestResult` (total return %, win rate %, profit factor, max peak-to-trough
drawdown %, plus the full equity curve).
"""

from __future__ import annotations

import asyncio
import csv
import json
import random
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend.config import ConfigStore
from backend.data_pipeline import MockOHLCVGenerator
from backend.execution_gateway import MockExecutionGateway
from backend.models import OHLCVBar, OpenPositionRecord, Timeframe, TradeRecord
from backend.risk_manager import RiskManager
from backend.strategies.momentum_engine import MomentumEngine
from backend.strategies.swing_engine import SwingEngine
from backend.utils.risk_guard import RiskGuard

STRATEGY_TIMEFRAMES: dict[str, Timeframe] = {
    "momentum": Timeframe.ONE_MINUTE,
    "swing": Timeframe.FOUR_HOUR,
}


class BacktestError(Exception):
    """Raised for unusable historical data or invalid backtest parameters."""


def _parse_timestamp(raw: Any) -> datetime:
    """Accepts epoch seconds/millis or ISO-8601 strings; always returns UTC."""
    if isinstance(raw, (int, float)):
        seconds = float(raw) / 1000.0 if raw > 1e12 else float(raw)
        return datetime.fromtimestamp(seconds, tz=timezone.utc)
    text = str(raw).strip()
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise BacktestError(f"unparseable timestamp: {raw!r}") from exc
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


class HistoricalTransport:
    """Historical candle source: local CSV / cached JSON instead of a stream.

    Bars are sorted by timestamp and can be windowed to [start, end]. The
    `stream()` async generator releases them strictly sequentially — one bar
    per loop tick (optionally paced by `tick_seconds` to mimic a real clock;
    0 replays as fast as possible, which is what tests and the API use).
    """

    def __init__(self, bars: Sequence[OHLCVBar]) -> None:
        self.bars: list[OHLCVBar] = sorted(bars, key=lambda b: b.timestamp)

    @classmethod
    def from_csv(cls, path: str | Path, symbol: str, timeframe: Timeframe) -> HistoricalTransport:
        """Reads `timestamp,open,high,low,close,volume` rows (header required)."""
        file_path = Path(path)
        if not file_path.exists():
            raise BacktestError(f"historical CSV not found: {file_path}")
        bars: list[OHLCVBar] = []
        with file_path.open(newline="") as handle:
            reader = csv.DictReader(handle)
            required = {"timestamp", "open", "high", "low", "close"}
            if reader.fieldnames is None or not required.issubset({f.lower() for f in reader.fieldnames}):
                raise BacktestError(f"CSV must have columns {sorted(required)} (+ optional volume)")
            for row in reader:
                normalized = {key.lower(): value for key, value in row.items() if key is not None}
                try:
                    bars.append(
                        OHLCVBar(
                            symbol=symbol,
                            timestamp=_parse_timestamp(normalized["timestamp"]),
                            timeframe=timeframe,
                            open=float(normalized["open"]),
                            high=float(normalized["high"]),
                            low=float(normalized["low"]),
                            close=float(normalized["close"]),
                            volume=int(float(normalized.get("volume") or 0)),
                        )
                    )
                except (KeyError, TypeError, ValueError) as exc:
                    raise BacktestError(f"malformed CSV row {row!r}: {exc}") from exc
        if not bars:
            raise BacktestError(f"historical CSV is empty: {file_path}")
        return cls(bars)

    @classmethod
    def from_json(cls, payload: str | Path | list[dict[str, Any]], symbol: str, timeframe: Timeframe) -> HistoricalTransport:
        """Accepts a cached JSON payload: a list of candle dicts
        (`{timestamp, open, high, low, close, volume}`), or a path/string of it."""
        data: Any = payload
        if isinstance(payload, Path):
            data = json.loads(payload.read_text())
        elif isinstance(payload, str):
            data = json.loads(Path(payload).read_text()) if Path(payload).exists() else json.loads(payload)
        if not isinstance(data, list) or not data:
            raise BacktestError("JSON payload must be a non-empty list of candle objects")
        bars: list[OHLCVBar] = []
        for row in data:
            try:
                bars.append(
                    OHLCVBar(
                        symbol=symbol,
                        timestamp=_parse_timestamp(row["timestamp"]),
                        timeframe=timeframe,
                        open=float(row["open"]),
                        high=float(row["high"]),
                        low=float(row["low"]),
                        close=float(row["close"]),
                        volume=int(float(row.get("volume") or 0)),
                    )
                )
            except (KeyError, TypeError, ValueError) as exc:
                raise BacktestError(f"malformed JSON candle {row!r}: {exc}") from exc
        return cls(bars)

    @classmethod
    def synthetic(
        cls,
        symbol: str,
        timeframe: Timeframe,
        start_time: datetime,
        end_time: datetime,
        *,
        seed: int | None = None,
        start_price: float = 100.0,
    ) -> HistoricalTransport:
        """Deterministic seeded random-walk history for the given window —
        the data-seeding fallback when no CSV/JSON exists for a symbol."""
        if end_time <= start_time:
            raise BacktestError("end_time must be after start_time")
        resolved_seed = seed if seed is not None else abs(hash(symbol)) % (2**32)
        generator = MockOHLCVGenerator(symbol, start_price=start_price, seed=resolved_seed)
        step = 60 if timeframe is Timeframe.ONE_MINUTE else 4 * 3600
        bars: list[OHLCVBar] = []
        cursor = start_time
        max_bars = 100_000  # hard bound: a year of 1m bars fits comfortably
        while cursor <= end_time and len(bars) < max_bars:
            bars.append(generator.next_bar(cursor, timeframe))
            cursor = datetime.fromtimestamp(cursor.timestamp() + step, tz=timezone.utc)
        return cls(bars)

    def window(self, start_time: datetime | None, end_time: datetime | None) -> list[OHLCVBar]:
        selected = self.bars
        if start_time is not None:
            selected = [b for b in selected if b.timestamp >= start_time]
        if end_time is not None:
            selected = [b for b in selected if b.timestamp <= end_time]
        return selected

    async def stream(
        self,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        *,
        tick_seconds: float = 0.0,
    ) -> AsyncIterator[OHLCVBar]:
        for bar in self.window(start_time, end_time):
            yield bar
            await asyncio.sleep(tick_seconds)


class BacktestRecorder:
    """In-memory `TradePersistence`: captures everything the engine books."""

    def __init__(self) -> None:
        self.trades: list[TradeRecord] = []
        self.equity_curve: list[tuple[datetime, float]] = []
        self.open_positions: list[OpenPositionRecord] = []

    async def record_trade(self, trade: TradeRecord) -> None:
        self.trades.append(trade)

    async def record_equity_snapshot(self, engine_type: str, timestamp: datetime, equity: float) -> None:
        self.equity_curve.append((timestamp, equity))

    async def record_open_position(self, position: OpenPositionRecord) -> None:
        self.open_positions.append(position)

    async def clear_open_position(self, engine_type: str, asset_ticker: str) -> None:
        self.open_positions = [
            p for p in self.open_positions if not (p.engine_type == engine_type and p.asset_ticker == asset_ticker)
        ]

    async def list_open_positions(self, engine_type: str | None = None) -> list[OpenPositionRecord]:
        if engine_type is None:
            return list(self.open_positions)
        return [p for p in self.open_positions if p.engine_type == engine_type]


@dataclass(slots=True)
class BacktestResult:
    """Aggregated performance statistics for one historical replay."""

    strategy: str
    symbol: str
    start_time: datetime
    end_time: datetime
    initial_capital: float
    bars_replayed: int
    trades: list[TradeRecord] = field(default_factory=list)
    equity_curve: list[tuple[datetime, float]] = field(default_factory=list)
    risk_guard_status: dict[str, Any] | None = None

    # --- derived metrics -------------------------------------------------------

    @property
    def net_pnl(self) -> float:
        return sum(trade.net_profit for trade in self.trades)

    @property
    def total_return_pct(self) -> float:
        return (self.net_pnl / self.initial_capital) * 100.0 if self.initial_capital else 0.0

    @property
    def win_rate_pct(self) -> float | None:
        if not self.trades:
            return None
        wins = sum(1 for trade in self.trades if trade.net_profit > 0)
        return wins / len(self.trades) * 100.0

    @property
    def profit_factor(self) -> float | None:
        """Gross gains / gross losses. None with no trades; inf-free: a run
        with gains and zero losses reports the gains against a 0.01 floor is
        wrong — instead it returns None and callers show the win rate."""
        gross_gains = sum(t.net_profit for t in self.trades if t.net_profit > 0)
        gross_losses = -sum(t.net_profit for t in self.trades if t.net_profit < 0)
        if gross_losses == 0:
            return None if gross_gains == 0 else float("inf")
        return gross_gains / gross_losses

    @property
    def max_drawdown_pct(self) -> float:
        """Max peak-to-trough drawdown of account equity (capital + booked PnL)."""
        equity = self.initial_capital
        peak = equity
        max_dd = 0.0
        for _timestamp, engine_equity in self.equity_curve:
            equity = self.initial_capital + engine_equity
            peak = max(peak, equity)
            if peak > 0:
                max_dd = max(max_dd, (peak - equity) / peak)
        return max_dd * 100.0

    def as_dict(self, *, max_curve_points: int = 500) -> dict[str, Any]:
        curve = self.equity_curve
        if len(curve) > max_curve_points:  # downsample for the API payload
            step = len(curve) / max_curve_points
            curve = [curve[int(i * step)] for i in range(max_curve_points)] + [curve[-1]]
        profit_factor = self.profit_factor
        return {
            "strategy": self.strategy,
            "symbol": self.symbol,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat(),
            "initial_capital": self.initial_capital,
            "bars_replayed": self.bars_replayed,
            "trade_count": len(self.trades),
            "net_pnl": round(self.net_pnl, 4),
            "total_return_pct": round(self.total_return_pct, 4),
            "win_rate_pct": round(self.win_rate_pct, 4) if self.win_rate_pct is not None else None,
            "profit_factor": (
                None if profit_factor is None else ("inf" if profit_factor == float("inf") else round(profit_factor, 4))
            ),
            "max_drawdown_pct": round(self.max_drawdown_pct, 4),
            "bracket_outcomes": {
                status: sum(1 for t in self.trades if t.bracket_status == status)
                for status in ("HIT_SL", "HIT_TP", "TIME_EXITED")
            },
            "equity_curve": [{"timestamp": ts.isoformat(), "equity": eq} for ts, eq in curve],
            "risk_guard": self.risk_guard_status,
        }


class Backtester:
    """Feeds a historical window sequentially through a wired strategy engine."""

    def __init__(
        self,
        engine: MomentumEngine | SwingEngine,
        transport: HistoricalTransport,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        *,
        recorder: BacktestRecorder,
        initial_capital: float = 100_000.0,
        strategy: str | None = None,
        risk_guard: RiskGuard | None = None,
        tick_seconds: float = 0.0,
    ) -> None:
        self.engine = engine
        self.transport = transport
        self.symbol = symbol
        self.start_time = start_time
        self.end_time = end_time
        self.recorder = recorder
        self.initial_capital = initial_capital
        self.strategy = strategy or engine.ENGINE_TYPE
        self.risk_guard = risk_guard
        self.tick_seconds = tick_seconds

    async def run(self) -> BacktestResult:
        bars_replayed = 0
        async for bar in self.transport.stream(self.start_time, self.end_time, tick_seconds=self.tick_seconds):
            await self.engine.on_bar(bar)
            bars_replayed += 1
        if bars_replayed == 0:
            raise BacktestError("no historical bars inside the requested window")
        return BacktestResult(
            strategy=self.strategy,
            symbol=self.symbol,
            start_time=self.start_time,
            end_time=self.end_time,
            initial_capital=self.initial_capital,
            bars_replayed=bars_replayed,
            trades=list(self.recorder.trades),
            equity_curve=list(self.recorder.equity_curve),
            risk_guard_status=self.risk_guard.status() if self.risk_guard is not None else None,
        )


def build_backtester(
    strategy: str,
    symbol: str,
    start_time: datetime,
    end_time: datetime,
    transport: HistoricalTransport,
    *,
    initial_capital: float = 100_000.0,
    risk_guard: RiskGuard | None = None,
    gateway: MockExecutionGateway | None = None,
    seed: int = 7,
) -> Backtester:
    """Wires a fresh engine + recorder + deterministic mock gateway for a replay.

    The gateway defaults to zero latency with a seeded RNG so identical inputs
    reproduce identical results; slippage stays on (realistic fills). Pass a
    `RiskGuard` to also replay the operational guard against history.
    """
    if strategy not in STRATEGY_TIMEFRAMES:
        raise BacktestError(f"unknown strategy {strategy!r}; expected one of {sorted(STRATEGY_TIMEFRAMES)}")
    recorder = BacktestRecorder()
    execution = gateway or MockExecutionGateway(latency_range_ms=(0.0, 0.0), rng=random.Random(seed))
    execution.risk_guard = risk_guard
    risk_manager = RiskManager(total_capital=initial_capital)
    if risk_guard is not None and risk_guard.on_trip is None:
        # A guard trip must flatten: halting the risk manager makes the engine
        # force-close on its next bar via the existing circuit-breaker path.
        risk_guard.on_trip = lambda reason: risk_manager.halt(f"risk_guard: {reason}")

    engine: MomentumEngine | SwingEngine
    if strategy == "momentum":
        engine = MomentumEngine(
            symbol,
            config_store=ConfigStore(),
            risk_manager=risk_manager,
            persistence=recorder,
            gateway=execution,
        )
    else:
        engine = SwingEngine(
            symbol,
            config_store=ConfigStore(),
            risk_manager=risk_manager,
            persistence=recorder,
            gateway=execution,
        )
    return Backtester(
        engine,
        transport,
        symbol,
        start_time,
        end_time,
        recorder=recorder,
        initial_capital=initial_capital,
        strategy=strategy,
        risk_guard=risk_guard,
    )


async def run_backtest(
    strategy: str,
    symbol: str,
    start_time: datetime,
    end_time: datetime,
    transport: HistoricalTransport,
    *,
    initial_capital: float = 100_000.0,
    risk_guard: RiskGuard | None = None,
    seed: int = 7,
) -> BacktestResult:
    """One-call convenience: build the wired backtester and replay the window."""
    backtester = build_backtester(
        strategy,
        symbol,
        start_time,
        end_time,
        transport,
        initial_capital=initial_capital,
        risk_guard=risk_guard,
        seed=seed,
    )
    return await backtester.run()
