"""Shared data types used across the data pipeline, strategy engines, and API layer.

Strategy engines depend only on this module, `backend/data_pipeline.py`, and the
injected interfaces defined here (`TradePersistence`) plus the standalone
`backend/config.py` (`ConfigStore`) and `backend/risk_manager.py` (`RiskManager`)
collaborators passed into their constructors. Engines never import each other,
`backend/db.py`, or `backend/main.py` directly — persistence is reached only
through the `TradePersistence` protocol, so engines stay unit-testable without a
real database.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Protocol


class Timeframe(str, Enum):
    ONE_MINUTE = "1m"
    FOUR_HOUR = "4h"


class SignalAction(str, Enum):
    BUY = "buy"
    SELL = "sell"
    SHORT = "short"
    EXIT = "exit"
    ALERT = "alert"
    CIRCUIT_BREAKER = "circuit_breaker_triggered"


@dataclass(frozen=True, slots=True)
class OHLCVBar:
    symbol: str
    timestamp: datetime
    timeframe: Timeframe
    open: float
    high: float
    low: float
    close: float
    volume: int


@dataclass(frozen=True, slots=True)
class TradeSignal:
    engine: str
    symbol: str
    action: SignalAction
    price: float
    timestamp: datetime
    reason: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class TradeRecord:
    """A completed round-trip trade, ready to be persisted by a `TradePersistence`."""

    engine_type: str
    asset_ticker: str
    entry_timestamp: datetime
    exit_timestamp: datetime
    entry_price: float
    exit_price: float
    position_size: float
    fees: float
    net_profit: float


class TradePersistence(Protocol):
    """Storage interface a strategy engine writes completed trades and equity marks
    through. Concrete implementations (e.g. `backend.db.DatabasePersistence`) are
    injected by the composition root (`main.py`), never imported by the engines."""

    async def record_trade(self, trade: TradeRecord) -> None: ...

    async def record_equity_snapshot(self, engine_type: str, timestamp: datetime, equity: float) -> None: ...


class NullPersistence:
    """No-op `TradePersistence`, used as the default when no database is wired up."""

    async def record_trade(self, trade: TradeRecord) -> None:
        return None

    async def record_equity_snapshot(self, engine_type: str, timestamp: datetime, equity: float) -> None:
        return None
