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
    DATA_DISCONNECTED = "data_disconnected"
    DATA_RECONNECTED = "data_reconnected"


class OrderStatus(str, Enum):
    FILLED = "filled"
    PARTIALLY_FILLED = "partially_filled"
    REJECTED = "rejected"


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
class OrderFill:
    """Result of routing an order through an `ExecutionGateway`.

    `requested_price` is the price the engine asked for (the bar close it acted
    on); `filled_price` is what the gateway actually filled at after slippage.
    `slippage_cost` is the dollar amount lost to that degradation (always >= 0
    for an adverse fill), computed by the gateway because only it knows which
    direction is adverse for the order's side.
    """

    order_id: str
    ticker: str
    signal_type: SignalAction
    requested_size: float
    filled_size: float
    requested_price: float
    filled_price: float
    fees: float
    slippage_cost: float
    status: OrderStatus
    latency_ms: float
    timestamp: datetime


@dataclass(frozen=True, slots=True)
class OpenPositionRecord:
    """A live (not yet closed) position, persisted so a crashed backend can
    reconcile against the gateway's open orders on the next boot."""

    engine_type: str
    asset_ticker: str
    side: str  # "long" | "short"
    entry_timestamp: datetime
    entry_price: float  # actual filled entry price (post-slippage)
    requested_entry_price: float
    position_size: float  # notional dollars
    entry_fees: float
    entry_order_id: str


@dataclass(frozen=True, slots=True)
class TradeRecord:
    """A completed round-trip trade, ready to be persisted by a `TradePersistence`.

    `entry_price` remains the actual (post-slippage) entry fill; `requested_price`
    / `actual_filled_price` expose the entry slippage pair explicitly, and
    `slippage_cost` is the combined entry+exit dollar cost of price degradation.
    """

    engine_type: str
    asset_ticker: str
    entry_timestamp: datetime
    exit_timestamp: datetime
    entry_price: float
    exit_price: float
    position_size: float
    fees: float
    net_profit: float
    requested_price: float = 0.0
    actual_filled_price: float = 0.0
    slippage_cost: float = 0.0


class ExecutionGateway(Protocol):
    """Order-routing interface injected into the strategy engines.

    Engines never simulate their own fills: every entry and exit goes through
    `execute_order`, and the returned `OrderFill` (post-slippage price, fees,
    partial-fill size) is the truth the engine books PnL against. Concrete
    implementations live in `backend/execution_gateway.py`; engines only ever
    see this protocol, keeping them unit-testable with fakes.
    """

    @property
    def name(self) -> str: ...

    async def execute_order(
        self,
        signal_type: SignalAction,
        size: float,
        ticker: str,
        requested_price: float,
    ) -> OrderFill: ...

    async def fetch_open_orders(self, ticker: str | None = None) -> list[OrderFill]: ...


class TradePersistence(Protocol):
    """Storage interface a strategy engine writes completed trades and equity marks
    through. Concrete implementations (e.g. `backend.db.DatabasePersistence`) are
    injected by the composition root (`main.py`), never imported by the engines."""

    async def record_trade(self, trade: TradeRecord) -> None: ...

    async def record_equity_snapshot(self, engine_type: str, timestamp: datetime, equity: float) -> None: ...

    async def record_open_position(self, position: OpenPositionRecord) -> None: ...

    async def clear_open_position(self, engine_type: str, asset_ticker: str) -> None: ...

    async def list_open_positions(self, engine_type: str | None = None) -> list[OpenPositionRecord]: ...


class NullPersistence:
    """No-op `TradePersistence`, used as the default when no database is wired up."""

    async def record_trade(self, trade: TradeRecord) -> None:
        return None

    async def record_equity_snapshot(self, engine_type: str, timestamp: datetime, equity: float) -> None:
        return None

    async def record_open_position(self, position: OpenPositionRecord) -> None:
        return None

    async def clear_open_position(self, engine_type: str, asset_ticker: str) -> None:
        return None

    async def list_open_positions(self, engine_type: str | None = None) -> list[OpenPositionRecord]:
        return []
