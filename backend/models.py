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


class BracketStatus(str, Enum):
    """Lifecycle of a bracket order's protective levels."""

    ACTIVE = "ACTIVE"
    HIT_SL = "HIT_SL"
    HIT_TP = "HIT_TP"
    TIME_EXITED = "TIME_EXITED"


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


@dataclass(slots=True)
class BracketOrder:
    """A protective SL/TP pair guarding one open position, tracked by the
    execution gateway from entry fill until an exit (or cancellation).

    Mutable on purpose: a trailing rule may raise `stop_loss_price` while the
    bracket is ACTIVE (see the swing engine's break-even trail).
    """

    order_id: str  # the entry fill's order id
    engine_type: str
    ticker: str
    side: str  # "long" | "short"
    entry_price: float
    stop_loss_price: float
    take_profit_price: float
    size: float  # filled notional the exit order must unwind
    created_at: datetime
    status: BracketStatus = BracketStatus.ACTIVE


@dataclass(frozen=True, slots=True)
class BracketExit:
    """Result of a bracket level being touched: the gateway has already
    executed the market exit; `fill` is what the position actually closed at."""

    order_id: str
    status: BracketStatus  # HIT_SL or HIT_TP
    triggered_price: float  # the SL/TP level that was touched
    fill: OrderFill


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
    # Bracket levels the trade ran with and how it ultimately exited.
    # Empty bracket_status marks legacy/bracketless trades.
    stop_loss_price: float = 0.0
    take_profit_price: float = 0.0
    bracket_status: str = ""


class ExecutionGateway(Protocol):
    """Order-routing interface injected into the strategy engines.

    `risk_guard` is the Phase 6 operational guard slot (an
    `backend.utils.risk_guard.RiskGuard` or None) — typed loosely here so this
    shared module stays free of collaborator imports.

    Engines never simulate their own fills: every entry and exit goes through
    `execute_order`, and the returned `OrderFill` (post-slippage price, fees,
    partial-fill size) is the truth the engine books PnL against. Concrete
    implementations live in `backend/execution_gateway.py`; engines only ever
    see this protocol, keeping them unit-testable with fakes.
    """

    risk_guard: Any

    @property
    def name(self) -> str: ...

    async def execute_order(
        self,
        signal_type: SignalAction,
        size: float,
        ticker: str,
        requested_price: float,
        *,
        is_exit: bool = False,
    ) -> OrderFill: ...

    async def fetch_open_orders(self, ticker: str | None = None) -> list[OrderFill]: ...

    # --- bracket order surface (Phase 5) ---
    # The gateway owns bracket monitoring: engines register SL/TP levels at
    # entry, then feed it every incoming candle. `check_bracket` both detects a
    # touched level AND executes the market exit, so engines never self-fill.

    def observe_bar(self, bar: OHLCVBar) -> None: ...

    async def register_bracket(self, bracket: BracketOrder) -> None: ...

    async def check_bracket(self, order_id: str, bar: OHLCVBar) -> BracketExit | None: ...

    async def adjust_bracket_stop(self, order_id: str, new_stop: float) -> bool: ...

    async def cancel_bracket(self, order_id: str) -> BracketOrder | None: ...

    def active_brackets(self) -> list[BracketOrder]: ...

    def last_price(self, ticker: str) -> float | None: ...


class OrderFlowTelemetry(Protocol):
    """Latency-tracking interface an engine reports each order flow through.

    Measures the signal-generation -> risk-approval -> gateway-fill pipeline in
    milliseconds. Synchronous by design: implementations must only aggregate
    in memory / emit a log line, never block. Engines receive this injected
    (like every other collaborator) and may be given `None` to disable it.
    """

    def record_order_flow(
        self,
        engine_type: str,
        ticker: str,
        signal_to_approval_ms: float,
        approval_to_fill_ms: float,
        fill: OrderFill,
    ) -> None: ...


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
