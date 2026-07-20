"""Broker/exchange execution gateway abstraction.

Every order a strategy engine wants filled is routed through a
`BaseExecutionGateway` subclass injected at construction (engines only ever see
the `ExecutionGateway` protocol from `backend/models.py`). Two implementations:

- `MockExecutionGateway` — default for local runs and tests. Simulates broker
  latency, fees, order-book-depth-based slippage, and occasional partial fills.
  Also keeps an in-memory book of open orders so boot-time reconciliation
  (`backend/reconciliation.py`) has something realistic to query.
- `LiveCCXTExecutionGateway` — maps the same interface onto a real exchange via
  the CCXT library, authenticating with the `API_KEY` / `API_SECRET`
  environment variables. CCXT is imported lazily so the dependency is only
  required when live trading is actually enabled.
"""

from __future__ import annotations

import asyncio
import itertools
import os
import random
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any

from backend.models import (
    BracketExit,
    BracketOrder,
    BracketStatus,
    OHLCVBar,
    OrderFill,
    OrderStatus,
    SignalAction,
)

# Which way an adverse fill moves the price for each order side. Closing a
# short is a market BUY, closing a long is a market SELL — engines pass the
# market action, not their bookkeeping intent.
_ADVERSE_DIRECTION: dict[SignalAction, float] = {
    SignalAction.BUY: 1.0,
    SignalAction.SHORT: -1.0,
    SignalAction.SELL: -1.0,
    SignalAction.EXIT: -1.0,
}


class GatewayError(Exception):
    """Raised when an order cannot be routed (bad side, broker rejection, ...)."""


class GatewayConfigError(GatewayError):
    """Raised when a gateway is constructed without the credentials/deps it needs."""


class BaseExecutionGateway(ABC):
    """Unified order-routing interface for every broker/exchange backend.

    Also owns the bracket-order monitor (Phase 5): engines register an SL/TP
    pair when an entry fills, feed every incoming candle through
    `observe_bar`/`check_bracket`, and the gateway detects touched levels and
    executes the market exit itself — engines book PnL off the returned fill
    but never simulate one. The registry lives here (not in the mock) so a live
    gateway monitors brackets identically, just with real exit orders.
    """

    def __init__(self) -> None:
        self._brackets: dict[str, BracketOrder] = {}
        self._last_prices: dict[str, float] = {}

    @property
    @abstractmethod
    def name(self) -> str:
        """Short identifier surfaced in /api/status and the frontend badge."""

    @abstractmethod
    async def execute_order(
        self,
        signal_type: SignalAction,
        size: float,
        ticker: str,
        requested_price: float,
    ) -> OrderFill:
        """Route a market order for `size` notional dollars of `ticker`.

        `requested_price` is the price the engine decided at (its bar close);
        the returned fill carries the post-slippage price actually obtained.
        """

    @abstractmethod
    async def fetch_open_orders(self, ticker: str | None = None) -> list[OrderFill]:
        """Entry fills the broker still holds open (no matching close yet)."""

    @staticmethod
    def adverse_direction(signal_type: SignalAction) -> float:
        direction = _ADVERSE_DIRECTION.get(signal_type)
        if direction is None:
            raise GatewayError(f"unroutable signal type: {signal_type.value}")
        return direction

    @staticmethod
    def slippage_cost(requested_price: float, filled_price: float, filled_size: float) -> float:
        """Dollar cost of the fill's price degradation over `filled_size` notional."""
        if requested_price <= 0:
            return 0.0
        shares = filled_size / requested_price
        return round(abs(filled_price - requested_price) * shares, 6)

    # --- bracket order monitor -------------------------------------------------

    def observe_bar(self, bar: OHLCVBar) -> None:
        """Records the freshest market price per ticker (drives /api/brackets)."""
        self._last_prices[bar.symbol] = bar.close

    def last_price(self, ticker: str) -> float | None:
        return self._last_prices.get(ticker)

    async def register_bracket(self, bracket: BracketOrder) -> None:
        if bracket.stop_loss_price <= 0 or bracket.take_profit_price <= 0:
            raise GatewayError("bracket SL/TP levels must be positive")
        if bracket.side == "long" and not bracket.stop_loss_price < bracket.entry_price < bracket.take_profit_price:
            raise GatewayError("long bracket requires SL < entry < TP")
        if bracket.side == "short" and not bracket.take_profit_price < bracket.entry_price < bracket.stop_loss_price:
            raise GatewayError("short bracket requires TP < entry < SL")
        self._brackets[bracket.order_id] = bracket

    def active_brackets(self) -> list[BracketOrder]:
        return list(self._brackets.values())

    async def adjust_bracket_stop(self, order_id: str, new_stop: float) -> bool:
        """Moves an active bracket's stop (trailing rules). Never widens risk:
        a long's stop only moves up, a short's only down. Returns True if moved."""
        bracket = self._brackets.get(order_id)
        if bracket is None:
            return False
        if bracket.side == "long":
            if new_stop <= bracket.stop_loss_price or new_stop >= bracket.take_profit_price:
                return False
        else:
            if new_stop >= bracket.stop_loss_price or new_stop <= bracket.take_profit_price:
                return False
        bracket.stop_loss_price = new_stop
        return True

    async def cancel_bracket(self, order_id: str) -> BracketOrder | None:
        """Removes a bracket without executing anything (time-stop/manual exits)."""
        return self._brackets.pop(order_id, None)

    async def check_bracket(self, order_id: str, bar: OHLCVBar) -> BracketExit | None:
        """Evaluates one candle against an active bracket; executes the exit on a touch.

        A candle whose low reaches the stop *and* whose high reaches the target
        is resolved pessimistically as HIT_SL — intrabar ordering is unknowable
        from OHLC data, so the bracket assumes the worst. A candle that gaps
        past a level fills at its open (you can't exit at a price the market
        skipped), with normal exit-side slippage applied on top either way.
        """
        self.observe_bar(bar)
        bracket = self._brackets.get(order_id)
        if bracket is None:
            return None

        if bracket.side == "long":
            sl_hit = bar.low <= bracket.stop_loss_price
            tp_hit = bar.high >= bracket.take_profit_price
        else:
            sl_hit = bar.high >= bracket.stop_loss_price
            tp_hit = bar.low <= bracket.take_profit_price
        if not sl_hit and not tp_hit:
            return None

        if sl_hit:  # pessimistic: stop wins when both trigger inside one candle
            status = BracketStatus.HIT_SL
            level = bracket.stop_loss_price
            gapped_through = bar.open < level if bracket.side == "long" else bar.open > level
        else:
            status = BracketStatus.HIT_TP
            level = bracket.take_profit_price
            gapped_through = bar.open > level if bracket.side == "long" else bar.open < level
        requested_price = bar.open if gapped_through else level

        exit_action = SignalAction.SELL if bracket.side == "long" else SignalAction.BUY
        fill = await self.execute_order(exit_action, bracket.size, bracket.ticker, requested_price)
        bracket.status = status
        del self._brackets[order_id]
        return BracketExit(order_id=order_id, status=status, triggered_price=level, fill=fill)


class MockExecutionGateway(BaseExecutionGateway):
    """Simulated broker: latency, fees, depth-based slippage, partial fills.

    Slippage model: the fill degrades adversely by `min_slippage_pct` for a
    tiny order, scaling linearly up to `max_slippage_pct` as the order's
    notional approaches `book_depth_notional` (simulated order-book depth),
    with a small random variance on top. Orders larger than the book depth may
    also only partially fill (`partial_fill_ratio` of the requested notional).
    """

    def __init__(
        self,
        fee_rate: float = 0.0005,
        min_slippage_pct: float = 0.0005,  # 0.05%
        max_slippage_pct: float = 0.002,  # 0.20%
        book_depth_notional: float = 50_000.0,
        partial_fill_ratio: float = 0.9,
        latency_range_ms: tuple[float, float] = (2.0, 10.0),
        rng: random.Random | None = None,
    ) -> None:
        super().__init__()
        if min_slippage_pct < 0 or max_slippage_pct < min_slippage_pct:
            raise GatewayConfigError("slippage bounds must satisfy 0 <= min <= max")
        if book_depth_notional <= 0:
            raise GatewayConfigError("book_depth_notional must be positive")
        if not 0 < partial_fill_ratio <= 1:
            raise GatewayConfigError("partial_fill_ratio must be in (0, 1]")
        self.fee_rate = fee_rate
        self.min_slippage_pct = min_slippage_pct
        self.max_slippage_pct = max_slippage_pct
        self.book_depth_notional = book_depth_notional
        self.partial_fill_ratio = partial_fill_ratio
        self.latency_range_ms = latency_range_ms
        self._rng = rng or random.Random()
        self._order_ids = itertools.count(1)
        self._open_orders: dict[str, OrderFill] = {}
        self._fills: list[OrderFill] = []

    @property
    def name(self) -> str:
        return "MOCK"

    def slippage_pct(self, size: float) -> float:
        """Deterministic component of the slippage model (variance added on top)."""
        depth_fraction = min(1.0, abs(size) / self.book_depth_notional)
        return self.min_slippage_pct + (self.max_slippage_pct - self.min_slippage_pct) * depth_fraction

    async def execute_order(
        self,
        signal_type: SignalAction,
        size: float,
        ticker: str,
        requested_price: float,
    ) -> OrderFill:
        if size <= 0:
            raise GatewayError(f"order size must be positive, got {size}")
        if requested_price <= 0:
            raise GatewayError(f"requested price must be positive, got {requested_price}")
        direction = self.adverse_direction(signal_type)

        started = time.perf_counter()
        await asyncio.sleep(self._rng.uniform(*self.latency_range_ms) / 1000.0)

        # Depth-scaled slippage with ±20% random variance, never below zero.
        slip_pct = self.slippage_pct(size) * self._rng.uniform(0.8, 1.2)
        filled_price = requested_price * (1.0 + direction * slip_pct)

        filled_size = size
        status = OrderStatus.FILLED
        if size > self.book_depth_notional:
            filled_size = size * self.partial_fill_ratio
            status = OrderStatus.PARTIALLY_FILLED

        latency_ms = (time.perf_counter() - started) * 1000.0
        fill = OrderFill(
            order_id=f"mock-{next(self._order_ids)}",
            ticker=ticker,
            signal_type=signal_type,
            requested_size=size,
            filled_size=filled_size,
            requested_price=requested_price,
            filled_price=round(filled_price, 6),
            fees=round(abs(filled_size) * self.fee_rate, 6),
            slippage_cost=self.slippage_cost(requested_price, filled_price, filled_size),
            status=status,
            latency_ms=round(latency_ms, 3),
            timestamp=datetime.now(timezone.utc),
        )
        self._fills.append(fill)

        if signal_type in (SignalAction.BUY, SignalAction.SHORT):
            self._open_orders[fill.order_id] = fill
        else:
            # A close removes the oldest open order on the same ticker.
            for order_id, open_fill in list(self._open_orders.items()):
                if open_fill.ticker == ticker:
                    del self._open_orders[order_id]
                    break
        return fill

    async def fetch_open_orders(self, ticker: str | None = None) -> list[OrderFill]:
        orders = list(self._open_orders.values())
        if ticker is not None:
            orders = [order for order in orders if order.ticker == ticker]
        return orders

    @property
    def fills(self) -> list[OrderFill]:
        """Every fill this gateway has produced (test/telemetry introspection)."""
        return list(self._fills)

    def seed_open_order(self, fill: OrderFill) -> None:
        """Inject a pre-existing broker-side open order (crash-recovery tests)."""
        self._open_orders[fill.order_id] = fill


class LiveCCXTExecutionGateway(BaseExecutionGateway):
    """Routes orders to a real exchange through CCXT's async client.

    Credentials come from explicit constructor args or the `API_KEY` /
    `API_SECRET` environment variables. The `ccxt` import happens at
    construction time so installations that only ever run the mock gateway
    don't need the dependency.
    """

    def __init__(
        self,
        exchange_id: str = "binance",
        api_key: str | None = None,
        api_secret: str | None = None,
    ) -> None:
        super().__init__()
        key = api_key or os.environ.get("API_KEY")
        secret = api_secret or os.environ.get("API_SECRET")
        if not key or not secret:
            raise GatewayConfigError(
                "LiveCCXTExecutionGateway requires API_KEY and API_SECRET "
                "(constructor args or environment variables)"
            )
        try:
            import ccxt.async_support as ccxt_async
        except ImportError as exc:
            raise GatewayConfigError(
                "the ccxt package is required for live trading: pip install ccxt"
            ) from exc
        try:
            exchange_cls = getattr(ccxt_async, exchange_id)
        except AttributeError as exc:
            raise GatewayConfigError(f"unknown CCXT exchange id: {exchange_id}") from exc

        self.exchange_id = exchange_id
        self._exchange = exchange_cls({"apiKey": key, "secret": secret, "enableRateLimit": True})

    @property
    def name(self) -> str:
        return f"LIVE:{self.exchange_id.upper()}"

    async def execute_order(
        self,
        signal_type: SignalAction,
        size: float,
        ticker: str,
        requested_price: float,
    ) -> OrderFill:
        if size <= 0 or requested_price <= 0:
            raise GatewayError("order size and requested price must be positive")
        direction = self.adverse_direction(signal_type)
        side = "buy" if direction > 0 else "sell"
        amount = size / requested_price  # notional dollars -> base-asset units

        started = time.perf_counter()
        raw: dict[str, Any] = await self._exchange.create_order(ticker, "market", side, amount)
        latency_ms = (time.perf_counter() - started) * 1000.0

        filled_amount = float(raw.get("filled") or amount)
        filled_price = float(raw.get("average") or raw.get("price") or requested_price)
        filled_size = filled_amount * filled_price
        fees = float((raw.get("fee") or {}).get("cost") or 0.0)
        status = OrderStatus.FILLED if filled_amount >= amount else OrderStatus.PARTIALLY_FILLED
        return OrderFill(
            order_id=str(raw.get("id") or ""),
            ticker=ticker,
            signal_type=signal_type,
            requested_size=size,
            filled_size=filled_size,
            requested_price=requested_price,
            filled_price=filled_price,
            fees=fees,
            slippage_cost=self.slippage_cost(requested_price, filled_price, filled_size),
            status=status,
            latency_ms=round(latency_ms, 3),
            timestamp=datetime.now(timezone.utc),
        )

    async def fetch_open_orders(self, ticker: str | None = None) -> list[OrderFill]:
        raw_orders: list[dict[str, Any]] = await self._exchange.fetch_open_orders(symbol=ticker)
        fills: list[OrderFill] = []
        for raw in raw_orders:
            price = float(raw.get("price") or 0.0)
            amount = float(raw.get("amount") or 0.0)
            side = str(raw.get("side") or "buy")
            fills.append(
                OrderFill(
                    order_id=str(raw.get("id") or ""),
                    ticker=str(raw.get("symbol") or ticker or ""),
                    signal_type=SignalAction.BUY if side == "buy" else SignalAction.SHORT,
                    requested_size=amount * price,
                    filled_size=float(raw.get("filled") or 0.0) * price,
                    requested_price=price,
                    filled_price=price,
                    fees=0.0,
                    slippage_cost=0.0,
                    status=OrderStatus.PARTIALLY_FILLED,
                    latency_ms=0.0,
                    timestamp=datetime.now(timezone.utc),
                )
            )
        return fills

    async def close(self) -> None:
        await self._exchange.close()
