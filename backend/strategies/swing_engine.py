"""Swing Trading Engine — macro trendline strategy, "Tori Trades" style.

Listens to the 4-hour bar stream, detects peak/trough pivots with a rolling
window, fits ascending (support) trendlines that connect at least N
historically-verified pivot touches (N and the retest proximity tolerance are
both live-configurable), and alerts when price retraces to within tolerance
of a valid trendline alongside a bullish engulfing candle. Position size is
capped by the shared `RiskManager`'s capital allocation limit for this
engine, and every closed (hypothetical) trade is written through the
injected `TradePersistence`. Orders are never self-filled: every entry and
exit routes through the injected `ExecutionGateway`, and PnL is booked against
the actual (post-slippage) fill prices it returns. The engine depends only on
`backend/models.py` (for the injected protocols) plus the standalone
`backend/config.py` / `backend/risk_manager.py` / `backend/execution_gateway.py`
collaborators — never on `backend/db.py`, `backend/main.py`, or the momentum
engine.
"""

from __future__ import annotations

import time
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime
from itertools import combinations

import pandas as pd

from backend.config import ConfigStore
from backend.execution_gateway import MockExecutionGateway
from backend.models import (
    BracketOrder,
    BracketStatus,
    ExecutionGateway,
    NullPersistence,
    OHLCVBar,
    OpenPositionRecord,
    OrderFill,
    OrderFlowTelemetry,
    SignalAction,
    TradePersistence,
    TradeRecord,
    TradeSignal,
)
from backend.risk_manager import RiskManager
from backend.utils.indicators import nearest_resistance

PIVOT_WINDOW = 5  # bars on each side required to confirm a pivot; not live-configurable
HOLD_PERIOD_BARS = 3  # bars held after an alert, for equity/performance tracking
MAX_BAR_HISTORY = 200  # bounds the rolling buffer so pivot/trendline recomputation stays O(1)-ish per bar
SUPPORT_WICK_BUFFER_PCT = 0.01  # SL sits 1% below the support pivot's lowest wick
TRAIL_TRIGGER_PCT = 0.02  # >2% unrealized profit trails the stop to break-even


@dataclass(slots=True)
class _OpenPosition:
    entry_price: float  # actual gateway fill price (post-slippage)
    entry_bar_count: int
    entry_timestamp: datetime
    notional: float  # actually-filled notional (may be < requested on partial fill)
    requested_entry_price: float
    entry_fees: float
    entry_slippage_cost: float
    entry_order_id: str
    stop_loss_price: float
    take_profit_price: float
    trailed_to_breakeven: bool = False


@dataclass(frozen=True, slots=True)
class Pivot:
    index: int
    timestamp: datetime
    price: float
    kind: str  # "peak" | "trough"


@dataclass(slots=True)
class Trendline:
    kind: str  # "support" | "resistance"
    slope: float
    intercept: float
    touches: int
    pivots: list[Pivot]

    def value_at(self, index: int) -> float:
        return self.slope * index + self.intercept


class SwingEngine:
    """Trendline-based macro swing-trading engine driven by 4-hour bars."""

    name = "swing_engine"
    ENGINE_TYPE = "swing"

    def __init__(
        self,
        symbol: str,
        *,
        pivot_window: int = PIVOT_WINDOW,
        hold_period_bars: int = HOLD_PERIOD_BARS,
        max_bar_history: int = MAX_BAR_HISTORY,
        config_store: ConfigStore | None = None,
        risk_manager: RiskManager | None = None,
        persistence: TradePersistence | None = None,
        gateway: ExecutionGateway | None = None,
        telemetry: OrderFlowTelemetry | None = None,
        starting_equity: float = 0.0,
    ) -> None:
        self.symbol = symbol
        self.pivot_window = pivot_window
        self.hold_period_bars = hold_period_bars
        self.max_bar_history = max_bar_history
        self._config_store = config_store or ConfigStore()
        self._risk_manager = risk_manager or RiskManager()
        self._persistence = persistence or NullPersistence()
        self._gateway: ExecutionGateway = gateway or MockExecutionGateway()
        self._telemetry = telemetry

        self._bars: list[OHLCVBar] = []
        self._bar_count: int = 0
        self.signals: list[TradeSignal] = []
        self.equity: float = starting_equity
        self.equity_curve: list[tuple[datetime, float]] = []
        self._alerted_lines: set[tuple[float, float]] = set()
        self._position: _OpenPosition | None = None

    @property
    def min_touches(self) -> int:
        """Live minimum trendline touchpoints required, read from config."""
        return self._config_store.swing.min_touches

    @property
    def touch_tolerance_pct(self) -> float:
        """Live bounce/retest proximity tolerance, read from config."""
        return self._config_store.swing.touch_tolerance_pct

    def to_dataframe(self) -> pd.DataFrame:
        records = [
            {
                "timestamp": bar.timestamp,
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": bar.volume,
            }
            for bar in self._bars
        ]
        return pd.DataFrame.from_records(records)

    def find_pivots(self) -> list[Pivot]:
        """Rolling-window peak/trough detection over the buffered bar history."""
        df = self.to_dataframe()
        window = self.pivot_window
        span = 2 * window + 1
        if len(df) < span:
            return []

        highs = df["high"]
        lows = df["low"]
        rolling_max = highs.rolling(window=span, center=True).max()
        rolling_min = lows.rolling(window=span, center=True).min()

        pivots: list[Pivot] = []
        for i in range(len(df)):
            if pd.isna(rolling_max.iloc[i]):
                continue
            timestamp = df["timestamp"].iloc[i]
            if highs.iloc[i] >= rolling_max.iloc[i]:
                pivots.append(Pivot(i, timestamp, float(highs.iloc[i]), "peak"))
            if lows.iloc[i] <= rolling_min.iloc[i]:
                pivots.append(Pivot(i, timestamp, float(lows.iloc[i]), "trough"))
        return pivots

    def _fit_trendline(self, pivots: list[Pivot], kind: str) -> Trendline | None:
        """Finds the best-fit line through any two pivots that at least `min_touches` pivots touch."""
        min_touches = self.min_touches
        tolerance = self.touch_tolerance_pct
        best: Trendline | None = None
        for p1, p2 in combinations(pivots, 2):
            if p1.index == p2.index:
                continue
            slope = (p2.price - p1.price) / (p2.index - p1.index)
            intercept = p1.price - slope * p1.index

            touching: list[Pivot] = []
            for p in pivots:
                predicted = slope * p.index + intercept
                if predicted == 0:
                    continue
                if abs(p.price - predicted) / abs(predicted) <= tolerance:
                    touching.append(p)

            if len(touching) >= min_touches and (best is None or len(touching) > best.touches):
                best = Trendline(kind, slope, intercept, len(touching), touching)
        return best

    def find_support_trendline(self) -> Trendline | None:
        """Best ascending trendline connecting trough pivots (>= min_touches)."""
        troughs = [p for p in self.find_pivots() if p.kind == "trough"]
        return self._fit_trendline(troughs, "support")

    def find_resistance_trendline(self) -> Trendline | None:
        """Best trendline connecting peak pivots (>= min_touches)."""
        peaks = [p for p in self.find_pivots() if p.kind == "peak"]
        return self._fit_trendline(peaks, "resistance")

    def _bracket_levels(self, support_pivots: list[Pivot], entry_price: float) -> tuple[float, float]:
        """Pivot-structure bracket for a long trendline bounce.

        Stop loss: 1% below the lowest wick of the most recent support pivot
        backing the trendline (trough pivot prices ARE the bar lows/wicks).
        Take profit: the nearest historical macro resistance ceiling — the
        lowest peak-pivot price above the entry; if price is already above
        every buffered peak, fall back to a 2R target.
        """
        latest_support = max(support_pivots, key=lambda p: p.index, default=None)
        stop_loss = (
            latest_support.price * (1.0 - SUPPORT_WICK_BUFFER_PCT)
            if latest_support is not None
            else entry_price * 0.98
        )
        if stop_loss >= entry_price:  # pivot sits above the fill (rare, deep-slip entry)
            stop_loss = entry_price * 0.98

        peak_prices = [p.price for p in self.find_pivots() if p.kind == "peak"]
        ceiling = nearest_resistance(peak_prices, entry_price)
        take_profit = ceiling if ceiling is not None else entry_price + 2.0 * (entry_price - stop_loss)
        return round(stop_loss, 6), round(take_profit, 6)

    @staticmethod
    def is_bullish_engulfing(previous: OHLCVBar, current: OHLCVBar) -> bool:
        """True if `current` is a bullish candle whose body engulfs the prior bearish body."""
        previous_bearish = previous.close < previous.open
        current_bullish = current.close > current.open
        engulfs = current.open <= previous.close and current.close >= previous.open
        return previous_bearish and current_bullish and engulfs

    def _emit(
        self,
        action: SignalAction,
        price: float,
        timestamp: datetime,
        reason: str,
        **metadata: object,
    ) -> TradeSignal:
        signal = TradeSignal(
            engine=self.name,
            symbol=self.symbol,
            action=action,
            price=price,
            timestamp=timestamp,
            reason=reason,
            metadata=metadata,
        )
        self.signals.append(signal)
        return signal

    async def _route_order(
        self,
        action: SignalAction,
        notional: float,
        bar: OHLCVBar,
        signal_started: float,
        approved_at: float,
    ) -> OrderFill:
        """Sends the order through the gateway and reports pipeline latency."""
        fill = await self._gateway.execute_order(action, notional, self.symbol, bar.close)
        filled_at = time.perf_counter()
        if self._telemetry is not None:
            self._telemetry.record_order_flow(
                self.ENGINE_TYPE,
                self.symbol,
                (approved_at - signal_started) * 1000.0,
                (filled_at - approved_at) * 1000.0,
                fill,
            )
        return fill

    async def _close_position(
        self,
        bar: OHLCVBar,
        reason: str,
        *,
        bracket_status: BracketStatus = BracketStatus.TIME_EXITED,
        exit_fill: OrderFill | None = None,
    ) -> list[TradeSignal]:
        position = self._position
        assert position is not None
        if exit_fill is None:
            # Market exit outside the bracket (hold-period/halt): drop the
            # now-moot protective levels, then route the order ourselves.
            await self._gateway.cancel_bracket(position.entry_order_id)
            signal_started = time.perf_counter()
            # Exits are always allowed — approval is instantaneous by design.
            fill = await self._route_order(SignalAction.SELL, position.notional, bar, signal_started, signal_started)
        else:
            fill = exit_fill  # the gateway's bracket monitor already executed it

        pct_move = (fill.filled_price - position.entry_price) / position.entry_price
        gross_pnl = pct_move * position.notional
        fees = position.entry_fees + fill.fees
        net_pnl = gross_pnl - fees
        slippage_cost = position.entry_slippage_cost + fill.slippage_cost

        self.equity += net_pnl
        self.equity_curve.append((bar.timestamp, self.equity))
        self._position = None

        signals = [
            self._emit(
                SignalAction.EXIT,
                fill.filled_price,
                bar.timestamp,
                reason,
                pnl=round(net_pnl, 4),
                fees=round(fees, 4),
                slippage_cost=round(slippage_cost, 4),
                order_id=fill.order_id,
                bracket_status=bracket_status.value,
                stop_loss_price=position.stop_loss_price,
                take_profit_price=position.take_profit_price,
            )
        ]

        await self._persistence.record_trade(
            TradeRecord(
                engine_type=self.ENGINE_TYPE,
                asset_ticker=self.symbol,
                entry_timestamp=position.entry_timestamp,
                exit_timestamp=bar.timestamp,
                entry_price=position.entry_price,
                exit_price=fill.filled_price,
                position_size=position.notional,
                fees=fees,
                net_profit=net_pnl,
                requested_price=position.requested_entry_price,
                actual_filled_price=position.entry_price,
                slippage_cost=slippage_cost,
                stop_loss_price=position.stop_loss_price,
                take_profit_price=position.take_profit_price,
                bracket_status=bracket_status.value,
            )
        )
        await self._persistence.record_equity_snapshot(self.ENGINE_TYPE, bar.timestamp, self.equity)
        await self._persistence.clear_open_position(self.ENGINE_TYPE, self.symbol)

        if self._risk_manager.record_realized_pnl(self.ENGINE_TYPE, net_pnl, bar.timestamp):
            signals.append(
                self._emit(SignalAction.CIRCUIT_BREAKER, fill.filled_price, bar.timestamp, "max_daily_drawdown_exceeded")
            )
        return signals

    async def on_bar(self, bar: OHLCVBar) -> list[TradeSignal]:
        """Feeds a single 4-hour bar to the engine, returning any signals that fired.

        A trendline-retest alert opens a hypothetical position (for equity/performance
        tracking); it is marked to market and closed automatically after a fixed
        holding period so the strategy's equity curve stays bounded and observable.
        """
        if self._risk_manager.is_data_disconnected(self.symbol):
            # The ticker's stream is down/unverified: freeze — don't evaluate,
            # don't even buffer bars whose integrity is in question.
            return []

        self._bars.append(bar)
        self._bar_count += 1
        if len(self._bars) > self.max_bar_history:
            # Bound the buffer so pivot/trendline recomputation stays cheap per bar.
            # Safe to trim from the front: pivot/trendline indices are local to each
            # call and never compared across calls, and hold-period tracking uses
            # `_bar_count` (a monotonic counter), not a list index.
            self._bars = self._bars[-self.max_bar_history :]
        self._gateway.observe_bar(bar)

        if not self._risk_manager.can_open_position(bar.timestamp):
            if self._position is not None:
                return await self._close_position(bar, "circuit_breaker_active")
            return []

        if self._position is not None:
            position = self._position
            # The gateway's bracket monitor rules first: it detects an SL/TP
            # touch on this candle and executes the exit itself.
            bracket_exit = await self._gateway.check_bracket(position.entry_order_id, bar)
            if bracket_exit is not None:
                reason = "stop_loss_hit" if bracket_exit.status is BracketStatus.HIT_SL else "take_profit_hit"
                return await self._close_position(
                    bar, reason, bracket_status=bracket_exit.status, exit_fill=bracket_exit.fill
                )

            # Trailing rule: once the trade is >2% in profit, lift the stop to
            # the entry price (break-even) so the downside is locked out.
            trail_trigger = position.entry_price * (1.0 + TRAIL_TRIGGER_PCT)
            if not position.trailed_to_breakeven and bar.close >= trail_trigger:
                moved = await self._gateway.adjust_bracket_stop(position.entry_order_id, position.entry_price)
                position.trailed_to_breakeven = True
                if moved:
                    position.stop_loss_price = position.entry_price
                    return [
                        self._emit(
                            SignalAction.ALERT,
                            bar.close,
                            bar.timestamp,
                            "trailing_stop_moved_to_breakeven",
                            stop_loss_price=position.entry_price,
                            take_profit_price=position.take_profit_price,
                            unrealized_pct=round((bar.close - position.entry_price) / position.entry_price, 6),
                        )
                    ]

            if self._bar_count - position.entry_bar_count >= self.hold_period_bars:
                return await self._close_position(
                    bar, f"hold_period_{self.hold_period_bars}_bars", bracket_status=BracketStatus.TIME_EXITED
                )
            return []

        span = 2 * self.pivot_window + 1
        if len(self._bars) < span:
            return []

        line = self.find_support_trendline()
        if line is None or line.slope <= 0:
            return []  # only ascending (bullish) support trendlines are tradeable here

        current_index = len(self._bars) - 1
        line_value = line.value_at(current_index)
        if line_value <= 0:
            return []

        distance_pct = abs(bar.close - line_value) / line_value
        if distance_pct > self.touch_tolerance_pct:
            return []

        previous_bar = self._bars[-2]
        if not self.is_bullish_engulfing(previous_bar, bar):
            return []

        line_key = (round(line.slope, 6), round(line.intercept, 6))
        if line_key in self._alerted_lines:
            return []
        self._alerted_lines.add(line_key)

        signal_started = time.perf_counter()
        notional = self._risk_manager.position_size(self.ENGINE_TYPE)  # risk approval: allocation cap
        approved_at = time.perf_counter()
        fill = await self._route_order(SignalAction.BUY, notional, bar, signal_started, approved_at)

        stop_loss, take_profit = self._bracket_levels(line.pivots, fill.filled_price)
        await self._gateway.register_bracket(
            BracketOrder(
                order_id=fill.order_id,
                engine_type=self.ENGINE_TYPE,
                ticker=self.symbol,
                side="long",
                entry_price=fill.filled_price,
                stop_loss_price=stop_loss,
                take_profit_price=take_profit,
                size=fill.filled_size,
                created_at=bar.timestamp,
            )
        )

        self._position = _OpenPosition(
            entry_price=fill.filled_price,
            entry_bar_count=self._bar_count,
            entry_timestamp=bar.timestamp,
            notional=fill.filled_size,
            requested_entry_price=fill.requested_price,
            entry_fees=fill.fees,
            entry_slippage_cost=fill.slippage_cost,
            entry_order_id=fill.order_id,
            stop_loss_price=stop_loss,
            take_profit_price=take_profit,
        )
        await self._persistence.record_open_position(
            OpenPositionRecord(
                engine_type=self.ENGINE_TYPE,
                asset_ticker=self.symbol,
                side="long",
                entry_timestamp=bar.timestamp,
                entry_price=fill.filled_price,
                requested_entry_price=fill.requested_price,
                position_size=fill.filled_size,
                entry_fees=fill.fees,
                entry_order_id=fill.order_id,
            )
        )

        risk = fill.filled_price - stop_loss
        reward = take_profit - fill.filled_price
        return [
            self._emit(
                SignalAction.ALERT,
                fill.filled_price,
                bar.timestamp,
                "trendline_retest_bullish_engulfing",
                trendline_slope=line.slope,
                trendline_touches=line.touches,
                position_size=fill.filled_size,
                requested_price=fill.requested_price,
                slippage_cost=fill.slippage_cost,
                order_id=fill.order_id,
                order_status=fill.status.value,
                stop_loss_price=stop_loss,
                take_profit_price=take_profit,
                risk_reward_ratio=round(reward / risk, 4) if risk > 0 else None,
            )
        ]

    async def run(self, bar_stream: AsyncIterator[OHLCVBar]) -> AsyncIterator[TradeSignal]:
        """Consumes a 4-hour bar stream indefinitely, yielding signals as they fire."""
        async for bar in bar_stream:
            for signal in await self.on_bar(bar):
                yield signal
