"""Day Trading Engine — Opening Range Breakout (ORB), "20-minute trader" style.

Listens to the 1-minute bar stream, establishes the opening range from the
first N minutes after market open (N is live-configurable), and trades
breakouts of that range with a time-stop so no position is left open
indefinitely. Position size is capped by the shared `RiskManager`'s capital
allocation limit for this engine, and every closed trade is written through
the injected `TradePersistence`. Orders are never self-filled: every entry and
exit routes through the injected `ExecutionGateway`, and PnL is booked against
the actual (post-slippage) fill prices it returns. The engine depends only on
`backend/models.py` (for the injected protocols) plus the standalone
`backend/config.py` / `backend/risk_manager.py` / `backend/execution_gateway.py`
collaborators — never on `backend/db.py`, `backend/main.py`, or the swing engine.
"""

from __future__ import annotations

import time
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime, timedelta

from backend.config import ConfigStore
from backend.execution_gateway import MockExecutionGateway
from backend.models import (
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

TARGET_PROFIT_PCT = 0.005  # 0.5% — exits early if hit before the time-stop; not live-configurable


@dataclass(slots=True)
class _OpenPosition:
    action: SignalAction
    entry_price: float  # actual gateway fill price (post-slippage)
    entry_time: datetime
    notional: float  # actually-filled notional (may be < requested on partial fill)
    requested_entry_price: float
    entry_fees: float
    entry_slippage_cost: float
    entry_order_id: str


class MomentumEngine:
    """Opening Range Breakout day-trading engine driven by 1-minute bars."""

    name = "momentum_engine"
    ENGINE_TYPE = "momentum"

    def __init__(
        self,
        symbol: str,
        *,
        target_profit_pct: float = TARGET_PROFIT_PCT,
        config_store: ConfigStore | None = None,
        risk_manager: RiskManager | None = None,
        persistence: TradePersistence | None = None,
        gateway: ExecutionGateway | None = None,
        telemetry: OrderFlowTelemetry | None = None,
        starting_equity: float = 0.0,
    ) -> None:
        self.symbol = symbol
        self.target_profit_pct = target_profit_pct
        self._config_store = config_store or ConfigStore()
        self._risk_manager = risk_manager or RiskManager()
        self._persistence = persistence or NullPersistence()
        self._gateway: ExecutionGateway = gateway or MockExecutionGateway()
        self._telemetry = telemetry

        self._opening_bars: list[OHLCVBar] = []
        self.opening_range_high: float | None = None
        self.opening_range_low: float | None = None
        self._position: _OpenPosition | None = None

        self.signals: list[TradeSignal] = []
        self.equity: float = starting_equity
        self.equity_curve: list[tuple[datetime, float]] = []

    @property
    def opening_range_bars(self) -> int:
        """Live opening-range duration in bars (1 bar == 1 minute), read from config."""
        return self._config_store.momentum.opening_range_minutes

    @property
    def time_stop(self) -> timedelta:
        """Live time-bound exit threshold, read from config."""
        return timedelta(minutes=self._config_store.momentum.time_stop_minutes)

    def _establish_opening_range(self, bar: OHLCVBar) -> None:
        self._opening_bars.append(bar)
        if len(self._opening_bars) >= self.opening_range_bars:
            self.opening_range_high = max(b.high for b in self._opening_bars)
            self.opening_range_low = min(b.low for b in self._opening_bars)

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

    async def _open_position(self, action: SignalAction, bar: OHLCVBar, reason: str) -> list[TradeSignal]:
        signal_started = time.perf_counter()
        notional = self._risk_manager.position_size(self.ENGINE_TYPE)  # risk approval: allocation cap
        approved_at = time.perf_counter()
        fill = await self._route_order(action, notional, bar, signal_started, approved_at)

        self._position = _OpenPosition(
            action=action,
            entry_price=fill.filled_price,
            entry_time=bar.timestamp,
            notional=fill.filled_size,
            requested_entry_price=fill.requested_price,
            entry_fees=fill.fees,
            entry_slippage_cost=fill.slippage_cost,
            entry_order_id=fill.order_id,
        )
        await self._persistence.record_open_position(
            OpenPositionRecord(
                engine_type=self.ENGINE_TYPE,
                asset_ticker=self.symbol,
                side="long" if action is SignalAction.BUY else "short",
                entry_timestamp=bar.timestamp,
                entry_price=fill.filled_price,
                requested_entry_price=fill.requested_price,
                position_size=fill.filled_size,
                entry_fees=fill.fees,
                entry_order_id=fill.order_id,
            )
        )
        return [
            self._emit(
                action,
                fill.filled_price,
                bar.timestamp,
                reason,
                opening_range_high=self.opening_range_high,
                opening_range_low=self.opening_range_low,
                position_size=fill.filled_size,
                requested_price=fill.requested_price,
                slippage_cost=fill.slippage_cost,
                order_id=fill.order_id,
                order_status=fill.status.value,
            )
        ]

    async def _close_position(self, bar: OHLCVBar, reason: str) -> list[TradeSignal]:
        position = self._position
        assert position is not None
        signal_started = time.perf_counter()
        # Exits are always allowed — approval is instantaneous by design.
        exit_action = SignalAction.SELL if position.action is SignalAction.BUY else SignalAction.BUY
        fill = await self._route_order(exit_action, position.notional, bar, signal_started, signal_started)

        direction = 1 if position.action is SignalAction.BUY else -1
        pct_move = direction * (fill.filled_price - position.entry_price) / position.entry_price
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
            )
        ]

        await self._persistence.record_trade(
            TradeRecord(
                engine_type=self.ENGINE_TYPE,
                asset_ticker=self.symbol,
                entry_timestamp=position.entry_time,
                exit_timestamp=bar.timestamp,
                entry_price=position.entry_price,
                exit_price=fill.filled_price,
                position_size=position.notional,
                fees=fees,
                net_profit=net_pnl,
                requested_price=position.requested_entry_price,
                actual_filled_price=position.entry_price,
                slippage_cost=slippage_cost,
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
        """Feeds a single 1-minute bar to the engine, returning any signals that fired."""
        if self._risk_manager.is_data_disconnected(self.symbol):
            # The ticker's stream is down/unverified: freeze — don't evaluate,
            # don't trade against bars whose integrity is in question.
            return []

        if not self._risk_manager.can_open_position(bar.timestamp):
            if self._position is not None:
                return await self._close_position(bar, "circuit_breaker_active")
            return []

        if self.opening_range_high is None or self.opening_range_low is None:
            self._establish_opening_range(bar)
            return []

        if self._position is not None:
            position = self._position
            direction = 1 if position.action is SignalAction.BUY else -1
            unrealized_pct = direction * (bar.close - position.entry_price) / position.entry_price

            if unrealized_pct >= self.target_profit_pct:
                return await self._close_position(bar, "target_profit_reached")
            if bar.timestamp - position.entry_time >= self.time_stop:
                return await self._close_position(bar, "time_stop_20min")
            return []

        if bar.close > self.opening_range_high:
            return await self._open_position(SignalAction.BUY, bar, "orb_breakout_above_high")
        if bar.close < self.opening_range_low:
            return await self._open_position(SignalAction.SHORT, bar, "orb_breakdown_below_low")
        return []

    async def run(self, bar_stream: AsyncIterator[OHLCVBar]) -> AsyncIterator[TradeSignal]:
        """Consumes a 1-minute bar stream indefinitely, yielding signals as they fire."""
        async for bar in bar_stream:
            for signal in await self.on_bar(bar):
                yield signal
