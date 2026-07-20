"""Day Trading Engine — Opening Range Breakout (ORB), "20-minute trader" style.

Listens to the 1-minute bar stream, establishes the opening range from the
first N minutes after market open (N is live-configurable), and trades
breakouts of that range with a time-stop so no position is left open
indefinitely. Position size is capped by the shared `RiskManager`'s capital
allocation limit for this engine, and every closed trade is written through
the injected `TradePersistence`. The engine depends only on
`backend/models.py` (for the injected protocol) plus the standalone
`backend/config.py` / `backend/risk_manager.py` collaborators — never on
`backend/db.py`, `backend/main.py`, or the swing engine.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime, timedelta

from backend.config import ConfigStore
from backend.models import NullPersistence, OHLCVBar, SignalAction, TradePersistence, TradeRecord, TradeSignal
from backend.risk_manager import RiskManager

TARGET_PROFIT_PCT = 0.005  # 0.5% — exits early if hit before the time-stop; not live-configurable


@dataclass(slots=True)
class _OpenPosition:
    action: SignalAction
    entry_price: float
    entry_time: datetime
    notional: float


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
        starting_equity: float = 0.0,
    ) -> None:
        self.symbol = symbol
        self.target_profit_pct = target_profit_pct
        self._config_store = config_store or ConfigStore()
        self._risk_manager = risk_manager or RiskManager()
        self._persistence = persistence or NullPersistence()

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

    async def _close_position(self, bar: OHLCVBar, reason: str) -> list[TradeSignal]:
        position = self._position
        assert position is not None
        direction = 1 if position.action is SignalAction.BUY else -1
        pct_move = direction * (bar.close - position.entry_price) / position.entry_price
        gross_pnl = pct_move * position.notional
        fees = self._risk_manager.compute_fees(position.notional)
        net_pnl = gross_pnl - fees

        self.equity += net_pnl
        self.equity_curve.append((bar.timestamp, self.equity))
        self._position = None

        signals = [self._emit(SignalAction.EXIT, bar.close, bar.timestamp, reason, pnl=round(net_pnl, 4), fees=round(fees, 4))]

        await self._persistence.record_trade(
            TradeRecord(
                engine_type=self.ENGINE_TYPE,
                asset_ticker=self.symbol,
                entry_timestamp=position.entry_time,
                exit_timestamp=bar.timestamp,
                entry_price=position.entry_price,
                exit_price=bar.close,
                position_size=position.notional,
                fees=fees,
                net_profit=net_pnl,
            )
        )
        await self._persistence.record_equity_snapshot(self.ENGINE_TYPE, bar.timestamp, self.equity)

        if self._risk_manager.record_realized_pnl(self.ENGINE_TYPE, net_pnl, bar.timestamp):
            signals.append(
                self._emit(SignalAction.CIRCUIT_BREAKER, bar.close, bar.timestamp, "max_daily_drawdown_exceeded")
            )
        return signals

    async def on_bar(self, bar: OHLCVBar) -> list[TradeSignal]:
        """Feeds a single 1-minute bar to the engine, returning any signals that fired."""
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
            notional = self._risk_manager.position_size(self.ENGINE_TYPE)
            self._position = _OpenPosition(SignalAction.BUY, bar.close, bar.timestamp, notional)
            return [
                self._emit(
                    SignalAction.BUY,
                    bar.close,
                    bar.timestamp,
                    "orb_breakout_above_high",
                    opening_range_high=self.opening_range_high,
                    opening_range_low=self.opening_range_low,
                    position_size=notional,
                )
            ]
        if bar.close < self.opening_range_low:
            notional = self._risk_manager.position_size(self.ENGINE_TYPE)
            self._position = _OpenPosition(SignalAction.SHORT, bar.close, bar.timestamp, notional)
            return [
                self._emit(
                    SignalAction.SHORT,
                    bar.close,
                    bar.timestamp,
                    "orb_breakdown_below_low",
                    opening_range_high=self.opening_range_high,
                    opening_range_low=self.opening_range_low,
                    position_size=notional,
                )
            ]
        return []

    async def run(self, bar_stream: AsyncIterator[OHLCVBar]) -> AsyncIterator[TradeSignal]:
        """Consumes a 1-minute bar stream indefinitely, yielding signals as they fire."""
        async for bar in bar_stream:
            for signal in await self.on_bar(bar):
                yield signal
