"""Day Trading Engine — Opening Range Breakout (ORB), "20-minute trader" style.

Listens to the 1-minute bar stream, establishes the opening range from the
first 5 minutes after market open, and trades breakouts of that range with a
hard 20-minute time-stop so no position is left open indefinitely.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime, timedelta

from backend.models import OHLCVBar, SignalAction, TradeSignal

OPENING_RANGE_BARS = 5
TIME_STOP = timedelta(minutes=20)
TARGET_PROFIT_PCT = 0.005  # 0.5% — exits early if hit before the time-stop


@dataclass(slots=True)
class _OpenPosition:
    action: SignalAction
    entry_price: float
    entry_time: datetime


class MomentumEngine:
    """Opening Range Breakout day-trading engine driven by 1-minute bars."""

    name = "momentum_engine"

    def __init__(
        self,
        symbol: str,
        target_profit_pct: float = TARGET_PROFIT_PCT,
        time_stop: timedelta = TIME_STOP,
        opening_range_bars: int = OPENING_RANGE_BARS,
    ) -> None:
        self.symbol = symbol
        self.target_profit_pct = target_profit_pct
        self.time_stop = time_stop
        self.opening_range_bars = opening_range_bars

        self._opening_bars: list[OHLCVBar] = []
        self.opening_range_high: float | None = None
        self.opening_range_low: float | None = None
        self._position: _OpenPosition | None = None

        self.signals: list[TradeSignal] = []
        self.equity: float = 0.0
        self.equity_curve: list[tuple[datetime, float]] = []

    def _establish_opening_range(self, bar: OHLCVBar) -> None:
        self._opening_bars.append(bar)
        if len(self._opening_bars) == self.opening_range_bars:
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

    def _close_position(self, bar: OHLCVBar, reason: str) -> TradeSignal:
        position = self._position
        assert position is not None
        direction = 1 if position.action is SignalAction.BUY else -1
        pnl = direction * (bar.close - position.entry_price)
        self.equity += pnl
        self.equity_curve.append((bar.timestamp, self.equity))
        self._position = None
        return self._emit(SignalAction.EXIT, bar.close, bar.timestamp, reason, pnl=round(pnl, 4))

    async def on_bar(self, bar: OHLCVBar) -> TradeSignal | None:
        """Feeds a single 1-minute bar to the engine, returning a signal if one fires."""
        if self.opening_range_high is None or self.opening_range_low is None:
            self._establish_opening_range(bar)
            return None

        if self._position is not None:
            position = self._position
            direction = 1 if position.action is SignalAction.BUY else -1
            unrealized_pct = direction * (bar.close - position.entry_price) / position.entry_price

            if unrealized_pct >= self.target_profit_pct:
                return self._close_position(bar, "target_profit_reached")
            if bar.timestamp - position.entry_time >= self.time_stop:
                return self._close_position(bar, "time_stop_20min")
            return None

        if bar.close > self.opening_range_high:
            self._position = _OpenPosition(SignalAction.BUY, bar.close, bar.timestamp)
            return self._emit(
                SignalAction.BUY,
                bar.close,
                bar.timestamp,
                "orb_breakout_above_high",
                opening_range_high=self.opening_range_high,
                opening_range_low=self.opening_range_low,
            )
        if bar.close < self.opening_range_low:
            self._position = _OpenPosition(SignalAction.SHORT, bar.close, bar.timestamp)
            return self._emit(
                SignalAction.SHORT,
                bar.close,
                bar.timestamp,
                "orb_breakdown_below_low",
                opening_range_high=self.opening_range_high,
                opening_range_low=self.opening_range_low,
            )
        return None

    async def run(self, bar_stream: AsyncIterator[OHLCVBar]) -> AsyncIterator[TradeSignal]:
        """Consumes a 1-minute bar stream indefinitely, yielding signals as they fire."""
        async for bar in bar_stream:
            signal = await self.on_bar(bar)
            if signal is not None:
                yield signal
