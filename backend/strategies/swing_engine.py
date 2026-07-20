"""Swing Trading Engine — macro trendline strategy, "Tori Trades" style.

Listens to the 4-hour bar stream, detects peak/trough pivots with a rolling
window, fits ascending (support) trendlines that connect at least 3
historically-verified pivot touches, and alerts when price retraces to
within 0.5% of a valid trendline alongside a bullish engulfing candle.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime
from itertools import combinations

import pandas as pd

from backend.models import OHLCVBar, SignalAction, TradeSignal

PIVOT_WINDOW = 5  # bars on each side required to confirm a pivot
MIN_TOUCHES = 3
TOUCH_TOLERANCE_PCT = 0.005  # 0.5%
HOLD_PERIOD_BARS = 3  # bars held after an alert, for equity/performance tracking


@dataclass(slots=True)
class _OpenPosition:
    entry_price: float
    entry_index: int


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

    def __init__(
        self,
        symbol: str,
        pivot_window: int = PIVOT_WINDOW,
        min_touches: int = MIN_TOUCHES,
        touch_tolerance_pct: float = TOUCH_TOLERANCE_PCT,
        hold_period_bars: int = HOLD_PERIOD_BARS,
    ) -> None:
        self.symbol = symbol
        self.pivot_window = pivot_window
        self.min_touches = min_touches
        self.touch_tolerance_pct = touch_tolerance_pct
        self.hold_period_bars = hold_period_bars

        self._bars: list[OHLCVBar] = []
        self.signals: list[TradeSignal] = []
        self.equity: float = 0.0
        self.equity_curve: list[tuple[datetime, float]] = []
        self._alerted_lines: set[tuple[float, float]] = set()
        self._position: _OpenPosition | None = None

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
                if abs(p.price - predicted) / abs(predicted) <= self.touch_tolerance_pct:
                    touching.append(p)

            if len(touching) >= self.min_touches and (best is None or len(touching) > best.touches):
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

    def _close_position(self, bar: OHLCVBar, reason: str) -> TradeSignal:
        position = self._position
        assert position is not None
        pnl = bar.close - position.entry_price
        self.equity += pnl
        self.equity_curve.append((bar.timestamp, self.equity))
        self._position = None
        return self._emit(SignalAction.EXIT, bar.close, bar.timestamp, reason, pnl=round(pnl, 4))

    async def on_bar(self, bar: OHLCVBar) -> TradeSignal | None:
        """Feeds a single 4-hour bar to the engine, returning a signal if one fires.

        A trendline-retest alert opens a hypothetical position (for equity/performance
        tracking); it is marked to market and closed automatically after a fixed
        holding period so the strategy's equity curve stays bounded and observable.
        """
        self._bars.append(bar)

        if self._position is not None:
            current_index = len(self._bars) - 1
            if current_index - self._position.entry_index >= self.hold_period_bars:
                return self._close_position(bar, f"hold_period_{self.hold_period_bars}_bars")
            return None

        span = 2 * self.pivot_window + 1
        if len(self._bars) < span:
            return None

        line = self.find_support_trendline()
        if line is None or line.slope <= 0:
            return None  # only ascending (bullish) support trendlines are tradeable here

        current_index = len(self._bars) - 1
        line_value = line.value_at(current_index)
        if line_value <= 0:
            return None

        distance_pct = abs(bar.close - line_value) / line_value
        if distance_pct > self.touch_tolerance_pct:
            return None

        previous_bar = self._bars[-2]
        if not self.is_bullish_engulfing(previous_bar, bar):
            return None

        line_key = (round(line.slope, 6), round(line.intercept, 6))
        if line_key in self._alerted_lines:
            return None
        self._alerted_lines.add(line_key)
        self._position = _OpenPosition(entry_price=bar.close, entry_index=current_index)

        return self._emit(
            SignalAction.ALERT,
            bar.close,
            bar.timestamp,
            "trendline_retest_bullish_engulfing",
            trendline_slope=line.slope,
            trendline_touches=line.touches,
        )

    async def run(self, bar_stream: AsyncIterator[OHLCVBar]) -> AsyncIterator[TradeSignal]:
        """Consumes a 4-hour bar stream indefinitely, yielding signals as they fire."""
        async for bar in bar_stream:
            signal = await self.on_bar(bar)
            if signal is not None:
                yield signal
