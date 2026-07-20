"""Technical-analysis helpers shared by the strategy engines.

Pure, synchronous math over standardized `OHLCVBar` sequences — no I/O, no
engine state. Uses standard pandas operations so the formulas stay legible and
auditable against textbook definitions.
"""

from __future__ import annotations

from collections.abc import Sequence

import pandas as pd

from backend.models import OHLCVBar

DEFAULT_ATR_PERIOD = 14


def true_ranges(bars: Sequence[OHLCVBar]) -> pd.Series:
    """True Range per bar: max(H-L, |H-prev C|, |L-prev C|).

    The first bar has no previous close, so its TR degrades to plain H-L.
    """
    if not bars:
        return pd.Series(dtype=float)
    df = pd.DataFrame(
        {
            "high": [bar.high for bar in bars],
            "low": [bar.low for bar in bars],
            "close": [bar.close for bar in bars],
        }
    )
    previous_close = df["close"].shift(1)
    components = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - previous_close).abs(),
            (df["low"] - previous_close).abs(),
        ],
        axis=1,
    )
    return pd.Series(components.max(axis=1, skipna=True))


def compute_atr(bars: Sequence[OHLCVBar], period: int = DEFAULT_ATR_PERIOD) -> float:
    """Average True Range: rolling mean of True Range over `period` bars.

    Returns the latest ATR value. With fewer than `period` bars the average is
    taken over whatever history exists (`min_periods=1`) — early in a session
    an approximate volatility estimate beats refusing to trade brackets at all.
    Returns 0.0 for an empty series.
    """
    if period < 1:
        raise ValueError(f"ATR period must be >= 1, got {period}")
    ranges = true_ranges(bars)
    if ranges.empty:
        return 0.0
    atr = pd.Series(ranges.rolling(window=period, min_periods=1).mean())
    return float(atr.iloc[-1])


def nearest_resistance(peak_prices: Sequence[float], above: float) -> float | None:
    """Nearest historical resistance ceiling strictly above `above`.

    Given the prices of detected peak pivots, returns the lowest one that sits
    above the reference price — the first "macro ceiling" a long trade would
    run into. None when no peak lies above (price is at all-time highs of the
    buffered history).
    """
    ceilings = [price for price in peak_prices if price > above]
    return min(ceilings) if ceilings else None
