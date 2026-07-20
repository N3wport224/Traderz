"""Tests for backend/utils/indicators.py: ATR math and resistance lookup."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from backend.models import OHLCVBar, Timeframe
from backend.utils.indicators import compute_atr, nearest_resistance, true_ranges

BASE_TIME = datetime(2026, 7, 20, 9, 30, tzinfo=timezone.utc)


def bar(index: int, open_: float, high: float, low: float, close: float) -> OHLCVBar:
    return OHLCVBar(
        symbol="MOCK",
        timestamp=BASE_TIME + timedelta(minutes=index),
        timeframe=Timeframe.ONE_MINUTE,
        open=open_,
        high=high,
        low=low,
        close=close,
        volume=1_000,
    )


# The momentum engine's canonical opening-range fixture plus its breakout bar —
# hand-computed TRs: [4, 4, 10, 4, 4, 7] -> ATR(14, min_periods=1) = 33/6 = 5.5
ORB_SEQUENCE = [
    bar(0, 100, 102, 98, 101),
    bar(1, 101, 103, 99, 100),
    bar(2, 100, 105, 95, 102),
    bar(3, 102, 104, 100, 101),
    bar(4, 101, 103, 99, 100),
    bar(5, 102, 107, 102, 106),
]


def test_true_range_first_bar_is_high_minus_low() -> None:
    ranges = true_ranges(ORB_SEQUENCE)
    assert ranges.iloc[0] == pytest.approx(4.0)  # 102 - 98, no previous close


def test_true_range_uses_previous_close_gaps() -> None:
    # Gap up: bar opens far above the previous close of 100.
    gapped = [bar(0, 100, 101, 99, 100), bar(1, 110, 111, 109, 110)]
    ranges = true_ranges(gapped)
    # TR = max(111-109=2, |111-100|=11, |109-100|=9) = 11
    assert ranges.iloc[1] == pytest.approx(11.0)


def test_atr_hand_computed_over_orb_sequence() -> None:
    assert compute_atr(ORB_SEQUENCE, period=14) == pytest.approx(5.5)


def test_atr_respects_rolling_window() -> None:
    """With more bars than the period, only the last `period` TRs count."""
    quiet = [bar(i, 100, 101, 99, 100) for i in range(20)]  # TR = 2 everywhere
    spike = bar(20, 100, 130, 90, 100)  # TR = 40
    series = quiet + [spike]
    # window of 14: 13 quiet TRs (2) + spike (40) -> (13*2 + 40) / 14
    assert compute_atr(series, period=14) == pytest.approx((13 * 2 + 40) / 14)
    # ...and once the spike falls out of the window, ATR returns to 2
    series_extended = series + [bar(21 + i, 100, 101, 99, 100) for i in range(14)]
    assert compute_atr(series_extended, period=14) == pytest.approx(2.0)


def test_atr_short_history_uses_available_bars() -> None:
    assert compute_atr(ORB_SEQUENCE[:1], period=14) == pytest.approx(4.0)
    assert compute_atr(ORB_SEQUENCE[:3], period=14) == pytest.approx((4 + 4 + 10) / 3)


def test_atr_empty_and_invalid_period() -> None:
    assert compute_atr([], period=14) == 0.0
    with pytest.raises(ValueError):
        compute_atr(ORB_SEQUENCE, period=0)


def test_atr_constant_prices_is_range_only() -> None:
    flat = [bar(i, 100, 100.5, 99.5, 100) for i in range(30)]
    assert compute_atr(flat) == pytest.approx(1.0)


def test_nearest_resistance_picks_lowest_ceiling_above() -> None:
    assert nearest_resistance([23.0, 18.0, 30.0], above=14.8) == 18.0
    assert nearest_resistance([23.0, 18.0, 30.0], above=25.0) == 30.0


def test_nearest_resistance_none_when_price_above_all_peaks() -> None:
    assert nearest_resistance([23.0, 18.0], above=31.0) is None
    assert nearest_resistance([], above=10.0) is None


def test_nearest_resistance_is_strictly_above() -> None:
    assert nearest_resistance([15.0], above=15.0) is None
