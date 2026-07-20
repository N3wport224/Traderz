"""Verification tests for the Momentum (ORB) and Swing (trendline) strategy engines."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from backend.models import OHLCVBar, SignalAction, Timeframe
from backend.strategies.momentum_engine import MomentumEngine
from backend.strategies.swing_engine import SwingEngine

BASE_TIME = datetime(2026, 7, 20, 9, 30, tzinfo=timezone.utc)


def make_1m_bar(minute_offset: int, open_: float, high: float, low: float, close: float) -> OHLCVBar:
    return OHLCVBar(
        symbol="MOCK",
        timestamp=BASE_TIME + timedelta(minutes=minute_offset),
        timeframe=Timeframe.ONE_MINUTE,
        open=open_,
        high=high,
        low=low,
        close=close,
        volume=10_000,
    )


OPENING_RANGE_BARS = [
    make_1m_bar(0, 100, 102, 98, 101),
    make_1m_bar(1, 101, 103, 99, 100),
    make_1m_bar(2, 100, 105, 95, 102),  # sets opening range high=105, low=95
    make_1m_bar(3, 102, 104, 100, 101),
    make_1m_bar(4, 101, 103, 99, 100),
]


# --- Momentum Engine (ORB) ------------------------------------------------


@pytest.mark.asyncio
async def test_opening_range_established_with_no_signal() -> None:
    engine = MomentumEngine("MOCK")
    for bar in OPENING_RANGE_BARS:
        signal = await engine.on_bar(bar)
        assert signal is None

    assert engine.opening_range_high == 105
    assert engine.opening_range_low == 95


@pytest.mark.asyncio
async def test_breakout_above_high_triggers_buy() -> None:
    engine = MomentumEngine("MOCK")
    for bar in OPENING_RANGE_BARS:
        await engine.on_bar(bar)

    breakout_bar = make_1m_bar(5, 102, 107, 102, 106)
    signal = await engine.on_bar(breakout_bar)

    assert signal is not None
    assert signal.action is SignalAction.BUY
    assert signal.price == 106
    assert signal.metadata["opening_range_high"] == 105


@pytest.mark.asyncio
async def test_breakdown_below_low_triggers_short() -> None:
    engine = MomentumEngine("MOCK")
    for bar in OPENING_RANGE_BARS:
        await engine.on_bar(bar)

    breakdown_bar = make_1m_bar(5, 96, 96, 93, 94)
    signal = await engine.on_bar(breakdown_bar)

    assert signal is not None
    assert signal.action is SignalAction.SHORT
    assert signal.price == 94


@pytest.mark.asyncio
async def test_time_stop_exits_after_20_minutes_without_target_profit() -> None:
    engine = MomentumEngine("MOCK")
    for bar in OPENING_RANGE_BARS:
        await engine.on_bar(bar)

    entry_signal = await engine.on_bar(make_1m_bar(5, 102, 107, 102, 106))
    assert entry_signal is not None and entry_signal.action is SignalAction.BUY

    # Price drifts sideways, well under the 0.5% target profit, for 19 more minutes.
    exit_signal = None
    for minute in range(6, 26):
        exit_signal = await engine.on_bar(make_1m_bar(minute, 106.1, 106.3, 105.9, 106.1))
        if exit_signal is not None:
            break

    assert exit_signal is not None
    assert exit_signal.action is SignalAction.EXIT
    assert exit_signal.reason == "time_stop_20min"
    assert exit_signal.timestamp - entry_signal.timestamp == timedelta(minutes=20)


@pytest.mark.asyncio
async def test_target_profit_exits_before_time_stop() -> None:
    engine = MomentumEngine("MOCK")
    for bar in OPENING_RANGE_BARS:
        await engine.on_bar(bar)

    entry_signal = await engine.on_bar(make_1m_bar(5, 102, 107, 102, 106))
    assert entry_signal is not None

    profit_bar = make_1m_bar(6, 106.5, 106.8, 106.4, 106.6)  # +0.57% >= 0.5% target
    exit_signal = await engine.on_bar(profit_bar)

    assert exit_signal is not None
    assert exit_signal.reason == "target_profit_reached"
    assert exit_signal.timestamp - entry_signal.timestamp == timedelta(minutes=1)


@pytest.mark.asyncio
async def test_momentum_engine_run_consumes_async_stream() -> None:
    engine = MomentumEngine("MOCK")

    async def bar_stream():
        for bar in OPENING_RANGE_BARS:
            yield bar
        yield make_1m_bar(5, 102, 107, 102, 106)

    signals = [signal async for signal in engine.run(bar_stream())]
    assert len(signals) == 1
    assert signals[0].action is SignalAction.BUY


# --- Swing Engine (trendline retest) --------------------------------------


def make_4h_bar(index: int, open_: float, high: float, low: float, close: float) -> OHLCVBar:
    return OHLCVBar(
        symbol="MOCK",
        timestamp=BASE_TIME + timedelta(hours=4 * index),
        timeframe=Timeframe.FOUR_HOUR,
        open=open_,
        high=high,
        low=low,
        close=close,
        volume=10_000,
    )


def test_is_bullish_engulfing_detects_pattern() -> None:
    bearish = make_4h_bar(0, 15.0, 15.2, 14.0, 14.2)
    bullish_engulfing = make_4h_bar(1, 14.0, 15.5, 13.9, 15.1)
    assert SwingEngine.is_bullish_engulfing(bearish, bullish_engulfing) is True


def test_is_bullish_engulfing_rejects_non_engulfing() -> None:
    bearish = make_4h_bar(0, 15.0, 15.2, 14.0, 14.2)
    small_bullish = make_4h_bar(1, 14.1, 14.4, 14.0, 14.3)  # doesn't engulf the prior body
    assert SwingEngine.is_bullish_engulfing(bearish, small_bullish) is False


def _neutral_bar(index: int, low: float) -> OHLCVBar:
    """A mildly-bearish filler bar whose 'low' is the only precisely-controlled value."""
    return make_4h_bar(index, low + 2, low + 3, low, low + 1)


def test_find_pivots_detects_local_trough() -> None:
    engine = SwingEngine("MOCK", pivot_window=1)
    lows = [20, 5, 20]
    for i, low in enumerate(lows):
        engine._bars.append(_neutral_bar(i, low))

    pivots = engine.find_pivots()
    troughs = [p for p in pivots if p.kind == "trough"]
    assert len(troughs) == 1
    assert troughs[0].index == 1
    assert troughs[0].price == 5


@pytest.mark.asyncio
async def test_trendline_retest_with_bullish_engulfing_triggers_alert() -> None:
    """Builds 3 collinear troughs (ascending support line) then a retest + engulfing candle."""
    engine = SwingEngine("MOCK", pivot_window=2, min_touches=3, touch_tolerance_pct=0.005)

    # Troughs at index 2 (10), 7 (12), 12 (14) lie exactly on: value = 0.4 * index + 9.2
    lows = {0: 20, 1: 15, 2: 10, 3: 15, 4: 20, 5: 20, 6: 15, 7: 12, 8: 15, 9: 20, 10: 20, 11: 15, 12: 14}

    last_signal = None
    for i in range(13):
        last_signal = await engine.on_bar(_neutral_bar(i, lows[i]))
        assert last_signal is None  # no alert should fire before the retest bar

    # Bearish bar 13, then a bullish engulfing bar 14 that closes exactly on the
    # projected trendline value at index 14 (0.4 * 14 + 9.2 = 14.8).
    bearish_bar = make_4h_bar(13, 14.6, 14.7, 14.1, 14.2)
    engulfing_bar = make_4h_bar(14, 14.15, 14.9, 14.05, 14.8)

    assert await engine.on_bar(bearish_bar) is None
    alert = await engine.on_bar(engulfing_bar)

    assert alert is not None
    assert alert.action is SignalAction.ALERT
    assert alert.reason == "trendline_retest_bullish_engulfing"
    assert alert.metadata["trendline_touches"] == 3
    assert alert.price == pytest.approx(14.8)


@pytest.mark.asyncio
async def test_alert_opens_position_closed_after_hold_period() -> None:
    """The hypothetical position opened on an ALERT should mark-to-market and
    close after `hold_period_bars`, feeding the strategy's equity curve."""
    engine = SwingEngine("MOCK", pivot_window=2, min_touches=3, touch_tolerance_pct=0.005, hold_period_bars=3)
    lows = {0: 20, 1: 15, 2: 10, 3: 15, 4: 20, 5: 20, 6: 15, 7: 12, 8: 15, 9: 20, 10: 20, 11: 15, 12: 14}

    for i in range(13):
        await engine.on_bar(_neutral_bar(i, lows[i]))

    await engine.on_bar(make_4h_bar(13, 14.6, 14.7, 14.1, 14.2))
    alert = await engine.on_bar(make_4h_bar(14, 14.15, 14.9, 14.05, 14.8))
    assert alert is not None and alert.action is SignalAction.ALERT

    assert await engine.on_bar(make_4h_bar(15, 15.0, 15.2, 14.8, 15.0)) is None
    assert await engine.on_bar(make_4h_bar(16, 15.0, 15.3, 14.9, 15.1)) is None
    exit_signal = await engine.on_bar(make_4h_bar(17, 15.1, 15.6, 15.0, 15.5))

    assert exit_signal is not None
    assert exit_signal.action is SignalAction.EXIT
    assert exit_signal.reason == "hold_period_3_bars"
    assert exit_signal.metadata["pnl"] == pytest.approx(15.5 - 14.8)
    assert engine.equity == pytest.approx(15.5 - 14.8)
    assert engine.equity_curve == [(exit_signal.timestamp, engine.equity)]


@pytest.mark.asyncio
async def test_no_alert_when_touches_below_minimum() -> None:
    """Only 2 troughs exist (below min_touches=3) so no valid trendline should form."""
    engine = SwingEngine("MOCK", pivot_window=2, min_touches=3, touch_tolerance_pct=0.005)
    lows = {0: 20, 1: 15, 2: 10, 3: 15, 4: 20, 5: 20, 6: 15, 7: 12, 8: 15, 9: 20, 10: 20}

    for i in range(11):
        signal = await engine.on_bar(_neutral_bar(i, lows[i]))
        assert signal is None

    assert engine.find_support_trendline() is None


@pytest.mark.asyncio
async def test_swing_engine_run_consumes_async_stream() -> None:
    engine = SwingEngine("MOCK", pivot_window=2, min_touches=3, touch_tolerance_pct=0.005)
    lows = {0: 20, 1: 15, 2: 10, 3: 15, 4: 20, 5: 20, 6: 15, 7: 12, 8: 15, 9: 20, 10: 20, 11: 15, 12: 14}

    async def bar_stream():
        for i in range(13):
            yield _neutral_bar(i, lows[i])
        yield make_4h_bar(13, 14.6, 14.7, 14.1, 14.2)
        yield make_4h_bar(14, 14.15, 14.9, 14.05, 14.8)

    signals = [signal async for signal in engine.run(bar_stream())]
    assert len(signals) == 1
    assert signals[0].action is SignalAction.ALERT
