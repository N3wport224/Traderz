"""Verification tests for backend/data_pipeline.py."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from backend.data_pipeline import (
    MockOHLCVGenerator,
    bars_to_dataframe,
    next_market_open,
    stream_1m_bars,
    stream_4h_bars,
)
from backend.models import OHLCVBar, Timeframe


def test_next_market_open_advances_to_future() -> None:
    now = datetime(2026, 7, 20, 15, 0, tzinfo=timezone.utc)
    opening = next_market_open(now)
    assert opening > now
    assert opening.hour == 9 and opening.minute == 30


def test_generator_produces_consistent_ohlc_bounds() -> None:
    generator = MockOHLCVGenerator("MOCK", start_price=100.0, seed=42)
    timestamp = datetime(2026, 7, 20, 9, 30, tzinfo=timezone.utc)
    for _ in range(50):
        bar = generator.next_bar(timestamp, Timeframe.ONE_MINUTE)
        assert bar.high >= bar.open
        assert bar.high >= bar.close
        assert bar.low <= bar.open
        assert bar.low <= bar.close
        assert bar.volume > 0
        timestamp += timedelta(minutes=1)


@pytest.mark.asyncio
async def test_stream_1m_bars_yields_one_minute_increments() -> None:
    start = datetime(2026, 7, 20, 9, 30, tzinfo=timezone.utc)
    collected: list[OHLCVBar] = []
    async for bar in stream_1m_bars("MOCK", bars=10, start_time=start, generator=MockOHLCVGenerator("MOCK", seed=1)):
        collected.append(bar)

    assert len(collected) == 10
    assert all(bar.timeframe is Timeframe.ONE_MINUTE for bar in collected)
    for prev, curr in zip(collected, collected[1:]):
        assert curr.timestamp - prev.timestamp == timedelta(minutes=1)


@pytest.mark.asyncio
async def test_stream_4h_bars_yields_four_hour_increments() -> None:
    start = datetime(2026, 7, 20, 9, 30, tzinfo=timezone.utc)
    collected: list[OHLCVBar] = []
    async for bar in stream_4h_bars("MOCK", bars=6, start_time=start, generator=MockOHLCVGenerator("MOCK", seed=2)):
        collected.append(bar)

    assert len(collected) == 6
    assert all(bar.timeframe is Timeframe.FOUR_HOUR for bar in collected)
    for prev, curr in zip(collected, collected[1:]):
        assert curr.timestamp - prev.timestamp == timedelta(hours=4)


@pytest.mark.asyncio
async def test_1m_and_4h_streams_are_independent_and_concurrent() -> None:
    """Both schedules must be independently drivable without blocking each other."""
    start = datetime(2026, 7, 20, 9, 30, tzinfo=timezone.utc)

    async def collect_1m() -> list[OHLCVBar]:
        return [
            bar
            async for bar in stream_1m_bars(
                "MOCK", bars=5, start_time=start, generator=MockOHLCVGenerator("MOCK", seed=3)
            )
        ]

    async def collect_4h() -> list[OHLCVBar]:
        return [
            bar
            async for bar in stream_4h_bars(
                "MOCK", bars=5, start_time=start, generator=MockOHLCVGenerator("MOCK", seed=4)
            )
        ]

    import asyncio

    one_min_bars, four_hour_bars = await asyncio.gather(collect_1m(), collect_4h())
    assert len(one_min_bars) == 5
    assert len(four_hour_bars) == 5
    assert one_min_bars[-1].timestamp - one_min_bars[0].timestamp == timedelta(minutes=4)
    assert four_hour_bars[-1].timestamp - four_hour_bars[0].timestamp == timedelta(hours=16)


def test_bars_to_dataframe_shape_and_index() -> None:
    generator = MockOHLCVGenerator("MOCK", seed=5)
    timestamp = datetime(2026, 7, 20, 9, 30, tzinfo=timezone.utc)
    bars = []
    for _ in range(5):
        bars.append(generator.next_bar(timestamp, Timeframe.ONE_MINUTE))
        timestamp += timedelta(minutes=1)

    df = bars_to_dataframe(bars)
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["open", "high", "low", "close", "volume"]
    assert len(df) == 5
    assert df.index.name == "timestamp"


def test_bars_to_dataframe_empty_input() -> None:
    df = bars_to_dataframe([])
    assert df.empty
    assert list(df.columns) == ["open", "high", "low", "close", "volume"]
