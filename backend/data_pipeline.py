"""Async mock market-data ingestion engine.

Simulates a live OHLCV feed for a symbol using a random-walk price model and
exposes it as two independently-paced async generators: a 1-minute stream for
the Day Trading (Momentum) Engine and a 4-hour stream for the Swing Engine.
Neither stream blocks the other — both are plain async generators intended to
be driven by separate `asyncio` tasks.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

from backend.models import OHLCVBar, Timeframe

MARKET_OPEN_HOUR = 9
MARKET_OPEN_MINUTE = 30


class MockOHLCVGenerator:
    """Stateful random-walk OHLCV bar generator for a single symbol."""

    def __init__(
        self,
        symbol: str,
        start_price: float = 100.0,
        volatility: float = 0.002,
        seed: int | None = None,
    ) -> None:
        self.symbol = symbol
        self.volatility = volatility
        self._last_close = start_price
        self._rng = np.random.default_rng(seed)

    def next_bar(self, timestamp: datetime, timeframe: Timeframe) -> OHLCVBar:
        """Synthesizes the next bar from the current close using a small random walk."""
        scale = self.volatility * (4.0 if timeframe is Timeframe.FOUR_HOUR else 1.0)
        open_price = self._last_close
        pct_moves = self._rng.normal(loc=0.0, scale=scale, size=4)
        path = open_price * (1.0 + np.cumsum(pct_moves))
        close_price = float(path[-1])
        high_price = float(max(open_price, close_price, path.max()))
        low_price = float(min(open_price, close_price, path.min()))
        volume = int(self._rng.integers(1_000, 50_000))

        self._last_close = close_price
        return OHLCVBar(
            symbol=self.symbol,
            timestamp=timestamp,
            timeframe=timeframe,
            open=round(open_price, 4),
            high=round(high_price, 4),
            low=round(low_price, 4),
            close=round(close_price, 4),
            volume=volume,
        )


def next_market_open(after: datetime | None = None) -> datetime:
    """Returns the next 9:30 market-open timestamp strictly after `after` (UTC)."""
    now = after or datetime.now(timezone.utc)
    candidate = now.replace(hour=MARKET_OPEN_HOUR, minute=MARKET_OPEN_MINUTE, second=0, microsecond=0)
    if candidate <= now:
        candidate += timedelta(days=1)
    return candidate


async def stream_1m_bars(
    symbol: str = "MOCK",
    *,
    bars: int | None = None,
    interval_seconds: float = 0.0,
    start_time: datetime | None = None,
    generator: MockOHLCVGenerator | None = None,
) -> AsyncIterator[OHLCVBar]:
    """Async generator yielding simulated 1-minute OHLCV bars, one per tick.

    Feeds the Day Trading Engine. `interval_seconds` paces real-time playback;
    pass 0 (default) for tests / batch backtesting to advance as fast as possible.
    """
    gen = generator or MockOHLCVGenerator(symbol)
    timestamp = start_time or next_market_open()
    count = 0
    while bars is None or count < bars:
        yield gen.next_bar(timestamp, Timeframe.ONE_MINUTE)
        timestamp += timedelta(minutes=1)
        count += 1
        await asyncio.sleep(interval_seconds)


async def stream_4h_bars(
    symbol: str = "MOCK",
    *,
    bars: int | None = None,
    interval_seconds: float = 0.0,
    start_time: datetime | None = None,
    generator: MockOHLCVGenerator | None = None,
) -> AsyncIterator[OHLCVBar]:
    """Async generator yielding simulated 4-hour OHLCV bars, one per tick.

    Feeds the Swing Trading Engine, independently paced from the 1-minute stream.
    """
    gen = generator or MockOHLCVGenerator(symbol)
    timestamp = start_time or next_market_open()
    count = 0
    while bars is None or count < bars:
        yield gen.next_bar(timestamp, Timeframe.FOUR_HOUR)
        timestamp += timedelta(hours=4)
        count += 1
        await asyncio.sleep(interval_seconds)


def bars_to_dataframe(bars: list[OHLCVBar]) -> pd.DataFrame:
    """Converts a list of OHLCVBar into a pandas DataFrame indexed by timestamp."""
    columns = ["open", "high", "low", "close", "volume"]
    if not bars:
        return pd.DataFrame(columns=columns)
    records = [
        {
            "timestamp": bar.timestamp,
            "open": bar.open,
            "high": bar.high,
            "low": bar.low,
            "close": bar.close,
            "volume": bar.volume,
        }
        for bar in bars
    ]
    return pd.DataFrame.from_records(records).set_index("timestamp")[columns]
