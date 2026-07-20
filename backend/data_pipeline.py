"""Async mock market-data ingestion engine.

Simulates a live OHLCV feed for a symbol using a random-walk price model and
exposes it as two independently-paced async generators: a 1-minute stream for
the Day Trading (Momentum) Engine and a 4-hour stream for the Swing Engine.
Neither stream blocks the other — both are plain async generators intended to
be driven by separate `asyncio` tasks.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Callable
from datetime import datetime, timedelta, timezone
from enum import Enum

import numpy as np
import pandas as pd

from backend.models import OHLCVBar, SignalAction, Timeframe
from backend.notifier import Notifier
from backend.risk_manager import RiskManager

logger = logging.getLogger("traderz.data_pipeline")

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


class StreamDisconnected(Exception):
    """Raised by a live/simulated feed when its connection drops mid-stream."""


class StreamState(str, Enum):
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    RECONNECTING = "reconnecting"
    VERIFYING = "verifying"


async def flaky_stream(
    base: AsyncIterator[OHLCVBar],
    *,
    drop_after: list[int],
) -> AsyncIterator[OHLCVBar]:
    """Test/simulation wrapper: raises `StreamDisconnected` after yielding the
    bar counts listed in `drop_after` (cumulative across reconnects is up to the
    caller — each `flaky_stream` instance counts its own yields)."""
    schedule = list(drop_after)
    count = 0
    async for bar in base:
        yield bar
        count += 1
        if schedule and count >= schedule[0]:
            schedule.pop(0)
            raise StreamDisconnected(f"simulated drop after {count} bars")


class ResilientStream:
    """Reconnection state machine wrapping a raw OHLCV stream.

    States: CONNECTED -> DISCONNECTED (drop detected) -> RECONNECTING
    (exponential backoff: 2s, 4s, 8s, ... capped at 64s) -> VERIFYING (a fresh
    stream must deliver `verify_bars` clean bars before it is trusted) ->
    CONNECTED. On every drop the notifier is alerted and the shared
    `RiskManager` marks the ticker DATA_DISCONNECTED so engines stop evaluating
    it; both are reversed only after verification passes. The backoff delay
    resets only after a *verified* reconnection — a stream that keeps dying
    during verification keeps backing off further, it does not get a fresh 2s.

    `backoff_scale` exists for tests: it multiplies every delay so a suite can
    exercise the full 2->64 progression in milliseconds of wall-clock time.
    """

    def __init__(
        self,
        stream_factory: Callable[[], AsyncIterator[OHLCVBar]],
        ticker: str,
        *,
        risk_manager: RiskManager | None = None,
        notifier: Notifier | None = None,
        backoff_initial: float = 2.0,
        backoff_max: float = 64.0,
        backoff_factor: float = 2.0,
        backoff_scale: float = 1.0,
        verify_bars: int = 1,
        max_retries: int | None = None,
    ) -> None:
        self._stream_factory = stream_factory
        self.ticker = ticker
        self._risk_manager = risk_manager
        self._notifier = notifier
        self.backoff_initial = backoff_initial
        self.backoff_max = backoff_max
        self.backoff_factor = backoff_factor
        self.backoff_scale = backoff_scale
        self.verify_bars = verify_bars
        self.max_retries = max_retries

        self.state: StreamState = StreamState.CONNECTED
        self.disconnect_count = 0
        self.reconnect_attempts = 0
        self._current_backoff = backoff_initial
        self.delays_used: list[float] = []

    def _next_delay(self) -> float:
        delay = self._current_backoff
        self._current_backoff = min(self._current_backoff * self.backoff_factor, self.backoff_max)
        self.delays_used.append(delay)
        return delay * self.backoff_scale

    def _reset_backoff(self) -> None:
        self._current_backoff = self.backoff_initial

    async def _on_disconnect(self, exc: StreamDisconnected) -> None:
        self.state = StreamState.DISCONNECTED
        self.disconnect_count += 1
        logger.warning("data stream for %s disconnected: %s", self.ticker, exc)
        if self._risk_manager is not None:
            self._risk_manager.mark_data_disconnected(self.ticker)
        if self._notifier is not None:
            await self._notifier.notify_data_event(
                SignalAction.DATA_DISCONNECTED,
                self.ticker,
                datetime.now(timezone.utc),
                f"stream_disconnected: {exc}",
            )

    async def _on_verified(self) -> None:
        self.state = StreamState.CONNECTED
        self._reset_backoff()
        logger.info("data stream for %s reconnected and verified", self.ticker)
        if self._risk_manager is not None:
            self._risk_manager.mark_data_verified(self.ticker)
        if self._notifier is not None:
            await self._notifier.notify_data_event(
                SignalAction.DATA_RECONNECTED,
                self.ticker,
                datetime.now(timezone.utc),
                f"stream_verified_after_{self.disconnect_count}_disconnects",
            )

    async def bars(self) -> AsyncIterator[OHLCVBar]:
        """Yields bars from the wrapped stream, reconnecting transparently.

        Unverified bars are never yielded: after a reconnect, the first
        `verify_bars` bars are buffered, and only once they all arrive cleanly
        (verification passes) are they released downstream.
        """
        retries = 0
        needs_verification = False
        while True:
            stream = self._stream_factory()
            pending: list[OHLCVBar] = []
            verified_count = 0
            try:
                async for bar in stream:
                    if needs_verification:
                        pending.append(bar)
                        verified_count += 1
                        if verified_count >= self.verify_bars:
                            await self._on_verified()
                            needs_verification = False
                            retries = 0
                            for buffered in pending:
                                yield buffered
                            pending = []
                        continue
                    yield bar
                return  # underlying stream finished normally (bounded runs)
            except StreamDisconnected as exc:
                await self._on_disconnect(exc)
                retries += 1
                if self.max_retries is not None and retries > self.max_retries:
                    raise
                self.state = StreamState.RECONNECTING
                self.reconnect_attempts += 1
                await asyncio.sleep(self._next_delay())
                self.state = StreamState.VERIFYING
                needs_verification = True


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
