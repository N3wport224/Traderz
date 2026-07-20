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
import os
from collections.abc import AsyncIterator, Callable
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any

import httpx
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


# --- Phase 4: live market data feeds -----------------------------------------
#
# Two live sources, selected per symbol shape when DATA_SOURCE_MODE=live:
#   - crypto pairs ("BTC/USDT" — anything with a slash) poll public OHLCV
#     candles through CCXT (no credentials needed for market data);
#   - stock tickers ("AAPL") poll Yahoo Finance's public chart API via httpx.
# Both are plain async generators shaped exactly like the mock streams, and
# both convert any transport/parse failure into `StreamDisconnected` so the
# `ResilientStream` state machine handles live outages the same way it handles
# simulated ones. Live data NEVER changes execution: the gateway stays
# `MockExecutionGateway` unless GATEWAY_MODE=live is set explicitly (paper
# trading by default — real charts, simulated fills).

YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
# Yahoo rejects default library user agents; a browser-ish UA is required.
YAHOO_HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Traderz/4.0"}

# Yahoo has no native 4h interval: the 4h stream aggregates 60m candles.
_YAHOO_PARAMS: dict[Timeframe, dict[str, str]] = {
    Timeframe.ONE_MINUTE: {"interval": "1m", "range": "1d"},
    Timeframe.FOUR_HOUR: {"interval": "60m", "range": "1mo"},
}
_CCXT_TIMEFRAMES: dict[Timeframe, str] = {
    Timeframe.ONE_MINUTE: "1m",
    Timeframe.FOUR_HOUR: "4h",
}
DEFAULT_POLL_SECONDS: dict[Timeframe, float] = {
    Timeframe.ONE_MINUTE: 30.0,
    Timeframe.FOUR_HOUR: 300.0,
}

DATA_SOURCE_MODES = ("mock", "live")


def resolve_data_source_mode(mode: str | None = None) -> str:
    """Normalizes DATA_SOURCE_MODE (arg wins over env; default mock)."""
    resolved = (mode or os.environ.get("DATA_SOURCE_MODE", "mock")).lower()
    if resolved not in DATA_SOURCE_MODES:
        raise ValueError(f"DATA_SOURCE_MODE must be one of {DATA_SOURCE_MODES}, got {resolved!r}")
    return resolved


def is_crypto_symbol(symbol: str) -> bool:
    """CCXT market symbols are pair-shaped ("BTC/USDT"); stocks are bare tickers."""
    return "/" in symbol


def parse_yahoo_chart(payload: dict[str, Any], symbol: str, timeframe: Timeframe) -> list[OHLCVBar]:
    """Converts a Yahoo v8 chart payload into OHLCVBars, skipping null rows
    (Yahoo pads halted/thin minutes with nulls)."""
    try:
        result = payload["chart"]["result"][0]
        timestamps: list[int | None] = result.get("timestamp") or []
        quote = result["indicators"]["quote"][0]
    except (KeyError, IndexError, TypeError) as exc:
        raise StreamDisconnected(f"malformed Yahoo chart payload for {symbol}: {exc}") from exc

    bars: list[OHLCVBar] = []
    opens = quote.get("open") or []
    highs = quote.get("high") or []
    lows = quote.get("low") or []
    closes = quote.get("close") or []
    volumes = quote.get("volume") or []
    for i, ts in enumerate(timestamps):
        row = (ts, opens[i], highs[i], lows[i], closes[i])
        if any(value is None for value in row):
            continue
        volume = volumes[i] if i < len(volumes) and volumes[i] is not None else 0
        bars.append(
            OHLCVBar(
                symbol=symbol,
                timestamp=datetime.fromtimestamp(int(ts), tz=timezone.utc),  # type: ignore[arg-type]
                timeframe=timeframe,
                open=round(float(opens[i]), 6),
                high=round(float(highs[i]), 6),
                low=round(float(lows[i]), 6),
                close=round(float(closes[i]), 6),
                volume=int(volume),
            )
        )
    return bars


def aggregate_bars(bars: list[OHLCVBar], timeframe: Timeframe, bucket_hours: int = 4) -> list[OHLCVBar]:
    """Aggregates finer bars into `bucket_hours`-aligned buckets (e.g. 60m -> 4h).

    Only *completed* buckets are returned — the trailing partial bucket is held
    back until later input proves a newer bucket has started, so the swing
    engine never sees a half-built candle that would mutate retroactively.
    """
    if not bars:
        return []
    buckets: dict[datetime, list[OHLCVBar]] = {}
    for bar in bars:
        start = bar.timestamp.replace(minute=0, second=0, microsecond=0)
        start = start.replace(hour=start.hour - start.hour % bucket_hours)
        buckets.setdefault(start, []).append(bar)

    ordered_starts = sorted(buckets)
    completed = ordered_starts[:-1]  # last bucket may still be filling
    aggregated: list[OHLCVBar] = []
    for start in completed:
        chunk = sorted(buckets[start], key=lambda b: b.timestamp)
        aggregated.append(
            OHLCVBar(
                symbol=chunk[0].symbol,
                timestamp=start,
                timeframe=timeframe,
                open=chunk[0].open,
                high=max(b.high for b in chunk),
                low=min(b.low for b in chunk),
                close=chunk[-1].close,
                volume=sum(b.volume for b in chunk),
            )
        )
    return aggregated


async def stream_live_stock_bars(
    symbol: str,
    timeframe: Timeframe,
    *,
    client: httpx.AsyncClient | None = None,
    poll_seconds: float | None = None,
    max_polls: int | None = None,
) -> AsyncIterator[OHLCVBar]:
    """Polls Yahoo Finance's public chart API and yields new standardized bars.

    The first poll backfills history (a day of 1m candles / a month of 60m
    candles aggregated to 4h) so the engines have context immediately; later
    polls yield only bars newer than the last one seen. Any HTTP/parse failure
    raises `StreamDisconnected` for the `ResilientStream` wrapper to absorb.
    """
    delay = poll_seconds if poll_seconds is not None else DEFAULT_POLL_SECONDS[timeframe]
    own_client = client is None
    http = client or httpx.AsyncClient(timeout=10.0)
    last_seen: datetime | None = None
    polls = 0
    try:
        while max_polls is None or polls < max_polls:
            try:
                response = await http.get(
                    YAHOO_CHART_URL.format(symbol=symbol),
                    params=_YAHOO_PARAMS[timeframe],
                    headers=YAHOO_HEADERS,
                )
                response.raise_for_status()
                raw_bars = parse_yahoo_chart(response.json(), symbol, timeframe)
            except httpx.HTTPError as exc:
                raise StreamDisconnected(f"Yahoo chart request failed for {symbol}: {exc}") from exc
            except ValueError as exc:  # response.json() decode failure
                raise StreamDisconnected(f"Yahoo chart returned non-JSON for {symbol}: {exc}") from exc

            if timeframe is Timeframe.FOUR_HOUR:
                raw_bars = aggregate_bars(raw_bars, timeframe)
            for bar in raw_bars:
                if last_seen is None or bar.timestamp > last_seen:
                    last_seen = bar.timestamp
                    yield bar

            polls += 1
            if max_polls is not None and polls >= max_polls:
                return
            await asyncio.sleep(delay)
    finally:
        if own_client:
            await http.aclose()


async def stream_live_crypto_bars(
    symbol: str,
    timeframe: Timeframe,
    *,
    exchange: Any | None = None,
    exchange_id: str = "binance",
    poll_seconds: float | None = None,
    max_polls: int | None = None,
    backfill_limit: int = 300,
) -> AsyncIterator[OHLCVBar]:
    """Polls public OHLCV candles for a crypto pair through CCXT.

    Market data needs no credentials — this constructs a keyless exchange
    client (lazily importing ccxt, mirroring `LiveCCXTExecutionGateway`) unless
    a pre-built one is injected (tests inject a fake). CCXT rows are
    `[ms, open, high, low, close, volume]`. Failures raise `StreamDisconnected`.
    """
    delay = poll_seconds if poll_seconds is not None else DEFAULT_POLL_SECONDS[timeframe]
    own_exchange = exchange is None
    live_exchange: Any = exchange
    if live_exchange is None:
        try:
            import ccxt.async_support as ccxt_async
        except ImportError as exc:
            raise StreamDisconnected(
                "live crypto data requires the ccxt package: pip install ccxt"
            ) from exc
        try:
            live_exchange = getattr(ccxt_async, exchange_id)({"enableRateLimit": True})
        except AttributeError as exc:
            raise StreamDisconnected(f"unknown CCXT exchange id: {exchange_id}") from exc

    timeframe_str = _CCXT_TIMEFRAMES[timeframe]
    last_seen_ms: int | None = None
    polls = 0
    try:
        while max_polls is None or polls < max_polls:
            since = None if last_seen_ms is None else last_seen_ms + 1
            limit = backfill_limit if last_seen_ms is None else 100
            try:
                rows: list[list[float]] = await live_exchange.fetch_ohlcv(
                    symbol, timeframe=timeframe_str, since=since, limit=limit
                )
            except Exception as exc:  # ccxt raises its own network/exchange error tree
                raise StreamDisconnected(f"CCXT OHLCV fetch failed for {symbol}: {exc}") from exc

            for row in rows:
                try:
                    ts_ms, open_, high, low, close, volume = (
                        int(row[0]), float(row[1]), float(row[2]), float(row[3]), float(row[4]),
                        float(row[5]) if len(row) > 5 and row[5] is not None else 0.0,
                    )
                except (TypeError, ValueError, IndexError) as exc:
                    raise StreamDisconnected(f"malformed CCXT candle for {symbol}: {row!r}") from exc
                if last_seen_ms is not None and ts_ms <= last_seen_ms:
                    continue
                last_seen_ms = ts_ms
                yield OHLCVBar(
                    symbol=symbol,
                    timestamp=datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc),
                    timeframe=timeframe,
                    open=open_,
                    high=high,
                    low=low,
                    close=close,
                    volume=int(volume),
                )

            polls += 1
            if max_polls is not None and polls >= max_polls:
                return
            await asyncio.sleep(delay)
    finally:
        if own_exchange and live_exchange is not None and hasattr(live_exchange, "close"):
            await live_exchange.close()


def build_stream_factory(
    symbol: str,
    timeframe: Timeframe,
    *,
    mode: str = "mock",
    interval_seconds: float = 0.0,
    poll_seconds: float | None = None,
    http_client: httpx.AsyncClient | None = None,
    exchange: Any | None = None,
    exchange_id: str = "binance",
) -> Callable[[], AsyncIterator[OHLCVBar]]:
    """Builds a `ResilientStream`-compatible stream factory for one symbol/timeframe.

    mode="mock": the existing random-walk generator (one generator shared across
    reconnects so the walk continues). mode="live": Yahoo for stock tickers,
    CCXT public data for crypto pairs. `http_client` / `exchange` exist so tests
    (and the composition root) can inject transports — unit tests must never
    touch the real network.
    """
    resolved = resolve_data_source_mode(mode)
    if resolved == "mock":
        generator = MockOHLCVGenerator(symbol)

        def mock_factory() -> AsyncIterator[OHLCVBar]:
            stream_fn = stream_1m_bars if timeframe is Timeframe.ONE_MINUTE else stream_4h_bars
            return stream_fn(symbol, interval_seconds=interval_seconds, generator=generator)

        return mock_factory

    if is_crypto_symbol(symbol):

        def crypto_factory() -> AsyncIterator[OHLCVBar]:
            return stream_live_crypto_bars(
                symbol, timeframe, exchange=exchange, exchange_id=exchange_id, poll_seconds=poll_seconds
            )

        return crypto_factory

    def stock_factory() -> AsyncIterator[OHLCVBar]:
        return stream_live_stock_bars(symbol, timeframe, client=http_client, poll_seconds=poll_seconds)

    return stock_factory
