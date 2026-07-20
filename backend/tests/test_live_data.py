"""Tests for the Phase 4 live data pipeline: Yahoo stock feed, CCXT crypto
feed, bar parsing/aggregation, and DATA_SOURCE_MODE stream-factory routing.

No test here touches the real network: the Yahoo path runs against
`httpx.MockTransport` and the CCXT path against an in-memory fake exchange.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime, timezone
from typing import Any

import httpx
import pytest

from backend.data_pipeline import (
    ResilientStream,
    StreamDisconnected,
    aggregate_bars,
    build_stream_factory,
    is_crypto_symbol,
    parse_yahoo_chart,
    resolve_data_source_mode,
    stream_live_crypto_bars,
    stream_live_stock_bars,
)
from backend.models import OHLCVBar, Timeframe

BASE_TS = 1_753_000_000  # aligned reference epoch for fabricated candles


def yahoo_payload(
    n: int,
    base_ts: int = BASE_TS,
    interval_s: int = 60,
    null_rows: set[int] | None = None,
) -> dict[str, Any]:
    nulls = null_rows or set()

    def col(offset: float) -> list[float | None]:
        return [None if i in nulls else offset + i for i in range(n)]

    return {
        "chart": {
            "result": [
                {
                    "timestamp": [base_ts + i * interval_s for i in range(n)],
                    "indicators": {
                        "quote": [
                            {
                                "open": col(100.0),
                                "high": col(101.0),
                                "low": col(99.0),
                                "close": col(100.5),
                                "volume": [1_000] * n,
                            }
                        ]
                    },
                }
            ]
        }
    }


def make_bar(hour: int, minute: int = 0, close: float = 100.0) -> OHLCVBar:
    return OHLCVBar(
        symbol="AAPL",
        timestamp=datetime(2026, 7, 20, hour, minute, tzinfo=timezone.utc),
        timeframe=Timeframe.ONE_MINUTE,
        open=close - 0.5,
        high=close + 1.0,
        low=close - 1.0,
        close=close,
        volume=1_000,
    )


class FakeExchange:
    """Minimal CCXT stand-in: serves scripted OHLCV pages, records calls."""

    def __init__(self, pages: list[list[list[Any]]] | None = None, error: Exception | None = None) -> None:
        base = 1_753_000_000_000
        self.pages = pages if pages is not None else [
            [[base + i * 60_000, 50_000.0 + i, 50_100.0 + i, 49_900.0 + i, 50_050.0 + i, 12.5] for i in range(3)],
            [[base + i * 60_000, 50_000.0 + i, 50_100.0 + i, 49_900.0 + i, 50_050.0 + i, 12.5] for i in range(2, 5)],
        ]
        self.error = error
        self.calls: list[tuple[str, str, int | None, int | None]] = []
        self.closed = False

    async def fetch_ohlcv(
        self, symbol: str, timeframe: str, since: int | None = None, limit: int | None = None
    ) -> list[list[float]]:
        if self.error is not None:
            raise self.error
        self.calls.append((symbol, timeframe, since, limit))
        index = min(len(self.calls) - 1, len(self.pages) - 1)
        return self.pages[index]

    async def close(self) -> None:
        self.closed = True


# --- mode / symbol resolution -------------------------------------------------


def test_resolve_data_source_mode_defaults_to_mock(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATA_SOURCE_MODE", raising=False)
    assert resolve_data_source_mode() == "mock"


def test_resolve_data_source_mode_reads_env_and_arg_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATA_SOURCE_MODE", "LIVE")
    assert resolve_data_source_mode() == "live"
    assert resolve_data_source_mode("MOCK") == "mock"  # explicit arg beats env


def test_resolve_data_source_mode_rejects_unknown() -> None:
    with pytest.raises(ValueError, match="DATA_SOURCE_MODE"):
        resolve_data_source_mode("paper")


def test_is_crypto_symbol_by_pair_shape() -> None:
    assert is_crypto_symbol("BTC/USDT") is True
    assert is_crypto_symbol("AAPL") is False
    assert is_crypto_symbol("BRK.B") is False


# --- Yahoo parsing ------------------------------------------------------------


def test_parse_yahoo_chart_produces_standard_bars() -> None:
    bars = parse_yahoo_chart(yahoo_payload(3), "AAPL", Timeframe.ONE_MINUTE)
    assert len(bars) == 3
    bar = bars[0]
    assert bar.symbol == "AAPL"
    assert bar.timeframe is Timeframe.ONE_MINUTE
    assert bar.timestamp == datetime.fromtimestamp(BASE_TS, tz=timezone.utc)
    assert (bar.open, bar.high, bar.low, bar.close, bar.volume) == (100.0, 101.0, 99.0, 100.5, 1_000)


def test_parse_yahoo_chart_skips_null_padded_rows() -> None:
    bars = parse_yahoo_chart(yahoo_payload(5, null_rows={1, 3}), "AAPL", Timeframe.ONE_MINUTE)
    assert len(bars) == 3
    assert all(bar.close is not None for bar in bars)


def test_parse_yahoo_chart_malformed_payload_raises_stream_disconnected() -> None:
    with pytest.raises(StreamDisconnected, match="malformed"):
        parse_yahoo_chart({"chart": {"result": []}}, "AAPL", Timeframe.ONE_MINUTE)
    with pytest.raises(StreamDisconnected):
        parse_yahoo_chart({}, "AAPL", Timeframe.ONE_MINUTE)


# --- 4h aggregation -----------------------------------------------------------


def test_aggregate_bars_builds_aligned_4h_buckets_and_holds_back_partial() -> None:
    # 9 hourly bars from 08:00: buckets 08:00 (4 bars), 12:00 (4 bars), 16:00 (1 bar, partial)
    hourly = [make_bar(8 + i, close=100.0 + i) for i in range(9)]
    aggregated = aggregate_bars(hourly, Timeframe.FOUR_HOUR)

    assert [bar.timestamp.hour for bar in aggregated] == [8, 12]  # 16:00 held back
    first = aggregated[0]
    assert first.timeframe is Timeframe.FOUR_HOUR
    assert first.open == hourly[0].open
    assert first.close == hourly[3].close
    assert first.high == max(b.high for b in hourly[:4])
    assert first.low == min(b.low for b in hourly[:4])
    assert first.volume == 4_000


def test_aggregate_bars_empty_input() -> None:
    assert aggregate_bars([], Timeframe.FOUR_HOUR) == []


# --- live stock stream (Yahoo via mocked httpx) -------------------------------


@pytest.mark.asyncio
async def test_stock_stream_backfills_then_yields_only_new_bars() -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        assert request.url.params["interval"] == "1m"
        assert "AAPL" in str(request.url)
        # poll 1: 3 bars; poll 2: same 3 plus 2 new
        return httpx.Response(200, json=yahoo_payload(3 if calls["n"] == 1 else 5))

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    bars = [
        bar
        async for bar in stream_live_stock_bars(
            "AAPL", Timeframe.ONE_MINUTE, client=client, poll_seconds=0.001, max_polls=2
        )
    ]
    assert len(bars) == 5  # 3 backfilled + 2 new, overlap deduped
    timestamps = [bar.timestamp for bar in bars]
    assert timestamps == sorted(timestamps) and len(set(timestamps)) == 5


@pytest.mark.asyncio
async def test_stock_4h_stream_polls_60m_and_aggregates() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["interval"] == "60m"
        return httpx.Response(200, json=yahoo_payload(9, interval_s=3_600))

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    bars = [
        bar
        async for bar in stream_live_stock_bars(
            "AAPL", Timeframe.FOUR_HOUR, client=client, poll_seconds=0.001, max_polls=1
        )
    ]
    assert bars, "expected aggregated 4h bars"
    assert all(bar.timeframe is Timeframe.FOUR_HOUR for bar in bars)
    assert all(bar.timestamp.hour % 4 == 0 for bar in bars)
    assert all(bar.volume == 4_000 for bar in bars)  # only complete 4x60m buckets


@pytest.mark.asyncio
async def test_stock_stream_http_error_raises_stream_disconnected() -> None:
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(500)))
    with pytest.raises(StreamDisconnected, match="Yahoo"):
        async for _bar in stream_live_stock_bars("AAPL", Timeframe.ONE_MINUTE, client=client, max_polls=1):
            pass


@pytest.mark.asyncio
async def test_stock_stream_network_failure_raises_stream_disconnected() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    with pytest.raises(StreamDisconnected):
        async for _bar in stream_live_stock_bars("AAPL", Timeframe.ONE_MINUTE, client=client, max_polls=1):
            pass


@pytest.mark.asyncio
async def test_resilient_stream_recovers_live_feed_after_outage() -> None:
    """The live feed and the Phase 3 reconnection machine compose: an HTTP
    outage surfaces as StreamDisconnected, ResilientStream backs off, and the
    next factory call succeeds."""
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 2:  # first reconnect attempt sees a dead API
            return httpx.Response(503)
        return httpx.Response(200, json=yahoo_payload(3 + calls["n"]))

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    def factory() -> AsyncIterator[OHLCVBar]:
        return stream_live_stock_bars("AAPL", Timeframe.ONE_MINUTE, client=client, poll_seconds=0.001, max_polls=2)

    stream = ResilientStream(factory, "AAPL", backoff_scale=0.0001)
    received: list[OHLCVBar] = []
    async for bar in stream.bars():
        received.append(bar)
        if len(received) >= 8:
            break

    assert len(received) >= 8
    assert stream.disconnect_count >= 1


# --- live crypto stream (CCXT via fake exchange) ------------------------------


@pytest.mark.asyncio
async def test_crypto_stream_parses_ccxt_candles() -> None:
    exchange = FakeExchange()
    bars = [
        bar
        async for bar in stream_live_crypto_bars(
            "BTC/USDT", Timeframe.ONE_MINUTE, exchange=exchange, poll_seconds=0.001, max_polls=1
        )
    ]
    assert len(bars) == 3
    bar = bars[0]
    assert bar.symbol == "BTC/USDT"
    assert bar.timeframe is Timeframe.ONE_MINUTE
    assert (bar.open, bar.high, bar.low, bar.close) == (50_000.0, 50_100.0, 49_900.0, 50_050.0)
    assert bar.volume == 12
    assert exchange.calls[0][1] == "1m"


@pytest.mark.asyncio
async def test_crypto_stream_paginates_with_since_and_dedupes() -> None:
    exchange = FakeExchange()
    bars = [
        bar
        async for bar in stream_live_crypto_bars(
            "BTC/USDT", Timeframe.ONE_MINUTE, exchange=exchange, poll_seconds=0.001, max_polls=2
        )
    ]
    # page 1: candles 0-2; page 2: candles 2-4 with candle 2 overlapping -> deduped
    assert len(bars) == 5
    timestamps = [bar.timestamp for bar in bars]
    assert timestamps == sorted(timestamps) and len(set(timestamps)) == 5
    second_call = exchange.calls[1]
    assert second_call[2] is not None  # since = last seen + 1


@pytest.mark.asyncio
async def test_crypto_stream_uses_4h_timeframe_string() -> None:
    exchange = FakeExchange()
    async for _bar in stream_live_crypto_bars(
        "BTC/USDT", Timeframe.FOUR_HOUR, exchange=exchange, poll_seconds=0.001, max_polls=1
    ):
        break
    assert exchange.calls[0][1] == "4h"


@pytest.mark.asyncio
async def test_crypto_stream_exchange_error_raises_stream_disconnected() -> None:
    exchange = FakeExchange(error=RuntimeError("exchange maintenance"))
    with pytest.raises(StreamDisconnected, match="CCXT"):
        async for _bar in stream_live_crypto_bars(
            "BTC/USDT", Timeframe.ONE_MINUTE, exchange=exchange, max_polls=1
        ):
            pass


@pytest.mark.asyncio
async def test_crypto_stream_malformed_candle_raises_stream_disconnected() -> None:
    exchange = FakeExchange(pages=[[[1_753_000_000_000, None, 1.0, 1.0, 1.0, 1.0]]])
    with pytest.raises(StreamDisconnected, match="malformed"):
        async for _bar in stream_live_crypto_bars(
            "BTC/USDT", Timeframe.ONE_MINUTE, exchange=exchange, max_polls=1
        ):
            pass


@pytest.mark.asyncio
async def test_crypto_stream_does_not_close_injected_exchange() -> None:
    """An injected exchange is owned by the caller (the app shares one across
    reconnects) — the stream must not close it on teardown."""
    exchange = FakeExchange()
    async for _bar in stream_live_crypto_bars(
        "BTC/USDT", Timeframe.ONE_MINUTE, exchange=exchange, poll_seconds=0.001, max_polls=1
    ):
        pass
    assert exchange.closed is False


# --- stream factory routing ---------------------------------------------------


@pytest.mark.asyncio
async def test_factory_mock_mode_yields_generator_bars() -> None:
    factory = build_stream_factory("MOCK", Timeframe.ONE_MINUTE, mode="mock")
    received = []
    async for bar in factory():
        received.append(bar)
        if len(received) >= 3:
            break
    assert all(bar.symbol == "MOCK" and bar.timeframe is Timeframe.ONE_MINUTE for bar in received)


@pytest.mark.asyncio
async def test_factory_live_mode_routes_crypto_pairs_to_ccxt() -> None:
    exchange = FakeExchange()
    factory = build_stream_factory(
        "BTC/USDT", Timeframe.ONE_MINUTE, mode="live", exchange=exchange, poll_seconds=0.001
    )
    async for _bar in factory():
        break
    assert exchange.calls, "live crypto factory must poll the CCXT exchange"


@pytest.mark.asyncio
async def test_factory_live_mode_routes_stock_tickers_to_yahoo() -> None:
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(str(request.url))
        return httpx.Response(200, json=yahoo_payload(3))

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    factory = build_stream_factory(
        "TSLA", Timeframe.ONE_MINUTE, mode="live", http_client=client, poll_seconds=0.001
    )
    async for _bar in factory():
        break
    assert seen and "TSLA" in seen[0]


def test_factory_rejects_unknown_mode() -> None:
    with pytest.raises(ValueError):
        build_stream_factory("AAPL", Timeframe.ONE_MINUTE, mode="hybrid")
