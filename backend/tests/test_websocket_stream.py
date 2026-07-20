"""Tests for the Phase 7 WebSocket ingestion pipeline: kline frame parsing,
the resilient pub/sub hub (multiplexing, reconnect backoff, overflow,
termination), and latency tracking. All against a scripted fake provider —
no test touches the real network."""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import AsyncIterator
from typing import Any

import pytest

from backend.data_pipeline import (
    BinanceKlineProvider,
    StreamDisconnected,
    StreamState,
    WebSocketStreamFactory,
    frame_event_latency_ms,
    parse_kline_frame,
)
from backend.models import OHLCVBar, Timeframe

BASE_MS = 1_753_000_000_000
FAST = 0.001  # backoff scale for tests


def kline(
    i: int,
    *,
    closed: bool = True,
    open_: float = 100.0,
    close: float = 100.5,
    event_lag_ms: float = 0.0,
) -> str:
    return json.dumps(
        {
            "e": "kline",
            "E": time.time() * 1000.0 - event_lag_ms if event_lag_ms else BASE_MS + i * 60_000,
            "k": {
                "t": BASE_MS + i * 60_000,
                "o": str(open_ + i),
                "h": str(open_ + i + 1.0),
                "l": str(open_ + i - 1.0),
                "c": str(close + i),
                "v": "12.5",
                "x": closed,
            },
        }
    )


class FakeProvider:
    """Scripted sessions: each connect yields its frames (or raises mid-way),
    then simulates the server closing the socket. An exhausted script idles."""

    def __init__(self, sessions: list[list[Any]]) -> None:
        self.sessions = list(sessions)
        self.connects = 0

    async def frames(self) -> AsyncIterator[str]:
        self.connects += 1
        if not self.sessions:
            await asyncio.sleep(3600)
        session = self.sessions.pop(0)
        for item in session:
            if isinstance(item, Exception):
                raise item
            yield item
            await asyncio.sleep(0)
        raise ConnectionError("server closed")


async def drain(stream: Any, count: int) -> list[OHLCVBar]:
    """Reads `count` bars from an already-created subscription, then closes it
    (explicit aclose so the subscriber deregisters deterministically)."""
    received: list[OHLCVBar] = []
    try:
        async for bar in stream:
            received.append(bar)
            if len(received) >= count:
                break
    finally:
        await stream.aclose()
    return received


# --- frame parsing ------------------------------------------------------------


def test_parse_closed_kline_into_standard_bar() -> None:
    bar = parse_kline_frame(kline(0), "BTC/USDT")
    assert bar is not None
    assert bar.symbol == "BTC/USDT"
    assert bar.timeframe is Timeframe.ONE_MINUTE
    assert (bar.open, bar.high, bar.low, bar.close) == (100.0, 101.0, 99.0, 100.5)
    assert bar.volume == 12
    assert bar.timestamp.timestamp() == BASE_MS / 1000.0


def test_parse_skips_forming_candles_and_non_kline_events() -> None:
    assert parse_kline_frame(kline(0, closed=False), "BTC/USDT") is None  # still forming
    assert parse_kline_frame(json.dumps({"e": "trade", "p": "1"}), "BTC/USDT") is None
    assert parse_kline_frame("not json at all", "BTC/USDT") is None
    assert parse_kline_frame(json.dumps({"k": {"x": True}}), "BTC/USDT") is None  # missing fields


def test_frame_event_latency_measures_now_minus_event_time() -> None:
    lagged = kline(0, event_lag_ms=250.0)
    latency = frame_event_latency_ms(lagged)
    assert latency is not None and 200.0 <= latency <= 5_000.0
    assert frame_event_latency_ms(json.dumps({"no": "E"})) is None
    assert frame_event_latency_ms("garbage") is None


def test_binance_provider_builds_keyless_stream_url() -> None:
    provider = BinanceKlineProvider("BTC/USDT")
    assert provider.url == "wss://stream.binance.com:9443/ws/btcusdt@kline_1m"


# --- pub/sub multiplexing ------------------------------------------------------


@pytest.mark.asyncio
async def test_hub_multiplexes_bars_to_multiple_engines_simultaneously() -> None:
    provider = FakeProvider([[kline(i) for i in range(6)]])
    hub = WebSocketStreamFactory("BTC/USDT", provider, backoff_scale=FAST)
    momentum_stream = hub.subscribe("momentum")  # queues register at subscribe()
    swing_stream = hub.subscribe("swing")
    await hub.start()

    momentum_bars, swing_bars = await asyncio.gather(
        drain(momentum_stream, 6), drain(swing_stream, 6)
    )
    assert [b.timestamp for b in momentum_bars] == [b.timestamp for b in swing_bars]
    assert [b.close for b in momentum_bars] == [b.close for b in swing_bars]
    assert hub.bars_received == 6
    await hub.stop()


@pytest.mark.asyncio
async def test_forming_candles_are_not_broadcast() -> None:
    provider = FakeProvider([[kline(0, closed=False), kline(0), kline(1, closed=False), kline(1)]])
    hub = WebSocketStreamFactory("BTC/USDT", provider, backoff_scale=FAST)
    stream = hub.subscribe("engine")
    await hub.start()
    bars = await drain(stream, 2)
    assert len(bars) == 2
    assert hub.frames_received >= 4  # all frames seen, only closed ones emitted
    await hub.stop()


# --- resilience ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_connection_drop_reconnects_and_bars_keep_flowing() -> None:
    events = {"down": 0, "up": 0}
    provider = FakeProvider([
        [kline(0), kline(1)],
        [kline(2), kline(3), kline(4), kline(5)],
    ])
    hub = WebSocketStreamFactory(
        "BTC/USDT",
        provider,
        backoff_scale=FAST,
        on_disconnect=lambda exc: events.__setitem__("down", events["down"] + 1),
        on_reconnect=lambda: events.__setitem__("up", events["up"] + 1),
    )
    stream = hub.subscribe("engine")
    await hub.start()
    bars = await drain(stream, 6)

    assert len(bars) == 6
    assert hub.disconnect_count >= 1
    assert events["down"] >= 1 and events["up"] >= 1  # risk-manager hooks fired both ways
    assert provider.connects >= 2
    assert hub.state is StreamState.CONNECTED
    await hub.stop()


@pytest.mark.asyncio
async def test_exhausted_retries_end_subscribers_with_stream_disconnected() -> None:
    provider = FakeProvider([[ConnectionError("down 1")], [ConnectionError("down 2")]])
    hub = WebSocketStreamFactory("BTC/USDT", provider, backoff_scale=FAST, max_retries=1)
    stream = hub.subscribe("engine")
    await hub.start()
    with pytest.raises(StreamDisconnected):
        async for _bar in stream:
            pass
    assert hub.state is StreamState.DISCONNECTED
    await hub.stop()


@pytest.mark.asyncio
async def test_slow_subscriber_overflow_drops_oldest_without_blocking() -> None:
    """A lagging engine loses its own oldest bars; the pump and other
    subscribers are never blocked, and the termination sentinel still lands."""
    provider = FakeProvider([[kline(i) for i in range(10)]])
    hub = WebSocketStreamFactory("BTC/USDT", provider, queue_maxsize=3, backoff_scale=FAST, max_retries=0)
    stream = hub.subscribe("laggard")
    await hub.start()

    received: list[OHLCVBar] = []
    with pytest.raises(StreamDisconnected):
        async for bar in stream:
            await asyncio.sleep(0.02)  # simulated slow consumer
            received.append(bar)

    assert hub.dropped_bars > 0
    assert len(received) < 10  # backlog was trimmed, stream still terminated cleanly
    await hub.stop()


@pytest.mark.asyncio
async def test_stop_ends_all_subscribers_cleanly() -> None:
    provider = FakeProvider([[kline(0), kline(1)]])
    hub = WebSocketStreamFactory("BTC/USDT", provider, backoff_scale=FAST)
    stream = hub.subscribe("engine")
    await hub.start()

    async def consume() -> int:
        count = 0
        async for _bar in stream:
            count += 1
        return count  # returns (no exception) on a clean stop

    consumer = asyncio.create_task(consume())
    await asyncio.sleep(0.05)
    await hub.stop()
    consumed = await asyncio.wait_for(consumer, timeout=2.0)
    assert consumed >= 0
    assert hub.state is StreamState.DISCONNECTED


@pytest.mark.asyncio
async def test_latency_and_status_reporting() -> None:
    provider = FakeProvider([[kline(0, event_lag_ms=300.0), kline(1, event_lag_ms=150.0)]])
    hub = WebSocketStreamFactory("BTC/USDT", provider, backoff_scale=FAST)
    stream = hub.subscribe("engine")
    await hub.start()
    await drain(stream, 2)

    status = hub.status()
    assert status["symbol"] == "BTC/USDT"
    assert status["bars_received"] == 2
    assert status["last_latency_ms"] is not None and status["last_latency_ms"] >= 100.0
    assert status["subscribers"] == 0  # drain() closed the stream -> deregistered
    await hub.stop()
