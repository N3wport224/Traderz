"""Tests for the data pipeline's reconnection state machine: simulated network
drops, exponential backoff (growth, cap, and reset-only-after-verification),
integrity verification, and the risk-manager/notifier side effects."""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest

from backend.data_pipeline import (
    MockOHLCVGenerator,
    ResilientStream,
    StreamDisconnected,
    StreamState,
    flaky_stream,
    stream_1m_bars,
)
from backend.models import OHLCVBar
from backend.notifier import Notifier
from backend.risk_manager import RiskManager

# Scale real backoff seconds down to sub-millisecond sleeps for tests.
FAST = 0.0001


class CaptureSink:
    def __init__(self) -> None:
        self.messages: list[str] = []

    async def send(self, message: str) -> None:
        self.messages.append(message)


def shared_generator_factory(
    symbol: str = "MOCK",
    bars: int = 50,
    drop_after: list[int] | None = None,
    drop_connections: int = 1,
):
    """Stream factory reusing one generator so the price walk survives reconnects.

    Only the first `drop_connections` connections are flaky — later ones are
    clean, so reconnection (and its verification phase) can actually succeed.
    """
    generator = MockOHLCVGenerator(symbol, seed=7)
    connection_count = 0

    def factory() -> AsyncIterator[OHLCVBar]:
        nonlocal connection_count
        connection_count += 1
        base = stream_1m_bars(symbol, bars=bars, generator=generator)
        if drop_after is not None and connection_count <= drop_connections:
            return flaky_stream(base, drop_after=list(drop_after))
        return base

    return factory


async def collect(stream: ResilientStream, limit: int) -> list[OHLCVBar]:
    got: list[OHLCVBar] = []
    async for bar in stream.bars():
        got.append(bar)
        if len(got) >= limit:
            break
    return got


# --- basic pass-through -------------------------------------------------------


@pytest.mark.asyncio
async def test_stable_stream_passes_bars_through_untouched() -> None:
    stream = ResilientStream(shared_generator_factory(bars=10), "MOCK", backoff_scale=FAST)
    bars = await collect(stream, 10)
    assert len(bars) == 10
    assert stream.disconnect_count == 0
    assert stream.state is StreamState.CONNECTED


# --- drop / reconnect cycle ----------------------------------------------------


@pytest.mark.asyncio
async def test_drop_triggers_reconnect_and_bars_keep_flowing() -> None:
    stream = ResilientStream(
        shared_generator_factory(bars=10, drop_after=[3]), "MOCK", backoff_scale=FAST
    )
    bars = await collect(stream, 12)
    assert len(bars) == 12
    assert stream.disconnect_count >= 1
    assert stream.state is StreamState.CONNECTED  # verified again by the end


@pytest.mark.asyncio
async def test_disconnect_marks_risk_manager_and_verification_clears_it() -> None:
    risk_manager = RiskManager()
    observed: list[tuple[int, bool]] = []

    stream = ResilientStream(
        shared_generator_factory(bars=10, drop_after=[3]),
        "MOCK",
        risk_manager=risk_manager,
        backoff_scale=FAST,
        verify_bars=2,
    )
    count = 0
    async for _bar in stream.bars():
        count += 1
        observed.append((count, risk_manager.is_data_disconnected("MOCK")))
        if count >= 8:
            break

    # Every bar the machine actually delivered arrived while the ticker was
    # trusted — unverified bars are never released downstream.
    assert all(not disconnected for _, disconnected in observed)
    assert stream.disconnect_count == 1
    assert risk_manager.system_status() == "RUNNING"


@pytest.mark.asyncio
async def test_disconnect_and_reconnect_alerts_flow_through_notifier() -> None:
    sink = CaptureSink()
    stream = ResilientStream(
        shared_generator_factory(bars=10, drop_after=[3]),
        "MOCK",
        notifier=Notifier([sink]),
        backoff_scale=FAST,
    )
    await collect(stream, 8)
    assert any("DATA_DISCONNECTED" in message for message in sink.messages)
    assert any("DATA_RECONNECTED" in message for message in sink.messages)
    # warning fires before the recovery notice
    first_disconnect = next(i for i, m in enumerate(sink.messages) if "DATA_DISCONNECTED" in m)
    first_reconnect = next(i for i, m in enumerate(sink.messages) if "DATA_RECONNECTED" in m)
    assert first_disconnect < first_reconnect


# --- exponential backoff --------------------------------------------------------


@pytest.mark.asyncio
async def test_backoff_grows_exponentially_and_caps_at_64s() -> None:
    """A stream that dies during every verification keeps backing off:
    2, 4, 8, 16, 32, 64, 64, ... — the delay only resets after a clean verify."""
    generator = MockOHLCVGenerator("MOCK", seed=1)

    def dying_factory() -> AsyncIterator[OHLCVBar]:
        return flaky_stream(stream_1m_bars("MOCK", bars=100, generator=generator), drop_after=[1])

    stream = ResilientStream(
        dying_factory, "MOCK", backoff_scale=FAST, verify_bars=3, max_retries=7
    )
    with pytest.raises(StreamDisconnected):
        async for _bar in stream.bars():
            pass

    assert stream.delays_used == [2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 64.0]


@pytest.mark.asyncio
async def test_backoff_resets_only_after_verified_reconnection() -> None:
    """Two separate drops with a healthy verified stretch between them must
    each start from the initial 2s delay — but repeated failures within one
    outage must not reset."""
    stream = ResilientStream(
        # the first 3 connections each deliver 3 bars then drop; later ones are clean
        shared_generator_factory(bars=6, drop_after=[3], drop_connections=3),
        "MOCK",
        backoff_scale=FAST,
        verify_bars=1,
    )
    await collect(stream, 12)
    # every disconnect was followed by a successful verification (3 bars > 1
    # verify bar), so every retry used the freshly-reset initial delay
    assert stream.disconnect_count == 3
    assert stream.delays_used == [2.0, 2.0, 2.0]


@pytest.mark.asyncio
async def test_max_retries_exhaustion_reraises() -> None:
    def dead_factory() -> AsyncIterator[OHLCVBar]:
        async def stream() -> AsyncIterator[OHLCVBar]:
            raise StreamDisconnected("永 down")
            yield  # pragma: no cover  # makes this an async generator

        return stream()

    stream = ResilientStream(dead_factory, "MOCK", backoff_scale=FAST, max_retries=2)
    with pytest.raises(StreamDisconnected):
        async for _bar in stream.bars():
            pass
    assert stream.disconnect_count == 3  # initial + 2 retries


# --- verification gating ---------------------------------------------------------


@pytest.mark.asyncio
async def test_bars_are_not_yielded_until_verification_completes() -> None:
    """With verify_bars=3, the three bars delivered right after a reconnect are
    buffered and only released together after the third arrives."""
    stream = ResilientStream(
        shared_generator_factory(bars=20, drop_after=[2]),
        "MOCK",
        backoff_scale=FAST,
        verify_bars=3,
    )
    states_at_delivery: list[StreamState] = []
    bars: list[OHLCVBar] = []
    async for bar in stream.bars():
        states_at_delivery.append(stream.state)
        bars.append(bar)
        if len(bars) >= 10:
            break

    assert len(bars) == 10
    assert stream.disconnect_count == 1
    # every delivered bar arrived with the stream already verified — nothing
    # leaked out mid-VERIFYING
    assert all(state is StreamState.CONNECTED for state in states_at_delivery)


@pytest.mark.asyncio
async def test_flaky_stream_raises_after_configured_bar_count() -> None:
    generator = MockOHLCVGenerator("MOCK", seed=3)
    stream = flaky_stream(stream_1m_bars("MOCK", bars=10, generator=generator), drop_after=[4])
    got = []
    with pytest.raises(StreamDisconnected):
        async for bar in stream:
            got.append(bar)
    assert len(got) == 4
