"""Tests for the broker/exchange execution gateway abstraction: slippage
model correctness (including error bounds), partial fills, open-order
tracking, and live-gateway configuration guards."""

from __future__ import annotations

import random

import pytest

from backend.execution_gateway import (
    BaseExecutionGateway,
    GatewayConfigError,
    GatewayError,
    LiveCCXTExecutionGateway,
    MockExecutionGateway,
)
from backend.models import OrderStatus, SignalAction


def seeded_gateway(**overrides: object) -> MockExecutionGateway:
    defaults: dict[str, object] = {"latency_range_ms": (0.0, 0.5), "rng": random.Random(42)}
    defaults.update(overrides)
    return MockExecutionGateway(**defaults)  # type: ignore[arg-type]


# --- slippage model ----------------------------------------------------------


def test_slippage_pct_scales_with_order_size_up_to_book_depth() -> None:
    gateway = seeded_gateway()
    tiny = gateway.slippage_pct(500.0)
    mid = gateway.slippage_pct(25_000.0)
    at_depth = gateway.slippage_pct(50_000.0)
    beyond = gateway.slippage_pct(500_000.0)

    assert tiny == pytest.approx(gateway.min_slippage_pct, rel=0.05)
    assert tiny < mid < at_depth
    assert at_depth == pytest.approx(gateway.max_slippage_pct)
    assert beyond == pytest.approx(gateway.max_slippage_pct)  # capped at full depth


@pytest.mark.asyncio
async def test_buy_fills_adversely_higher_and_short_lower() -> None:
    gateway = seeded_gateway()
    buy = await gateway.execute_order(SignalAction.BUY, 5_000.0, "MOCK", 100.0)
    short = await gateway.execute_order(SignalAction.SHORT, 5_000.0, "MOCK", 100.0)
    sell = await gateway.execute_order(SignalAction.SELL, 5_000.0, "MOCK", 100.0)

    assert buy.filled_price > 100.0
    assert short.filled_price < 100.0
    assert sell.filled_price < 100.0


@pytest.mark.asyncio
async def test_filled_price_stays_within_configured_slippage_bounds() -> None:
    """The randomized fill must stay inside [min, max] slippage (±20% variance
    included) — a miscalculated slippage would silently corrupt every PnL."""
    gateway = seeded_gateway()
    for _ in range(50):
        fill = await gateway.execute_order(SignalAction.BUY, 5_000.0, "MOCK", 100.0)
        slip_fraction = (fill.filled_price - 100.0) / 100.0
        assert slip_fraction >= gateway.min_slippage_pct * 0.8 * 0.999
        assert slip_fraction <= gateway.max_slippage_pct * 1.2 * 1.001


@pytest.mark.asyncio
async def test_slippage_cost_is_dollar_cost_of_price_degradation() -> None:
    gateway = seeded_gateway()
    fill = await gateway.execute_order(SignalAction.BUY, 10_000.0, "MOCK", 200.0)
    shares = fill.filled_size / 200.0
    expected = abs(fill.filled_price - 200.0) * shares
    assert fill.slippage_cost == pytest.approx(expected, rel=1e-4)
    assert fill.slippage_cost > 0


def test_slippage_cost_zero_for_degenerate_requested_price() -> None:
    assert BaseExecutionGateway.slippage_cost(0.0, 100.0, 1_000.0) == 0.0


@pytest.mark.asyncio
async def test_zero_slippage_configuration_fills_at_requested_price() -> None:
    gateway = MockExecutionGateway(min_slippage_pct=0.0, max_slippage_pct=0.0, latency_range_ms=(0.0, 0.0))
    fill = await gateway.execute_order(SignalAction.BUY, 5_000.0, "MOCK", 123.45)
    assert fill.filled_price == pytest.approx(123.45)
    assert fill.slippage_cost == 0.0


# --- fees, latency, partial fills -------------------------------------------


@pytest.mark.asyncio
async def test_fees_charged_on_filled_notional() -> None:
    gateway = seeded_gateway(fee_rate=0.001)
    fill = await gateway.execute_order(SignalAction.BUY, 5_000.0, "MOCK", 100.0)
    assert fill.fees == pytest.approx(5_000.0 * 0.001, rel=1e-6)


@pytest.mark.asyncio
async def test_fill_reports_positive_latency() -> None:
    gateway = MockExecutionGateway(latency_range_ms=(1.0, 2.0), rng=random.Random(1))
    fill = await gateway.execute_order(SignalAction.BUY, 5_000.0, "MOCK", 100.0)
    assert fill.latency_ms >= 1.0


@pytest.mark.asyncio
async def test_order_larger_than_book_depth_partially_fills() -> None:
    gateway = seeded_gateway(book_depth_notional=50_000.0, partial_fill_ratio=0.9)
    fill = await gateway.execute_order(SignalAction.BUY, 100_000.0, "MOCK", 100.0)
    assert fill.status is OrderStatus.PARTIALLY_FILLED
    assert fill.filled_size == pytest.approx(90_000.0)
    assert fill.requested_size == 100_000.0


@pytest.mark.asyncio
async def test_order_within_book_depth_fully_fills() -> None:
    gateway = seeded_gateway()
    fill = await gateway.execute_order(SignalAction.BUY, 5_000.0, "MOCK", 100.0)
    assert fill.status is OrderStatus.FILLED
    assert fill.filled_size == 5_000.0


# --- validation and error paths ----------------------------------------------


@pytest.mark.asyncio
async def test_rejects_nonpositive_size_and_price() -> None:
    gateway = seeded_gateway()
    with pytest.raises(GatewayError):
        await gateway.execute_order(SignalAction.BUY, 0.0, "MOCK", 100.0)
    with pytest.raises(GatewayError):
        await gateway.execute_order(SignalAction.BUY, 5_000.0, "MOCK", -1.0)


@pytest.mark.asyncio
async def test_rejects_unroutable_signal_type() -> None:
    gateway = seeded_gateway()
    with pytest.raises(GatewayError):
        await gateway.execute_order(SignalAction.ALERT, 5_000.0, "MOCK", 100.0)


def test_invalid_slippage_configuration_raises() -> None:
    with pytest.raises(GatewayConfigError):
        MockExecutionGateway(min_slippage_pct=0.01, max_slippage_pct=0.001)
    with pytest.raises(GatewayConfigError):
        MockExecutionGateway(book_depth_notional=0.0)
    with pytest.raises(GatewayConfigError):
        MockExecutionGateway(partial_fill_ratio=0.0)


# --- open-order tracking (reconciliation surface) ----------------------------


@pytest.mark.asyncio
async def test_entries_tracked_as_open_orders_until_closed() -> None:
    gateway = seeded_gateway()
    entry = await gateway.execute_order(SignalAction.BUY, 5_000.0, "AAA", 100.0)
    await gateway.execute_order(SignalAction.SHORT, 5_000.0, "BBB", 50.0)

    assert {o.order_id for o in await gateway.fetch_open_orders()} == {"mock-1", "mock-2"}
    assert [o.order_id for o in await gateway.fetch_open_orders("AAA")] == [entry.order_id]

    await gateway.execute_order(SignalAction.SELL, 5_000.0, "AAA", 101.0)  # closes AAA
    remaining = await gateway.fetch_open_orders()
    assert [o.ticker for o in remaining] == ["BBB"]


# --- live gateway configuration guards ---------------------------------------


def test_live_gateway_requires_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("API_KEY", raising=False)
    monkeypatch.delenv("API_SECRET", raising=False)
    with pytest.raises(GatewayConfigError, match="API_KEY"):
        LiveCCXTExecutionGateway()


def test_live_gateway_requires_ccxt_or_valid_exchange(monkeypatch: pytest.MonkeyPatch) -> None:
    """With credentials present, construction proceeds to the ccxt import /
    exchange-id resolution and must fail with a config error, never silently."""
    monkeypatch.setenv("API_KEY", "k")
    monkeypatch.setenv("API_SECRET", "s")
    with pytest.raises(GatewayConfigError):
        LiveCCXTExecutionGateway(exchange_id="definitely_not_a_real_exchange")


def test_mock_gateway_name_is_mock() -> None:
    assert seeded_gateway().name == "MOCK"
