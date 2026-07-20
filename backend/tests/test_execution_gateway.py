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


# --- Phase 5: bracket order monitor ------------------------------------------


from datetime import datetime, timezone

from backend.models import BracketOrder, BracketStatus, OHLCVBar, Timeframe


def make_bracket(
    order_id: str = "o1",
    side: str = "long",
    entry: float = 106.0,
    sl: float = 97.75,
    tp: float = 119.75,
    size: float = 5_000.0,
) -> BracketOrder:
    return BracketOrder(
        order_id=order_id,
        engine_type="momentum",
        ticker="MOCK",
        side=side,
        entry_price=entry,
        stop_loss_price=sl,
        take_profit_price=tp,
        size=size,
        created_at=datetime(2026, 7, 20, 9, 30, tzinfo=timezone.utc),
    )


def candle(open_: float, high: float, low: float, close: float, symbol: str = "MOCK") -> OHLCVBar:
    return OHLCVBar(
        symbol=symbol,
        timestamp=datetime(2026, 7, 20, 9, 40, tzinfo=timezone.utc),
        timeframe=Timeframe.ONE_MINUTE,
        open=open_,
        high=high,
        low=low,
        close=close,
        volume=1_000,
    )


def zero_slip() -> MockExecutionGateway:
    return MockExecutionGateway(min_slippage_pct=0.0, max_slippage_pct=0.0, latency_range_ms=(0.0, 0.0))


@pytest.mark.asyncio
async def test_bracket_untouched_candle_returns_none() -> None:
    gateway = zero_slip()
    await gateway.register_bracket(make_bracket())
    assert await gateway.check_bracket("o1", candle(106, 110, 100, 108)) is None
    assert len(gateway.active_brackets()) == 1


@pytest.mark.asyncio
async def test_bracket_low_touching_stop_executes_hit_sl_exit() -> None:
    gateway = zero_slip()
    await gateway.register_bracket(make_bracket())
    exit_event = await gateway.check_bracket("o1", candle(99, 100, 97.75, 98))  # low == SL exactly

    assert exit_event is not None
    assert exit_event.status is BracketStatus.HIT_SL
    assert exit_event.triggered_price == 97.75
    assert exit_event.fill.signal_type is SignalAction.SELL  # long exit is a market sell
    assert exit_event.fill.filled_price == pytest.approx(97.75)
    assert gateway.active_brackets() == []  # consumed


@pytest.mark.asyncio
async def test_bracket_high_touching_target_executes_hit_tp_exit() -> None:
    gateway = zero_slip()
    await gateway.register_bracket(make_bracket())
    exit_event = await gateway.check_bracket("o1", candle(118, 119.75, 117, 119))  # high == TP exactly

    assert exit_event is not None
    assert exit_event.status is BracketStatus.HIT_TP
    assert exit_event.fill.filled_price == pytest.approx(119.75)


@pytest.mark.asyncio
async def test_bracket_sl_wins_when_both_levels_inside_one_candle() -> None:
    """Intrabar ordering is unknowable from OHLC — resolve pessimistically."""
    gateway = zero_slip()
    await gateway.register_bracket(make_bracket())
    exit_event = await gateway.check_bracket("o1", candle(106, 125, 95, 110))
    assert exit_event is not None and exit_event.status is BracketStatus.HIT_SL


@pytest.mark.asyncio
async def test_bracket_gap_through_stop_fills_at_open() -> None:
    """A candle opening below the stop can't fill at the stop — you get the open."""
    gateway = zero_slip()
    await gateway.register_bracket(make_bracket())
    exit_event = await gateway.check_bracket("o1", candle(92, 93, 90, 91))
    assert exit_event is not None
    assert exit_event.fill.filled_price == pytest.approx(92.0)


@pytest.mark.asyncio
async def test_short_bracket_mirrors_levels_and_exits_with_market_buy() -> None:
    gateway = zero_slip()
    await gateway.register_bracket(make_bracket(side="short", entry=94.0, sl=102.25, tp=80.25))
    exit_event = await gateway.check_bracket("o1", candle(101, 103, 100, 102))  # high >= short SL

    assert exit_event is not None
    assert exit_event.status is BracketStatus.HIT_SL
    assert exit_event.fill.signal_type is SignalAction.BUY  # short exit is a market buy


@pytest.mark.asyncio
async def test_adjust_bracket_stop_never_widens_risk() -> None:
    gateway = zero_slip()
    await gateway.register_bracket(make_bracket(order_id="o6", entry=14.8, sl=13.86, tp=23.0, size=15_000.0))

    assert await gateway.adjust_bracket_stop("o6", 14.8) is True  # trail up to break-even
    assert await gateway.adjust_bracket_stop("o6", 13.0) is False  # widening refused
    assert await gateway.adjust_bracket_stop("o6", 25.0) is False  # beyond TP refused
    assert await gateway.adjust_bracket_stop("missing", 14.0) is False
    assert gateway.active_brackets()[0].stop_loss_price == 14.8


@pytest.mark.asyncio
async def test_cancel_bracket_removes_without_executing() -> None:
    gateway = zero_slip()
    await gateway.register_bracket(make_bracket())
    fills_before = len(gateway.fills)
    cancelled = await gateway.cancel_bracket("o1")
    assert cancelled is not None and cancelled.order_id == "o1"
    assert await gateway.cancel_bracket("o1") is None
    assert gateway.active_brackets() == []
    assert len(gateway.fills) == fills_before  # no exit order was placed


@pytest.mark.asyncio
async def test_register_bracket_validates_level_ordering() -> None:
    gateway = zero_slip()
    with pytest.raises(GatewayError):
        await gateway.register_bracket(make_bracket(sl=110.0))  # long SL above entry
    with pytest.raises(GatewayError):
        await gateway.register_bracket(make_bracket(side="short", entry=94.0, sl=80.0, tp=102.0))


@pytest.mark.asyncio
async def test_observe_bar_tracks_last_price_per_ticker() -> None:
    gateway = zero_slip()
    gateway.observe_bar(candle(50, 51, 49, 50.5, symbol="AAPL"))
    gateway.observe_bar(candle(106, 107, 105, 106.4))
    assert gateway.last_price("AAPL") == 50.5
    assert gateway.last_price("MOCK") == 106.4
    assert gateway.last_price("UNSEEN") is None
