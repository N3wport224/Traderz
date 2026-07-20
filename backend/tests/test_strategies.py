"""Verification tests for the Momentum (ORB) and Swing (trendline) strategy engines,
including their Phase 2 wiring: live config, capital allocation, persistence, and the
shared risk manager's circuit breaker."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from backend.config import ConfigStore, MomentumConfig, SwingConfig
from backend.models import OHLCVBar, SignalAction, Timeframe, TradeRecord
from backend.risk_manager import RiskManager
from backend.strategies.momentum_engine import MomentumEngine
from backend.strategies.swing_engine import SwingEngine

BASE_TIME = datetime(2026, 7, 20, 9, 30, tzinfo=timezone.utc)


class FakePersistence:
    """Records every call made through the `TradePersistence` protocol, for assertions."""

    def __init__(self) -> None:
        self.trades: list[TradeRecord] = []
        self.equity_snapshots: list[tuple[str, datetime, float]] = []

    async def record_trade(self, trade: TradeRecord) -> None:
        self.trades.append(trade)

    async def record_equity_snapshot(self, engine_type: str, timestamp: datetime, equity: float) -> None:
        self.equity_snapshots.append((engine_type, timestamp, equity))


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


# --- Momentum Engine (ORB): core signal behavior ----------------------------


@pytest.mark.asyncio
async def test_opening_range_established_with_no_signal() -> None:
    engine = MomentumEngine("MOCK")
    for bar in OPENING_RANGE_BARS:
        assert await engine.on_bar(bar) == []

    assert engine.opening_range_high == 105
    assert engine.opening_range_low == 95


@pytest.mark.asyncio
async def test_breakout_above_high_triggers_buy() -> None:
    engine = MomentumEngine("MOCK")
    for bar in OPENING_RANGE_BARS:
        await engine.on_bar(bar)

    signals = await engine.on_bar(make_1m_bar(5, 102, 107, 102, 106))
    assert len(signals) == 1
    signal = signals[0]
    assert signal.action is SignalAction.BUY
    assert signal.price == 106
    assert signal.metadata["opening_range_high"] == 105


@pytest.mark.asyncio
async def test_breakdown_below_low_triggers_short() -> None:
    engine = MomentumEngine("MOCK")
    for bar in OPENING_RANGE_BARS:
        await engine.on_bar(bar)

    signals = await engine.on_bar(make_1m_bar(5, 96, 96, 93, 94))
    assert len(signals) == 1
    assert signals[0].action is SignalAction.SHORT
    assert signals[0].price == 94


@pytest.mark.asyncio
async def test_time_stop_exits_after_20_minutes_without_target_profit() -> None:
    engine = MomentumEngine("MOCK")
    for bar in OPENING_RANGE_BARS:
        await engine.on_bar(bar)

    entry_signals = await engine.on_bar(make_1m_bar(5, 102, 107, 102, 106))
    entry_signal = entry_signals[0]

    exit_signal = None
    for minute in range(6, 26):
        signals = await engine.on_bar(make_1m_bar(minute, 106.1, 106.3, 105.9, 106.1))
        if signals:
            exit_signal = signals[0]
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

    entry_signal = (await engine.on_bar(make_1m_bar(5, 102, 107, 102, 106)))[0]
    exit_signals = await engine.on_bar(make_1m_bar(6, 106.5, 106.8, 106.4, 106.6))  # +0.57% >= 0.5% target

    assert exit_signals[0].reason == "target_profit_reached"
    assert exit_signals[0].timestamp - entry_signal.timestamp == timedelta(minutes=1)


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


# --- Momentum Engine: live configuration -----------------------------------


@pytest.mark.asyncio
async def test_opening_range_bars_reads_live_from_config() -> None:
    store = ConfigStore()
    engine = MomentumEngine("MOCK", config_store=store)
    assert engine.opening_range_bars == 5
    await store.update_momentum(opening_range_minutes=8)
    assert engine.opening_range_bars == 8


@pytest.mark.asyncio
async def test_time_stop_reads_live_from_config() -> None:
    store = ConfigStore()
    engine = MomentumEngine("MOCK", config_store=store)
    assert engine.time_stop == timedelta(minutes=20)
    await store.update_momentum(time_stop_minutes=10)
    assert engine.time_stop == timedelta(minutes=10)


@pytest.mark.asyncio
async def test_shortening_opening_range_mid_collection_finalizes_earlier() -> None:
    """A config change made between bars must take effect on the very next bar."""
    store = ConfigStore(momentum=MomentumConfig(opening_range_minutes=5, time_stop_minutes=20))
    engine = MomentumEngine("MOCK", config_store=store)

    await engine.on_bar(OPENING_RANGE_BARS[0])
    await engine.on_bar(OPENING_RANGE_BARS[1])
    assert engine.opening_range_high is None  # only 2 of 5 bars collected

    await store.update_momentum(opening_range_minutes=3)
    await engine.on_bar(OPENING_RANGE_BARS[2])  # now the 3rd bar — meets the new lower threshold

    assert engine.opening_range_high == max(b.high for b in OPENING_RANGE_BARS[:3])
    assert engine.opening_range_low == min(b.low for b in OPENING_RANGE_BARS[:3])


@pytest.mark.asyncio
async def test_shortening_time_stop_mid_trade_exits_sooner() -> None:
    store = ConfigStore()
    engine = MomentumEngine("MOCK", config_store=store)
    for bar in OPENING_RANGE_BARS:
        await engine.on_bar(bar)

    entry_signal = (await engine.on_bar(make_1m_bar(5, 102, 107, 102, 106)))[0]
    await store.update_momentum(time_stop_minutes=2)  # was 20

    exit_signal = None
    for minute in range(6, 10):
        signals = await engine.on_bar(make_1m_bar(minute, 106.1, 106.3, 105.9, 106.1))
        if signals:
            exit_signal = signals[0]
            break

    assert exit_signal is not None
    assert exit_signal.reason == "time_stop_20min"
    assert exit_signal.timestamp - entry_signal.timestamp == timedelta(minutes=2)


# --- Momentum Engine: capital allocation, fees, persistence ----------------


@pytest.mark.asyncio
async def test_position_size_respects_risk_manager_allocation() -> None:
    risk_manager = RiskManager(total_capital=200_000.0, allocation_pct={"momentum": 0.05, "swing": 0.15})
    engine = MomentumEngine("MOCK", risk_manager=risk_manager)
    for bar in OPENING_RANGE_BARS:
        await engine.on_bar(bar)

    signal = (await engine.on_bar(make_1m_bar(5, 102, 107, 102, 106)))[0]
    assert signal.metadata["position_size"] == 200_000.0 * 0.05


@pytest.mark.asyncio
async def test_net_pnl_reflects_notional_position_size_and_fees() -> None:
    risk_manager = RiskManager(total_capital=100_000.0, allocation_pct={"momentum": 0.05, "swing": 0.15}, fee_rate=0.001)
    engine = MomentumEngine("MOCK", risk_manager=risk_manager)
    for bar in OPENING_RANGE_BARS:
        await engine.on_bar(bar)

    await engine.on_bar(make_1m_bar(5, 102, 107, 102, 106))  # entry @ 106
    exit_signals = await engine.on_bar(make_1m_bar(6, 106.5, 106.8, 106.4, 106.6))  # exit @ 106.6

    notional = 100_000.0 * 0.05
    pct_move = (106.6 - 106.0) / 106.0
    expected_fees = notional * 0.001
    expected_net_pnl = pct_move * notional - expected_fees

    assert exit_signals[0].metadata["pnl"] == pytest.approx(round(expected_net_pnl, 4))
    assert exit_signals[0].metadata["fees"] == pytest.approx(round(expected_fees, 4))
    assert engine.equity == pytest.approx(expected_net_pnl)


@pytest.mark.asyncio
async def test_starting_equity_is_used_as_the_base() -> None:
    engine = MomentumEngine("MOCK", starting_equity=500.0)
    for bar in OPENING_RANGE_BARS:
        await engine.on_bar(bar)
    await engine.on_bar(make_1m_bar(5, 102, 107, 102, 106))
    await engine.on_bar(make_1m_bar(6, 106.5, 106.8, 106.4, 106.6))

    assert engine.equity > 500.0  # base + realized pnl, not reset to 0


@pytest.mark.asyncio
async def test_closed_trade_and_equity_snapshot_are_persisted() -> None:
    persistence = FakePersistence()
    engine = MomentumEngine("MOCK", persistence=persistence)
    for bar in OPENING_RANGE_BARS:
        await engine.on_bar(bar)
    await engine.on_bar(make_1m_bar(5, 102, 107, 102, 106))
    await engine.on_bar(make_1m_bar(6, 106.5, 106.8, 106.4, 106.6))

    assert len(persistence.trades) == 1
    trade = persistence.trades[0]
    assert trade.engine_type == "momentum"
    assert trade.asset_ticker == "MOCK"
    assert trade.entry_price == 106
    assert trade.exit_price == 106.6
    assert trade.net_profit == pytest.approx(engine.equity)

    assert len(persistence.equity_snapshots) == 1
    engine_type, timestamp, equity = persistence.equity_snapshots[0]
    assert engine_type == "momentum"
    assert equity == pytest.approx(engine.equity)


# --- Momentum Engine: risk manager gating -----------------------------------


@pytest.mark.asyncio
async def test_halted_risk_manager_blocks_new_entries() -> None:
    risk_manager = RiskManager()
    risk_manager.pause()
    engine = MomentumEngine("MOCK", risk_manager=risk_manager)

    for bar in OPENING_RANGE_BARS:
        assert await engine.on_bar(bar) == []
    # Would normally break out and BUY, but entries are blocked.
    assert await engine.on_bar(make_1m_bar(5, 102, 107, 102, 106)) == []
    assert engine.opening_range_high is None  # gate short-circuits before range bookkeeping too


@pytest.mark.asyncio
async def test_halt_mid_trade_force_closes_the_open_position() -> None:
    risk_manager = RiskManager()
    engine = MomentumEngine("MOCK", risk_manager=risk_manager)
    for bar in OPENING_RANGE_BARS:
        await engine.on_bar(bar)
    await engine.on_bar(make_1m_bar(5, 102, 107, 102, 106))  # opens a BUY position
    assert engine.opening_range_high is not None

    risk_manager.pause()  # simulate an operator pause / breaker trip mid-trade
    signals = await engine.on_bar(make_1m_bar(6, 106.1, 106.3, 106.0, 106.1))

    assert len(signals) == 1
    assert signals[0].action is SignalAction.EXIT
    assert signals[0].reason == "circuit_breaker_active"


@pytest.mark.asyncio
async def test_circuit_breaker_triggered_signal_emitted_on_trip() -> None:
    """A loss that crosses the daily drawdown threshold must, on the very same
    bar, emit both the EXIT for that trade and a CIRCUIT_BREAKER_TRIGGERED alert.

    This engine only closes on target-profit or the time-stop (no stop-loss), so
    the loss is realized via a 20-minute time-stop exit on an adverse move.
    """
    risk_manager = RiskManager(
        total_capital=10_000.0, max_daily_drawdown_pct=0.01, allocation_pct={"momentum": 1.0, "swing": 1.0}
    )
    engine = MomentumEngine("MOCK", risk_manager=risk_manager)
    for bar in OPENING_RANGE_BARS:
        await engine.on_bar(bar)
    await engine.on_bar(make_1m_bar(5, 102, 107, 102, 106))  # BUY @ 106, notional=10,000

    signals: list = []
    for minute in range(6, 26):
        signals = await engine.on_bar(make_1m_bar(minute, 90.0, 90.5, 89.5, 90.0))  # adverse move, held to time-stop
        if signals:
            break

    assert [s.action for s in signals] == [SignalAction.EXIT, SignalAction.CIRCUIT_BREAKER]
    assert signals[0].reason == "time_stop_20min"
    assert signals[1].reason == "max_daily_drawdown_exceeded"
    assert risk_manager.is_halted(BASE_TIME) is True


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
    small_bullish = make_4h_bar(1, 14.1, 14.4, 14.0, 14.3)
    assert SwingEngine.is_bullish_engulfing(bearish, small_bullish) is False


def _neutral_bar(index: int, low: float) -> OHLCVBar:
    """A mildly-bearish filler bar whose 'low' is the only precisely-controlled value."""
    return make_4h_bar(index, low + 2, low + 3, low, low + 1)


# Troughs at index 2 (10), 7 (12), 12 (14) lie exactly on: value = 0.4 * index + 9.2
COLLINEAR_LOWS = {0: 20, 1: 15, 2: 10, 3: 15, 4: 20, 5: 20, 6: 15, 7: 12, 8: 15, 9: 20, 10: 20, 11: 15, 12: 14}
BEARISH_BAR_13 = make_4h_bar(13, 14.6, 14.7, 14.1, 14.2)
ENGULFING_BAR_14 = make_4h_bar(14, 14.15, 14.9, 14.05, 14.8)  # closes exactly on the line: 0.4*14+9.2=14.8


def test_find_pivots_detects_local_trough() -> None:
    engine = SwingEngine("MOCK", pivot_window=1)
    for i, low in enumerate([20, 5, 20]):
        engine._bars.append(_neutral_bar(i, low))

    pivots = engine.find_pivots()
    troughs = [p for p in pivots if p.kind == "trough"]
    assert len(troughs) == 1
    assert troughs[0].index == 1
    assert troughs[0].price == 5


@pytest.mark.asyncio
async def test_trendline_retest_with_bullish_engulfing_triggers_alert() -> None:
    engine = SwingEngine("MOCK", pivot_window=2)

    for i in range(13):
        assert await engine.on_bar(_neutral_bar(i, COLLINEAR_LOWS[i])) == []

    assert await engine.on_bar(BEARISH_BAR_13) == []
    signals = await engine.on_bar(ENGULFING_BAR_14)

    assert len(signals) == 1
    alert = signals[0]
    assert alert.action is SignalAction.ALERT
    assert alert.reason == "trendline_retest_bullish_engulfing"
    assert alert.metadata["trendline_touches"] == 3
    assert alert.price == pytest.approx(14.8)


@pytest.mark.asyncio
async def test_no_alert_when_touches_below_minimum() -> None:
    engine = SwingEngine("MOCK", pivot_window=2)
    lows = {0: 20, 1: 15, 2: 10, 3: 15, 4: 20, 5: 20, 6: 15, 7: 12, 8: 15, 9: 20, 10: 20}

    for i in range(11):
        assert await engine.on_bar(_neutral_bar(i, lows[i])) == []

    assert engine.find_support_trendline() is None


@pytest.mark.asyncio
async def test_swing_engine_run_consumes_async_stream() -> None:
    engine = SwingEngine("MOCK", pivot_window=2)

    async def bar_stream():
        for i in range(13):
            yield _neutral_bar(i, COLLINEAR_LOWS[i])
        yield BEARISH_BAR_13
        yield ENGULFING_BAR_14

    signals = [signal async for signal in engine.run(bar_stream())]
    assert len(signals) == 1
    assert signals[0].action is SignalAction.ALERT


# --- Swing Engine: live configuration ---------------------------------------


@pytest.mark.asyncio
async def test_min_touches_reads_live_from_config() -> None:
    store = ConfigStore()
    engine = SwingEngine("MOCK", config_store=store)
    assert engine.min_touches == 3
    await store.update_swing(min_touches=5)
    assert engine.min_touches == 5


@pytest.mark.asyncio
async def test_touch_tolerance_reads_live_from_config() -> None:
    store = ConfigStore()
    engine = SwingEngine("MOCK", config_store=store)
    assert engine.touch_tolerance_pct == 0.005
    await store.update_swing(touch_tolerance_pct=0.02)
    assert engine.touch_tolerance_pct == 0.02


@pytest.mark.asyncio
async def test_raising_min_touches_mid_stream_suppresses_alert_until_lowered_again() -> None:
    """Requiring 4 touches (only 3 exist) suppresses the alert; lowering the
    requirement back to 3 between bars unlocks it on the very next bar."""
    store = ConfigStore(swing=SwingConfig(min_touches=4, touch_tolerance_pct=0.005))
    engine = SwingEngine("MOCK", pivot_window=2, config_store=store)

    for i in range(13):
        await engine.on_bar(_neutral_bar(i, COLLINEAR_LOWS[i]))
    await engine.on_bar(BEARISH_BAR_13)
    assert await engine.on_bar(ENGULFING_BAR_14) == []  # only 3 touches exist; 4 required

    # A fresh scenario re-run with the requirement lowered before the retest bar.
    store2 = ConfigStore(swing=SwingConfig(min_touches=4, touch_tolerance_pct=0.005))
    engine2 = SwingEngine("MOCK", pivot_window=2, config_store=store2)
    for i in range(13):
        await engine2.on_bar(_neutral_bar(i, COLLINEAR_LOWS[i]))
    await engine2.on_bar(BEARISH_BAR_13)
    await store2.update_swing(min_touches=3)
    signals = await engine2.on_bar(ENGULFING_BAR_14)
    assert len(signals) == 1
    assert signals[0].action is SignalAction.ALERT


# --- Swing Engine: capital allocation, persistence, risk gating ------------


@pytest.mark.asyncio
async def test_swing_position_size_respects_risk_manager_allocation() -> None:
    risk_manager = RiskManager(total_capital=200_000.0, allocation_pct={"momentum": 0.05, "swing": 0.15})
    engine = SwingEngine("MOCK", pivot_window=2, risk_manager=risk_manager)

    for i in range(13):
        await engine.on_bar(_neutral_bar(i, COLLINEAR_LOWS[i]))
    await engine.on_bar(BEARISH_BAR_13)
    signals = await engine.on_bar(ENGULFING_BAR_14)

    assert signals[0].metadata["position_size"] == 200_000.0 * 0.15


@pytest.mark.asyncio
async def test_swing_closed_trade_is_persisted_after_hold_period() -> None:
    persistence = FakePersistence()
    engine = SwingEngine("MOCK", pivot_window=2, hold_period_bars=3, persistence=persistence)

    for i in range(13):
        await engine.on_bar(_neutral_bar(i, COLLINEAR_LOWS[i]))
    await engine.on_bar(BEARISH_BAR_13)
    await engine.on_bar(ENGULFING_BAR_14)  # opens hypothetical position @ 14.8

    await engine.on_bar(make_4h_bar(15, 15.0, 15.2, 14.8, 15.0))
    await engine.on_bar(make_4h_bar(16, 15.0, 15.3, 14.9, 15.1))
    await engine.on_bar(make_4h_bar(17, 15.1, 15.6, 15.0, 15.5))  # hold period elapses -> closes

    assert len(persistence.trades) == 1
    assert persistence.trades[0].engine_type == "swing"
    assert persistence.trades[0].entry_price == pytest.approx(14.8)
    assert persistence.trades[0].exit_price == pytest.approx(15.5)
    assert len(persistence.equity_snapshots) == 1


@pytest.mark.asyncio
async def test_swing_halted_risk_manager_blocks_new_alerts() -> None:
    risk_manager = RiskManager()
    risk_manager.pause()
    engine = SwingEngine("MOCK", pivot_window=2, risk_manager=risk_manager)

    for i in range(13):
        assert await engine.on_bar(_neutral_bar(i, COLLINEAR_LOWS[i])) == []
    await engine.on_bar(BEARISH_BAR_13)
    assert await engine.on_bar(ENGULFING_BAR_14) == []
    assert engine._position is None
