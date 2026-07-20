"""Tests for the historical backtesting engine: transport parsing, metric
math, a clean uptrend (high profit factor) and a catastrophic whipsaw that
must trip the RiskGuard's MAX_DAILY_LOSS_PCT and block all further entries."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from backend.engine.backtester import (
    BacktestError,
    BacktestResult,
    HistoricalTransport,
    run_backtest,
)
from backend.models import Timeframe, TradeRecord
from backend.utils.risk_guard import RiskGuard

BASE = datetime(2026, 7, 20, 9, 30, tzinfo=timezone.utc)


def candle(i: int, open_: float, high: float, low: float, close: float) -> dict[str, Any]:
    return {
        "timestamp": (BASE + timedelta(minutes=i)).isoformat(),
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": 1_000,
    }


OPENING_RANGE = [
    candle(0, 100, 102, 98, 101),
    candle(1, 101, 103, 99, 100),
    candle(2, 100, 105, 95, 102),
    candle(3, 102, 104, 100, 101),
    candle(4, 101, 103, 99, 100),
]


def uptrend_payload(bars: int = 120) -> list[dict[str, Any]]:
    """Stair-stepping rally: repeated breakouts whose wide highs keep tagging
    the 2.5x-ATR take profit — a strategy paradise."""
    rows = list(OPENING_RANGE)
    price = 106.0
    for i in range(5, bars):
        rows.append(candle(i, price, price + 16, price - 1, price + 2))
        price += 2
    return rows


def whipsaw_payload(cycles: int = 10) -> list[dict[str, Any]]:
    """The catastrophe for a breakout strategy: every long breakout immediately
    dumps through its ATR stop, then price recovers into the range and does it
    again. Every trade is a stop-out."""
    rows = list(OPENING_RANGE)
    i = 5
    for _ in range(cycles):
        rows.append(candle(i, 100, 107, 99, 106))  # close above the 105 range high -> BUY
        i += 1
        rows.append(candle(i, 100, 101, 80, 100))  # low 80 pierces any 1.5x-ATR stop -> HIT_SL
        i += 1
    return rows


def make_trade(net_profit: float, minute: int = 0) -> TradeRecord:
    return TradeRecord(
        engine_type="momentum",
        asset_ticker="TEST",
        entry_timestamp=BASE + timedelta(minutes=minute),
        exit_timestamp=BASE + timedelta(minutes=minute + 5),
        entry_price=100.0,
        exit_price=100.0 + net_profit / 50.0,
        position_size=5_000.0,
        fees=5.0,
        net_profit=net_profit,
    )


# --- HistoricalTransport ------------------------------------------------------


def test_transport_from_json_payload_sorts_and_parses() -> None:
    shuffled = [candle(2, 100, 101, 99, 100), candle(0, 100, 101, 99, 100), candle(1, 100, 101, 99, 100)]
    transport = HistoricalTransport.from_json(shuffled, "TEST", Timeframe.ONE_MINUTE)
    timestamps = [bar.timestamp for bar in transport.bars]
    assert timestamps == sorted(timestamps)
    assert transport.bars[0].symbol == "TEST"
    assert transport.bars[0].timeframe is Timeframe.ONE_MINUTE


def test_transport_from_csv_file(tmp_path: Path) -> None:
    csv_path = tmp_path / "TEST.csv"
    csv_path.write_text(
        "timestamp,open,high,low,close,volume\n"
        f"{BASE.isoformat()},100,101,99,100.5,1200\n"
        f"{(BASE + timedelta(minutes=1)).isoformat()},100.5,102,100,101,900\n"
    )
    transport = HistoricalTransport.from_csv(csv_path, "TEST", Timeframe.ONE_MINUTE)
    assert len(transport.bars) == 2
    assert transport.bars[0].close == 100.5
    assert transport.bars[0].volume == 1_200


def test_transport_csv_missing_file_and_bad_columns(tmp_path: Path) -> None:
    with pytest.raises(BacktestError, match="not found"):
        HistoricalTransport.from_csv(tmp_path / "missing.csv", "TEST", Timeframe.ONE_MINUTE)
    bad = tmp_path / "bad.csv"
    bad.write_text("time,price\n1,2\n")
    with pytest.raises(BacktestError, match="columns"):
        HistoricalTransport.from_csv(bad, "TEST", Timeframe.ONE_MINUTE)


def test_transport_epoch_timestamps_and_window() -> None:
    payload = [
        {"timestamp": int(BASE.timestamp()) + i * 60, "open": 100, "high": 101, "low": 99, "close": 100, "volume": 1}
        for i in range(10)
    ]
    transport = HistoricalTransport.from_json(payload, "TEST", Timeframe.ONE_MINUTE)
    windowed = transport.window(BASE + timedelta(minutes=2), BASE + timedelta(minutes=5))
    assert len(windowed) == 4  # inclusive bounds: minutes 2,3,4,5
    assert windowed[0].timestamp == BASE + timedelta(minutes=2)


@pytest.mark.asyncio
async def test_transport_stream_is_strictly_sequential() -> None:
    transport = HistoricalTransport.from_json(uptrend_payload(10), "TEST", Timeframe.ONE_MINUTE)
    seen = [bar.timestamp async for bar in transport.stream()]
    assert seen == sorted(seen)
    assert len(seen) == 10


def test_transport_synthetic_is_deterministic() -> None:
    a = HistoricalTransport.synthetic("AAPL", Timeframe.ONE_MINUTE, BASE, BASE + timedelta(hours=1), seed=5)
    b = HistoricalTransport.synthetic("AAPL", Timeframe.ONE_MINUTE, BASE, BASE + timedelta(hours=1), seed=5)
    assert len(a.bars) == 61
    assert [bar.close for bar in a.bars] == [bar.close for bar in b.bars]


# --- BacktestResult metric math ----------------------------------------------


def make_result(trades: list[TradeRecord], curve: list[tuple[datetime, float]]) -> BacktestResult:
    return BacktestResult(
        strategy="momentum",
        symbol="TEST",
        start_time=BASE,
        end_time=BASE + timedelta(hours=1),
        initial_capital=100_000.0,
        bars_replayed=60,
        trades=trades,
        equity_curve=curve,
    )


def test_metrics_hand_computed() -> None:
    trades = [make_trade(300.0, 0), make_trade(-100.0, 10), make_trade(200.0, 20), make_trade(-150.0, 30)]
    curve = [
        (BASE + timedelta(minutes=5), 300.0),
        (BASE + timedelta(minutes=15), 200.0),
        (BASE + timedelta(minutes=25), 400.0),
        (BASE + timedelta(minutes=35), 250.0),
    ]
    result = make_result(trades, curve)

    assert result.net_pnl == pytest.approx(250.0)
    assert result.total_return_pct == pytest.approx(0.25)
    assert result.win_rate_pct == pytest.approx(50.0)
    assert result.profit_factor == pytest.approx(500.0 / 250.0)  # gross gains / gross losses
    # equity path: 100300 -> 100200 -> 100400 -> 100250; peak 100400, trough after peak 100250
    assert result.max_drawdown_pct == pytest.approx((100_400 - 100_250) / 100_400 * 100, abs=1e-6)


def test_metrics_empty_run() -> None:
    result = make_result([], [])
    assert result.net_pnl == 0.0
    assert result.win_rate_pct is None
    assert result.profit_factor is None
    assert result.max_drawdown_pct == 0.0
    payload = result.as_dict()
    assert payload["trade_count"] == 0 and payload["profit_factor"] is None


def test_metrics_all_wins_reports_inf_profit_factor() -> None:
    result = make_result([make_trade(100.0), make_trade(50.0, 10)], [])
    assert result.profit_factor == float("inf")
    assert result.as_dict()["profit_factor"] == "inf"  # JSON-safe encoding


# --- full replays -------------------------------------------------------------


@pytest.mark.asyncio
async def test_uptrend_history_produces_high_profit_factor() -> None:
    """A clean rally: every bracket resolves as HIT_TP, so the profit factor is
    maximal and the drawdown negligible."""
    transport = HistoricalTransport.from_json(uptrend_payload(), "TEST", Timeframe.ONE_MINUTE)
    result = await run_backtest("momentum", "TEST", BASE, BASE + timedelta(hours=3), transport)

    assert result.bars_replayed == 120
    assert len(result.trades) >= 5
    assert result.win_rate_pct == pytest.approx(100.0)
    profit_factor = result.profit_factor
    assert profit_factor is not None and (profit_factor == float("inf") or profit_factor > 3.0)
    assert result.total_return_pct > 1.0
    assert result.max_drawdown_pct < 1.0
    assert all(trade.bracket_status == "HIT_TP" for trade in result.trades)


@pytest.mark.asyncio
async def test_backtest_is_reproducible_with_same_seed() -> None:
    transport = HistoricalTransport.from_json(uptrend_payload(), "TEST", Timeframe.ONE_MINUTE)
    first = await run_backtest("momentum", "TEST", BASE, BASE + timedelta(hours=3), transport, seed=11)
    second = await run_backtest("momentum", "TEST", BASE, BASE + timedelta(hours=3), transport, seed=11)
    assert first.net_pnl == pytest.approx(second.net_pnl)
    assert len(first.trades) == len(second.trades)


@pytest.mark.asyncio
async def test_catastrophic_whipsaw_trips_risk_guard_and_blocks_entries() -> None:
    """Every breakout stops out for ~-8% of the 5k allocation (~-430 with
    fees). With MAX_DAILY_LOSS_PCT=1% of 100k the guard must trip after ~3
    stop-outs and reject every subsequent entry: 10 whipsaw cycles but only
    ~3 trades ever fill."""
    guard = RiskGuard(100_000.0, max_daily_loss_pct=0.01, max_daily_trade_count=50)
    transport = HistoricalTransport.from_json(whipsaw_payload(cycles=10), "TEST", Timeframe.ONE_MINUTE)
    result = await run_backtest("momentum", "TEST", BASE, BASE + timedelta(hours=3), transport, risk_guard=guard)

    assert result.risk_guard_status is not None
    assert result.risk_guard_status["locked"] is True
    assert "max_daily_loss" in result.risk_guard_status["locked_reason"]
    assert result.net_pnl <= -1_000.0  # the daily budget was genuinely burned
    # the lock held: far fewer fills than whipsaw cycles offered
    assert len(result.trades) < 10
    assert result.risk_guard_status["daily_entry_count"] == len(result.trades)
    assert all(trade.bracket_status == "HIT_SL" for trade in result.trades)
    assert result.win_rate_pct == pytest.approx(0.0)


@pytest.mark.asyncio
async def test_trade_count_cap_limits_entries_in_replay() -> None:
    guard = RiskGuard(100_000.0, max_daily_loss_pct=0.5, max_daily_trade_count=2)
    transport = HistoricalTransport.from_json(uptrend_payload(), "TEST", Timeframe.ONE_MINUTE)
    result = await run_backtest("momentum", "TEST", BASE, BASE + timedelta(hours=3), transport, risk_guard=guard)

    assert len(result.trades) == 2  # third and later breakouts were rejected
    assert result.risk_guard_status is not None
    assert result.risk_guard_status["locked"] is True
    assert "max_daily_trade_count" in result.risk_guard_status["locked_reason"]


@pytest.mark.asyncio
async def test_empty_window_raises() -> None:
    transport = HistoricalTransport.from_json(uptrend_payload(10), "TEST", Timeframe.ONE_MINUTE)
    with pytest.raises(BacktestError, match="no historical bars"):
        await run_backtest("momentum", "TEST", BASE + timedelta(days=30), BASE + timedelta(days=31), transport)


@pytest.mark.asyncio
async def test_unknown_strategy_rejected() -> None:
    transport = HistoricalTransport.from_json(uptrend_payload(10), "TEST", Timeframe.ONE_MINUTE)
    with pytest.raises(BacktestError, match="unknown strategy"):
        await run_backtest("scalper", "TEST", BASE, BASE + timedelta(hours=1), transport)


@pytest.mark.asyncio
async def test_swing_strategy_replays_through_the_same_loop() -> None:
    """The swing engine runs the identical replay loop on 4h bars — a fabricated
    trendline-retest history produces its pivot-bracket trade."""

    def bar4h(i: int, open_: float, high: float, low: float, close: float) -> dict[str, Any]:
        return {
            "timestamp": (BASE + timedelta(hours=4 * i)).isoformat(),
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": 1_000,
        }

    lows = {0: 20, 1: 15, 2: 10, 3: 15, 4: 20, 5: 20, 6: 15, 7: 12, 8: 15, 9: 20, 10: 20, 11: 15, 12: 14}
    rows = [bar4h(i, lows[i] + 2, lows[i] + 3, lows[i], lows[i] + 1) for i in range(13)]
    rows.append(bar4h(13, 14.6, 14.7, 14.1, 14.2))
    rows.append(bar4h(14, 14.15, 14.9, 14.05, 14.8))  # trendline retest + engulfing -> entry
    rows.append(bar4h(15, 22.0, 23.5, 21.5, 23.2))  # tags the 23.0 resistance TP

    # pivot_window default is 5 in the swing engine, but build_backtester uses the
    # engine defaults — this fixture was built for window=2, so replay manually.
    from backend.engine.backtester import BacktestRecorder, Backtester
    from backend.execution_gateway import MockExecutionGateway
    from backend.risk_manager import RiskManager
    from backend.strategies.swing_engine import SwingEngine

    recorder = BacktestRecorder()
    engine = SwingEngine(
        "TEST",
        pivot_window=2,
        risk_manager=RiskManager(),
        persistence=recorder,
        gateway=MockExecutionGateway(min_slippage_pct=0.0, max_slippage_pct=0.0, latency_range_ms=(0.0, 0.0)),
    )
    transport = HistoricalTransport.from_json(rows, "TEST", Timeframe.FOUR_HOUR)
    backtester = Backtester(
        engine, transport, "TEST", BASE, BASE + timedelta(hours=64), recorder=recorder, initial_capital=100_000.0
    )
    result = await backtester.run()

    assert len(result.trades) == 1
    assert result.trades[0].bracket_status == "HIT_TP"
    assert result.trades[0].exit_price == pytest.approx(23.0)
    assert result.win_rate_pct == pytest.approx(100.0)
