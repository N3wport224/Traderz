"""Verification tests for backend/risk_manager.py: capital allocation and the
cross-engine daily drawdown circuit breaker."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from backend.risk_manager import RiskManager

DAY_1 = datetime(2026, 7, 20, 9, 30, tzinfo=timezone.utc)
DAY_2 = DAY_1 + timedelta(days=1)


def make_risk_manager(**overrides: object) -> RiskManager:
    defaults: dict[str, object] = {
        "total_capital": 100_000.0,
        "max_daily_drawdown_pct": 0.02,
        "allocation_pct": {"momentum": 0.05, "swing": 0.15},
        "fee_rate": 0.0005,
    }
    defaults.update(overrides)
    return RiskManager(**defaults)  # type: ignore[arg-type]


# --- Capital allocation ----------------------------------------------------


def test_position_size_respects_per_engine_allocation_limits() -> None:
    rm = make_risk_manager()
    assert rm.position_size("momentum") == 100_000.0 * 0.05
    assert rm.position_size("swing") == 100_000.0 * 0.15


def test_compute_fees_is_a_fraction_of_notional() -> None:
    rm = make_risk_manager(fee_rate=0.001)
    assert rm.compute_fees(10_000.0) == 10.0
    assert rm.compute_fees(-10_000.0) == 10.0  # notional magnitude, sign-independent


# --- Circuit breaker: single-engine trip ------------------------------------


def test_can_open_position_true_by_default() -> None:
    rm = make_risk_manager()
    assert rm.can_open_position(DAY_1) is True


def test_small_loss_does_not_trip_breaker() -> None:
    rm = make_risk_manager()
    # 2% of 100k = 2000 threshold; well under it.
    tripped = rm.record_realized_pnl("momentum", -500.0, DAY_1)
    assert tripped is False
    assert rm.is_halted(DAY_1) is False
    assert rm.can_open_position(DAY_1) is True


def test_loss_at_exact_threshold_trips_breaker() -> None:
    rm = make_risk_manager()
    tripped = rm.record_realized_pnl("momentum", -2_000.0, DAY_1)
    assert tripped is True
    assert rm.is_halted(DAY_1) is True
    assert rm.can_open_position(DAY_1) is False


def test_breaker_trip_returns_true_only_once() -> None:
    rm = make_risk_manager()
    assert rm.record_realized_pnl("momentum", -2_500.0, DAY_1) is True
    # Already halted — further losses must not re-report a fresh trip.
    assert rm.record_realized_pnl("momentum", -100.0, DAY_1) is False


# --- Circuit breaker: aggregated across both engines ------------------------


def test_combined_losses_across_both_engines_trip_the_shared_breaker() -> None:
    """Neither engine alone crosses 2% of capital, but their combined loss does —
    the risk manager tracks total net loss across both engines, not per-engine."""
    rm = make_risk_manager()
    assert rm.record_realized_pnl("momentum", -1_200.0, DAY_1) is False
    assert rm.can_open_position(DAY_1) is True

    tripped = rm.record_realized_pnl("swing", -900.0, DAY_1)
    assert tripped is True
    assert rm.can_open_position(DAY_1) is False


def test_gains_offset_losses_before_tripping() -> None:
    rm = make_risk_manager()
    rm.record_realized_pnl("momentum", -1_800.0, DAY_1)
    rm.record_realized_pnl("swing", 1_000.0, DAY_1)  # net -800, still under threshold
    assert rm.can_open_position(DAY_1) is True
    tripped = rm.record_realized_pnl("momentum", -1_500.0, DAY_1)  # net -2300, now over
    assert tripped is True


# --- Daily reset -------------------------------------------------------------


def test_breaker_resets_automatically_on_a_new_calendar_day() -> None:
    rm = make_risk_manager()
    rm.record_realized_pnl("momentum", -3_000.0, DAY_1)
    assert rm.can_open_position(DAY_1) is False

    assert rm.can_open_position(DAY_2) is True
    assert rm.is_halted(DAY_2) is False


def test_new_day_pnl_accumulator_starts_fresh() -> None:
    rm = make_risk_manager()
    rm.record_realized_pnl("momentum", -1_900.0, DAY_1)
    # New day: a loss that alone wouldn't trip should not inherit yesterday's balance.
    tripped = rm.record_realized_pnl("momentum", -1_900.0, DAY_2)
    assert tripped is False
    assert rm.can_open_position(DAY_2) is True


# --- Manual pause/resume -----------------------------------------------------


def test_pause_blocks_new_entries_without_halting() -> None:
    rm = make_risk_manager()
    rm.pause()
    assert rm.can_open_position(DAY_1) is False
    assert rm.is_halted(DAY_1) is False  # paused, not drawdown-halted
    assert rm.system_status() == "PAUSED"


def test_resume_restores_entries() -> None:
    rm = make_risk_manager()
    rm.pause()
    rm.resume()
    assert rm.can_open_position(DAY_1) is True
    assert rm.system_status() == "RUNNING"


def test_pause_survives_a_day_rollover() -> None:
    rm = make_risk_manager()
    rm.pause()
    assert rm.can_open_position(DAY_2) is False  # manual pause is not a daily-reset concept


# --- System status / status snapshot ----------------------------------------


def test_system_status_running_by_default() -> None:
    rm = make_risk_manager()
    assert rm.system_status(DAY_1) == "RUNNING"


def test_system_status_halted_by_drawdown_takes_priority_over_paused() -> None:
    rm = make_risk_manager()
    rm.pause()
    rm.record_realized_pnl("momentum", -3_000.0, DAY_1)
    assert rm.system_status(DAY_1) == "HALTED_BY_DRAWDOWN"


def test_status_snapshot_reports_expected_fields() -> None:
    rm = make_risk_manager()
    rm.record_realized_pnl("momentum", -1_000.0, DAY_1)
    snapshot = rm.status(DAY_1)

    assert snapshot["system_status"] == "RUNNING"
    assert snapshot["halted"] is False
    assert snapshot["paused"] is False
    assert snapshot["daily_pnl"] == -1_000.0
    assert snapshot["daily_drawdown_pct"] == 0.01
    assert snapshot["max_daily_drawdown_pct"] == 0.02
    assert snapshot["total_capital"] == 100_000.0
    assert snapshot["allocation_pct"] == {"momentum": 0.05, "swing": 0.15}
    assert snapshot["current_date"] == DAY_1.date().isoformat()


def test_status_snapshot_reports_halted_reason_after_trip() -> None:
    rm = make_risk_manager()
    rm.record_realized_pnl("swing", -3_000.0, DAY_1)
    snapshot = rm.status(DAY_1)
    assert snapshot["halted"] is True
    assert snapshot["halted_reason"] is not None
    assert "swing" in str(snapshot["halted_reason"])


# --- Phase 3: DATA_DISCONNECTED state ----------------------------------------

BASE_TIME = DAY_1


def test_data_disconnected_blocks_entries_for_that_ticker_only() -> None:
    rm = RiskManager()
    rm.mark_data_disconnected("MOCK")

    assert rm.can_open_position(BASE_TIME, ticker="MOCK") is False
    assert rm.can_open_position(BASE_TIME, ticker="OTHER") is True
    assert rm.can_open_position(BASE_TIME) is True  # ticker-agnostic callers unaffected


def test_mark_data_verified_clears_the_block() -> None:
    rm = RiskManager()
    rm.mark_data_disconnected("MOCK")
    assert rm.is_data_disconnected("MOCK") is True
    assert rm.is_data_disconnected() is True

    rm.mark_data_verified("MOCK")
    assert rm.is_data_disconnected("MOCK") is False
    assert rm.is_data_disconnected() is False
    assert rm.can_open_position(BASE_TIME, ticker="MOCK") is True


def test_mark_data_verified_is_safe_when_not_disconnected() -> None:
    rm = RiskManager()
    rm.mark_data_verified("NEVER_MARKED")  # must not raise
    assert rm.is_data_disconnected() is False


def test_system_status_reports_data_disconnected() -> None:
    rm = RiskManager()
    rm.mark_data_disconnected("MOCK")
    assert rm.system_status() == "DATA_DISCONNECTED"

    status = rm.status()
    assert status["data_disconnected"] is True
    assert status["disconnected_tickers"] == ["MOCK"]


def test_drawdown_halt_outranks_data_disconnected_in_status() -> None:
    """An active circuit breaker is the more severe condition — it must win the
    single system_status slot even while a stream is also down."""
    rm = RiskManager(total_capital=10_000.0, max_daily_drawdown_pct=0.01)
    rm.record_realized_pnl("momentum", -200.0, BASE_TIME)  # trips the breaker
    rm.mark_data_disconnected("MOCK")
    assert rm.system_status() == "HALTED_BY_DRAWDOWN"


def test_data_disconnected_outranks_paused_in_status() -> None:
    rm = RiskManager()
    rm.pause()
    rm.mark_data_disconnected("MOCK")
    assert rm.system_status() == "DATA_DISCONNECTED"
    rm.mark_data_verified("MOCK")
    assert rm.system_status() == "PAUSED"


def test_multiple_disconnected_tickers_all_reported_sorted() -> None:
    rm = RiskManager()
    rm.mark_data_disconnected("ZED")
    rm.mark_data_disconnected("ABC")
    assert rm.disconnected_tickers == ["ABC", "ZED"]
    rm.mark_data_verified("ZED")
    assert rm.disconnected_tickers == ["ABC"]
    assert rm.system_status() == "DATA_DISCONNECTED"  # one broken ticker is enough
