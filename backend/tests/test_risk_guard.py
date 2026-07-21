"""Tests for the operational RiskGuard and its gateway integration: daily loss
threshold, trade-count cap, manual circuit breaker, calendar rollover, and the
entries-blocked/exits-allowed contract inside the execution path."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from backend.execution_gateway import MockExecutionGateway
from backend.models import SignalAction
from backend.utils.risk_guard import RiskGuard, RiskGuardTripped

DAY_1 = datetime(2026, 7, 20, 9, 30, tzinfo=timezone.utc)
DAY_2 = DAY_1 + timedelta(days=1)


def make_guard(**overrides: object) -> RiskGuard:
    defaults: dict[str, object] = {
        "max_daily_loss_pct": 0.03,
        "max_daily_trade_count": 5,
    }
    defaults.update(overrides)
    return RiskGuard(100_000.0, **defaults)  # type: ignore[arg-type]


def zero_slip() -> MockExecutionGateway:
    return MockExecutionGateway(min_slippage_pct=0.0, max_slippage_pct=0.0, latency_range_ms=(0.0, 0.0))


# --- daily loss threshold -----------------------------------------------------


def test_losses_below_threshold_do_not_trip() -> None:
    guard = make_guard()
    guard.record_realized_pnl(-1_000.0, DAY_1)
    guard.record_realized_pnl(-1_500.0, DAY_1)
    assert guard.locked is False
    guard.validate_entry(DAY_1)  # still tradeable


def test_daily_loss_threshold_trips_hard_lock_and_fires_callback() -> None:
    tripped: list[str] = []
    guard = make_guard(on_trip=tripped.append)
    guard.record_realized_pnl(-3_000.0, DAY_1)  # exactly -3% of 100k

    assert guard.locked is True
    assert guard.locked_reason is not None and "max_daily_loss" in guard.locked_reason
    assert tripped and "max_daily_loss" in tripped[0]
    with pytest.raises(RiskGuardTripped):
        guard.validate_entry(DAY_1)


def test_gains_offset_losses_within_the_day() -> None:
    guard = make_guard()
    guard.record_realized_pnl(-2_500.0, DAY_1)
    guard.record_realized_pnl(+2_000.0, DAY_1)
    guard.record_realized_pnl(-2_000.0, DAY_1)  # net -2500 > -3000
    assert guard.locked is False


def test_daily_loss_lock_clears_on_new_calendar_day() -> None:
    guard = make_guard()
    guard.record_realized_pnl(-5_000.0, DAY_1)
    assert guard.locked is True
    guard.roll_day(DAY_2)
    assert guard.locked is False
    assert guard.status()["daily_realized_pnl"] == 0.0
    guard.validate_entry(DAY_2)


# --- trade count cap ----------------------------------------------------------


def test_trade_count_cap_blocks_further_entries() -> None:
    guard = make_guard(max_daily_trade_count=2)
    for _ in range(2):
        guard.validate_entry(DAY_1)
        guard.register_entry(DAY_1)
    with pytest.raises(RiskGuardTripped):
        guard.validate_entry(DAY_1)
    assert guard.locked is True
    assert guard.locked_reason is not None and "max_daily_trade_count" in guard.locked_reason


def test_trade_count_resets_on_new_day() -> None:
    guard = make_guard(max_daily_trade_count=1)
    guard.register_entry(DAY_1)
    with pytest.raises(RiskGuardTripped):
        guard.validate_entry(DAY_1)
    guard.validate_entry(DAY_2)  # fresh budget


def test_backwards_timestamp_never_resets_the_day() -> None:
    """The day roll is strictly forward-only. Mixed clocks are real: a backtest
    replays historical bar timestamps while gateway fills carry wall-clock
    stamps — an earlier date arriving after a later one must not wipe the
    day's counters or release a lock."""
    guard = make_guard(max_daily_trade_count=5)
    guard.register_entry(DAY_2)
    guard.record_realized_pnl(-5_000.0, DAY_2)  # trips the 3% lock
    assert guard.locked is True

    guard.roll_day(DAY_1)  # stale/out-of-order timestamp from a replayed bar
    assert guard.locked is True  # lock survives
    status = guard.status()
    assert status["daily_entry_count"] == 1  # counters survive
    assert status["daily_realized_pnl"] == -5_000.0
    assert status["current_date"] == DAY_2.date().isoformat()


# --- manual circuit breaker ---------------------------------------------------


def test_forced_circuit_breaker_halts_and_survives_day_roll() -> None:
    guard = make_guard()
    guard.force_circuit_breaker(True)
    with pytest.raises(RiskGuardTripped, match="circuit_breaker_forced"):
        guard.validate_entry(DAY_1)
    guard.roll_day(DAY_2)  # the manual switch does NOT clear with the calendar
    assert guard.locked is True
    guard.force_circuit_breaker(False)
    guard.validate_entry(DAY_2)


def test_reset_clears_everything() -> None:
    guard = make_guard()
    guard.force_circuit_breaker(True)
    guard.record_realized_pnl(-9_000.0, DAY_1)
    guard.reset()
    assert guard.locked is False
    assert guard.status()["daily_realized_pnl"] == 0.0
    guard.validate_entry(DAY_1)


def test_env_construction(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MAX_DAILY_LOSS_PCT", "0.05")
    monkeypatch.setenv("MAX_DAILY_TRADE_COUNT", "7")
    monkeypatch.setenv("CIRCUIT_BREAKER_ACTIVE", "true")
    guard = RiskGuard.from_env(50_000.0)
    assert guard.max_daily_loss_pct == 0.05
    assert guard.max_daily_trade_count == 7
    assert guard.circuit_breaker_active is True


def test_invalid_limits_rejected() -> None:
    with pytest.raises(ValueError):
        RiskGuard(0.0)
    with pytest.raises(ValueError):
        RiskGuard(100_000.0, max_daily_loss_pct=0.0)
    with pytest.raises(ValueError):
        RiskGuard(100_000.0, max_daily_trade_count=0)


# --- gateway integration ------------------------------------------------------


@pytest.mark.asyncio
async def test_gateway_rejects_entries_when_guard_locked_but_allows_exits() -> None:
    """The seatbelt contract: a locked guard blocks new BUY/SHORT entries at
    the execution path, but flattening (is_exit orders) always goes through."""
    gateway = zero_slip()
    gateway.risk_guard = make_guard()
    entry = await gateway.execute_order(SignalAction.BUY, 5_000.0, "MOCK", 100.0)

    gateway.risk_guard.force_circuit_breaker(True)
    with pytest.raises(RiskGuardTripped):
        await gateway.execute_order(SignalAction.BUY, 5_000.0, "MOCK", 100.0)
    with pytest.raises(RiskGuardTripped):
        await gateway.execute_order(SignalAction.SHORT, 5_000.0, "MOCK", 100.0)

    # the open long can still be flattened
    exit_fill = await gateway.execute_order(SignalAction.SELL, 5_000.0, "MOCK", 99.0, is_exit=True)
    assert exit_fill.filled_price == pytest.approx(99.0)
    assert await gateway.fetch_open_orders() == []
    assert entry.order_id not in {o.order_id for o in await gateway.fetch_open_orders()}


@pytest.mark.asyncio
async def test_gateway_books_round_trip_pnl_into_the_guard() -> None:
    """The gateway computes realized round-trip PnL from its own fill stream
    and feeds the guard's daily loss budget — a big enough loss trips it."""
    gateway = zero_slip()
    guard = make_guard(max_daily_loss_pct=0.01)  # -1000 threshold
    gateway.risk_guard = guard

    await gateway.execute_order(SignalAction.BUY, 50_000.0, "MOCK", 100.0)
    # close 2.5% lower: pnl = -0.025 * 50k - fees on both 50k legs (25 + 25) = -1300
    await gateway.execute_order(SignalAction.SELL, 50_000.0, "MOCK", 97.5, is_exit=True)

    assert guard.locked is True
    assert guard.status()["daily_realized_pnl"] == pytest.approx(-1_300.0, abs=0.01)
    with pytest.raises(RiskGuardTripped):
        await gateway.execute_order(SignalAction.BUY, 5_000.0, "MOCK", 97.0)


@pytest.mark.asyncio
async def test_short_cover_matches_open_order_instead_of_extending_book() -> None:
    """A short cover arrives as a market BUY with is_exit=True: it must close
    the SHORT's open order (and book its PnL), not register a phantom entry."""
    gateway = zero_slip()
    guard = make_guard()
    gateway.risk_guard = guard

    await gateway.execute_order(SignalAction.SHORT, 10_000.0, "MOCK", 100.0)
    assert len(await gateway.fetch_open_orders()) == 1
    await gateway.execute_order(SignalAction.BUY, 10_000.0, "MOCK", 95.0, is_exit=True)

    assert await gateway.fetch_open_orders() == []  # matched, not extended
    # short profited 5% minus both 10k legs' fees (5 + 5)
    assert guard.status()["daily_realized_pnl"] == pytest.approx(0.05 * 10_000.0 - 10.0, abs=0.01)
    assert guard.status()["daily_entry_count"] == 1  # the cover is not an entry


@pytest.mark.asyncio
async def test_bracket_exit_flows_bypass_the_guard() -> None:
    """A bracket SL exit executed by the gateway itself is an exit — it must
    fill even while the guard is locked (that's the flattening path)."""
    from datetime import datetime, timezone as tz

    from backend.models import BracketOrder, BracketStatus, OHLCVBar, Timeframe

    gateway = zero_slip()
    guard = make_guard()
    gateway.risk_guard = guard
    await gateway.execute_order(SignalAction.BUY, 5_000.0, "MOCK", 106.0)
    await gateway.register_bracket(
        BracketOrder(
            order_id="mock-1",
            engine_type="momentum",
            ticker="MOCK",
            side="long",
            entry_price=106.0,
            stop_loss_price=97.75,
            take_profit_price=119.75,
            size=5_000.0,
            created_at=datetime(2026, 7, 20, 9, 35, tzinfo=tz.utc),
        )
    )
    guard.force_circuit_breaker(True)

    bar = OHLCVBar("MOCK", datetime(2026, 7, 20, 9, 40, tzinfo=tz.utc), Timeframe.ONE_MINUTE, 99, 100, 97, 98, 1_000)
    exit_event = await gateway.check_bracket("mock-1", bar)
    assert exit_event is not None and exit_event.status is BracketStatus.HIT_SL  # filled despite the lock


# --- Phase 7: state serialization & crash recovery ----------------------------


@pytest.mark.asyncio
async def test_every_mutation_schedules_a_persist() -> None:
    saved: list[dict[str, object]] = []

    async def hook(snapshot: dict[str, object]) -> None:
        saved.append(snapshot)

    guard = make_guard()
    guard.persist_hook = hook
    guard.roll_day(DAY_1)  # first day observed
    guard.register_entry(DAY_1)
    guard.record_realized_pnl(-500.0, DAY_1)
    guard.force_circuit_breaker(True)
    guard.reset()
    await guard.flush_persists()

    assert len(saved) >= 5
    last = saved[-1]
    assert last["state_date"] == "2026-07-20"
    assert last["daily_realized_pnl"] == 0.0  # snapshots are captured (and ordered) at schedule time


@pytest.mark.asyncio
async def test_trip_snapshot_carries_the_lock_reason() -> None:
    saved: list[dict[str, object]] = []

    async def hook(snapshot: dict[str, object]) -> None:
        saved.append(snapshot)

    guard = make_guard()
    guard.persist_hook = hook
    guard.record_realized_pnl(-9_000.0, DAY_1)  # trips
    await guard.flush_persists()
    assert any("max_daily_loss" in str(s["locked_reason"]) for s in saved)


def test_persist_hook_is_noop_outside_event_loop() -> None:
    async def hook(snapshot: dict[str, object]) -> None:  # pragma: no cover
        raise AssertionError("must not run")

    guard = make_guard()
    guard.persist_hook = hook
    guard.register_entry(DAY_1)  # sync context: silently skipped, no crash
    assert guard.status()["daily_entry_count"] == 1


@pytest.mark.asyncio
async def test_mid_day_crash_recovery_restores_exact_counters(tmp_path: Any) -> None:
    """Session 1 books entries + losses and 'crashes'; session 2 restores from
    the SystemState table and trips at the ORIGINAL cumulative threshold."""
    from backend.db import Database

    url = f"sqlite+aiosqlite:///{tmp_path / 'guard.db'}"
    first_db = Database(url)
    await first_db.init()
    first_guard = make_guard()  # -3000 threshold
    first_guard.persist_hook = first_db.save_system_state
    first_guard.register_entry(DAY_1)
    first_guard.register_entry(DAY_1)
    first_guard.record_realized_pnl(-1_800.0, DAY_1)
    await first_guard.flush_persists()
    await first_db.dispose()  # crash

    second_db = Database(url)
    await second_db.init()
    second_guard = make_guard()
    second_guard.persist_hook = second_db.save_system_state
    state = await second_db.load_latest_system_state()
    assert state is not None and second_guard.restore(state) is True

    status = second_guard.status()
    assert status["daily_realized_pnl"] == pytest.approx(-1_800.0)
    assert status["daily_entry_count"] == 2
    assert status["locked"] is False

    # zero amnesia: only -1200 more is needed to trip, not a fresh -3000
    second_guard.record_realized_pnl(-1_300.0, DAY_1)
    assert second_guard.locked is True
    await second_guard.flush_persists()
    await second_db.dispose()


@pytest.mark.asyncio
async def test_tripped_lock_survives_reboot_and_clears_next_day(tmp_path: Any) -> None:
    from backend.db import Database

    url = f"sqlite+aiosqlite:///{tmp_path / 'locked.db'}"
    first_db = Database(url)
    await first_db.init()
    first_guard = make_guard()
    first_guard.persist_hook = first_db.save_system_state
    first_guard.record_realized_pnl(-9_000.0, DAY_1)  # trip + persist
    await first_guard.flush_persists()
    await first_db.dispose()

    second_db = Database(url)
    await second_db.init()
    second_guard = make_guard()
    state = await second_db.load_latest_system_state()
    assert state is not None and second_guard.restore(state)
    assert second_guard.locked is True  # seatbelt still fastened after reboot
    with pytest.raises(RiskGuardTripped):
        second_guard.validate_entry(DAY_1)

    second_guard.roll_day(DAY_2)  # daily lock releases with the calendar
    assert second_guard.locked is False
    await second_db.dispose()


@pytest.mark.asyncio
async def test_restored_manual_breaker_survives_but_env_true_wins_either_way() -> None:
    guard = make_guard()
    assert guard.restore(
        {
            "state_date": "2026-07-20",
            "daily_realized_pnl": 0.0,
            "daily_entry_count": 0,
            "locked_reason": "",
            "circuit_breaker_active": True,  # kill switch was engaged pre-crash
        }
    )
    assert guard.locked is True
    assert guard.locked_reason == "circuit_breaker_forced"


def test_restore_rejects_empty_snapshot() -> None:
    guard = make_guard()
    assert guard.restore({"state_date": None}) is False
    assert guard.restore({}) is False
