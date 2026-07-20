"""Verification tests for backend/db.py: the async SQLAlchemy persistence layer."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from backend.db import Database
from backend.models import OpenPositionRecord, TradeRecord

BASE_TIME = datetime(2026, 7, 20, 9, 30, tzinfo=timezone.utc)


def make_trade(
    engine_type: str = "momentum",
    offset_minutes: int = 0,
    net_profit: float = 10.0,
    slippage_cost: float = 0.0,
) -> TradeRecord:
    entry = BASE_TIME + timedelta(minutes=offset_minutes)
    exit_ = entry + timedelta(minutes=5)
    return TradeRecord(
        engine_type=engine_type,
        asset_ticker="MOCK",
        entry_timestamp=entry,
        exit_timestamp=exit_,
        entry_price=100.05,
        exit_price=100.05 + net_profit,
        position_size=5000.0,
        fees=2.5,
        net_profit=net_profit,
        requested_price=100.0,
        actual_filled_price=100.05,
        slippage_cost=slippage_cost,
    )


def make_open_position(engine_type: str = "momentum", ticker: str = "MOCK", order_id: str = "mock-1") -> OpenPositionRecord:
    return OpenPositionRecord(
        engine_type=engine_type,
        asset_ticker=ticker,
        side="long",
        entry_timestamp=BASE_TIME,
        entry_price=100.05,
        requested_entry_price=100.0,
        position_size=5000.0,
        entry_fees=2.5,
        entry_order_id=order_id,
    )


@pytest.fixture
async def db() -> AsyncGenerator[Database, None]:
    database = Database("sqlite+aiosqlite:///:memory:")
    await database.init()
    yield database
    await database.dispose()


def as_utc(value: datetime) -> datetime:
    """SQLite round-trips datetimes as naive; our system's invariant is that
    everything stored is already UTC, so tests reattach that tzinfo explicitly."""
    return value.replace(tzinfo=timezone.utc)


@pytest.mark.asyncio
async def test_new_database_starts_empty(db: Database) -> None:
    assert await db.get_trades("momentum") == []
    assert await db.get_equity_curve("momentum") == []
    assert await db.get_latest_equity("momentum") == 0.0


@pytest.mark.asyncio
async def test_record_and_query_trade(db: Database) -> None:
    await db.record_trade(make_trade("momentum", net_profit=42.0))

    trades = await db.get_trades("momentum")
    assert len(trades) == 1
    trade = trades[0]
    assert trade.engine_type == "momentum"
    assert trade.asset_ticker == "MOCK"
    assert trade.net_profit == 42.0
    assert trade.position_size == 5000.0
    assert trade.fees == 2.5
    assert as_utc(trade.entry_timestamp) == BASE_TIME
    assert as_utc(trade.exit_timestamp) == BASE_TIME + timedelta(minutes=5)


@pytest.mark.asyncio
async def test_trades_are_isolated_by_engine_type(db: Database) -> None:
    await db.record_trade(make_trade("momentum", net_profit=10.0))
    await db.record_trade(make_trade("swing", net_profit=20.0))

    momentum_trades = await db.get_trades("momentum")
    swing_trades = await db.get_trades("swing")

    assert len(momentum_trades) == 1 and momentum_trades[0].net_profit == 10.0
    assert len(swing_trades) == 1 and swing_trades[0].net_profit == 20.0


@pytest.mark.asyncio
async def test_equity_curve_returned_in_chronological_order(db: Database) -> None:
    later = BASE_TIME + timedelta(hours=1)
    earlier = BASE_TIME

    # Insert out of order to verify the query sorts, not the insert order.
    await db.record_equity_snapshot("momentum", later, equity=20.0)
    await db.record_equity_snapshot("momentum", earlier, equity=10.0)

    curve = await db.get_equity_curve("momentum")
    assert [as_utc(point.timestamp) for point in curve] == [earlier, later]
    assert [point.equity for point in curve] == [10.0, 20.0]


@pytest.mark.asyncio
async def test_get_latest_equity_returns_most_recent_by_timestamp(db: Database) -> None:
    await db.record_equity_snapshot("momentum", BASE_TIME, equity=10.0)
    await db.record_equity_snapshot("momentum", BASE_TIME + timedelta(hours=2), equity=30.0)
    # Inserted last but timestamped earlier than the row above — must not win.
    await db.record_equity_snapshot("momentum", BASE_TIME + timedelta(hours=1), equity=20.0)

    assert await db.get_latest_equity("momentum") == 30.0


@pytest.mark.asyncio
async def test_data_persists_across_a_simulated_restart(tmp_path: Path) -> None:
    """Writes through one `Database` instance, then opens a fresh instance
    against the same file to prove data survives a process restart."""
    db_path = tmp_path / "traderz_test.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"

    first = Database(database_url)
    await first.init()
    await first.record_trade(make_trade("swing", net_profit=99.0))
    await first.record_equity_snapshot("swing", BASE_TIME, equity=99.0)
    await first.dispose()

    restarted = Database(database_url)
    await restarted.init()  # CREATE TABLE IF NOT EXISTS — must not wipe existing data

    trades = await restarted.get_trades("swing")
    assert len(trades) == 1
    assert trades[0].net_profit == 99.0
    assert await restarted.get_latest_equity("swing") == 99.0
    await restarted.dispose()


# --- Phase 3: slippage columns, open positions, aggregates -------------------


@pytest.mark.asyncio
async def test_trade_slippage_columns_round_trip(db: Database) -> None:
    await db.record_trade(make_trade(slippage_cost=7.25))
    trade = (await db.get_trades("momentum"))[0]
    assert trade.requested_price == 100.0
    assert trade.actual_filled_price == 100.05
    assert trade.slippage_cost == 7.25


@pytest.mark.asyncio
async def test_total_slippage_cost_sums_across_engines(db: Database) -> None:
    assert await db.total_slippage_cost() == 0.0  # empty table -> 0, not NULL
    await db.record_trade(make_trade("momentum", slippage_cost=3.0))
    await db.record_trade(make_trade("swing", offset_minutes=10, slippage_cost=4.5))
    assert await db.total_slippage_cost() == pytest.approx(7.5)


@pytest.mark.asyncio
async def test_open_position_record_list_clear_lifecycle(db: Database) -> None:
    await db.record_open_position(make_open_position("momentum", order_id="mock-1"))
    await db.record_open_position(make_open_position("swing", ticker="OTHER", order_id="mock-2"))

    all_positions = await db.list_open_positions()
    assert {p.entry_order_id for p in all_positions} == {"mock-1", "mock-2"}

    momentum_only = await db.list_open_positions("momentum")
    assert len(momentum_only) == 1
    position = momentum_only[0]
    assert position.side == "long"
    assert position.entry_price == 100.05
    assert position.requested_entry_price == 100.0

    await db.clear_open_position("momentum", "MOCK")
    remaining = await db.list_open_positions()
    assert [p.entry_order_id for p in remaining] == ["mock-2"]


@pytest.mark.asyncio
async def test_open_positions_survive_a_simulated_restart(tmp_path: Path) -> None:
    """The whole point of persisting open positions is crash recovery — they
    must be readable by a fresh Database instance over the same file."""
    database_url = f"sqlite+aiosqlite:///{tmp_path / 'traderz_test.db'}"
    first = Database(database_url)
    await first.init()
    await first.record_open_position(make_open_position(order_id="mock-9"))
    await first.dispose()

    restarted = Database(database_url)
    await restarted.init()
    positions = await restarted.list_open_positions()
    assert [p.entry_order_id for p in positions] == ["mock-9"]
    await restarted.dispose()


@pytest.mark.asyncio
async def test_bracket_columns_round_trip(db: Database) -> None:
    """Phase 5: SL/TP levels and the bracket outcome are permanently persisted."""
    await db.record_trade(
        TradeRecord(
            engine_type="momentum",
            asset_ticker="MOCK",
            entry_timestamp=BASE_TIME,
            exit_timestamp=BASE_TIME + timedelta(minutes=3),
            entry_price=106.0,
            exit_price=119.75,
            position_size=5000.0,
            fees=10.0,
            net_profit=638.58,
            stop_loss_price=97.75,
            take_profit_price=119.75,
            bracket_status="HIT_TP",
        )
    )
    trade = (await db.get_trades("momentum"))[0]
    assert trade.stop_loss_price == 97.75
    assert trade.take_profit_price == 119.75
    assert trade.bracket_status == "HIT_TP"


@pytest.mark.asyncio
async def test_legacy_trades_default_to_empty_bracket_status(db: Database) -> None:
    await db.record_trade(make_trade())  # helper predates brackets: no bracket args
    trade = (await db.get_trades("momentum"))[0]
    assert trade.bracket_status == ""
    assert trade.stop_loss_price == 0.0


# --- Phase 7: WAL mode, session scope, SystemState persistence ---------------


@pytest.mark.asyncio
async def test_file_database_runs_in_wal_mode(tmp_path: Path) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'wal.db'}")
    await database.init()
    assert await database.journal_mode() == "wal"
    await database.dispose()


@pytest.mark.asyncio
async def test_in_memory_database_keeps_memory_journal(db: Database) -> None:
    assert await db.journal_mode() == "memory"  # WAL applies to file DBs only


@pytest.mark.asyncio
async def test_get_db_session_commits_on_success_and_rolls_back_on_error(db: Database) -> None:
    from backend.db import Trade

    async with db.get_db_session() as session:
        session.add(
            Trade(
                engine_type="momentum",
                asset_ticker="MOCK",
                entry_timestamp=BASE_TIME,
                exit_timestamp=BASE_TIME,
                entry_price=1.0,
                exit_price=1.0,
                position_size=1.0,
                fees=0.0,
                net_profit=0.0,
            )
        )
    assert len(await db.get_trades("momentum")) == 1  # committed by the scope

    with pytest.raises(RuntimeError):
        async with db.get_db_session() as session:
            session.add(
                Trade(
                    engine_type="momentum",
                    asset_ticker="MOCK",
                    entry_timestamp=BASE_TIME,
                    exit_timestamp=BASE_TIME,
                    entry_price=2.0,
                    exit_price=2.0,
                    position_size=1.0,
                    fees=0.0,
                    net_profit=0.0,
                )
            )
            raise RuntimeError("boom")
    assert len(await db.get_trades("momentum")) == 1  # rolled back


@pytest.mark.asyncio
async def test_concurrent_writers_and_readers_never_lock(tmp_path: Path) -> None:
    """WAL + busy_timeout + scoped sessions: parallel streams of writes and
    analytics reads must complete without 'database is locked'."""
    import asyncio

    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'concurrent.db'}")
    await database.init()

    async def writer(worker: int) -> None:
        for i in range(10):
            await database.record_trade(make_trade("momentum", offset_minutes=worker * 100 + i))

    async def reader() -> None:
        for _ in range(20):
            await database.get_trades("momentum")
            await database.total_slippage_cost()

    await asyncio.gather(*[writer(w) for w in range(5)], *[reader() for _ in range(5)])
    assert len(await database.get_trades("momentum")) == 50
    await database.dispose()


@pytest.mark.asyncio
async def test_system_state_upsert_and_load(db: Database) -> None:
    await db.save_system_state(
        {
            "state_date": "2026-07-20",
            "daily_realized_pnl": -1250.5,
            "daily_entry_count": 7,
            "locked_reason": "",
            "circuit_breaker_active": False,
        }
    )
    await db.save_system_state(
        {
            "state_date": "2026-07-20",  # same date -> update, not a second row
            "daily_realized_pnl": -3100.0,
            "daily_entry_count": 9,
            "locked_reason": "max_daily_loss_3.00%_exceeded",
            "circuit_breaker_active": False,
        }
    )
    row = await db.load_system_state("2026-07-20")
    assert row is not None
    assert row["daily_realized_pnl"] == -3100.0
    assert row["daily_entry_count"] == 9
    assert "max_daily_loss" in row["locked_reason"]
    assert row["updated_at"] is not None


@pytest.mark.asyncio
async def test_load_latest_system_state_picks_newest_date(db: Database) -> None:
    assert await db.load_latest_system_state() is None
    for day, pnl in (("2026-07-18", 5.0), ("2026-07-20", -40.0), ("2026-07-19", 10.0)):
        await db.save_system_state(
            {
                "state_date": day,
                "daily_realized_pnl": pnl,
                "daily_entry_count": 1,
                "locked_reason": "",
                "circuit_breaker_active": False,
            }
        )
    latest = await db.load_latest_system_state()
    assert latest is not None and latest["state_date"] == "2026-07-20"
