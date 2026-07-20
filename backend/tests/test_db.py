"""Verification tests for backend/db.py: the async SQLAlchemy persistence layer."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from backend.db import Database
from backend.models import TradeRecord

BASE_TIME = datetime(2026, 7, 20, 9, 30, tzinfo=timezone.utc)


def make_trade(engine_type: str = "momentum", offset_minutes: int = 0, net_profit: float = 10.0) -> TradeRecord:
    entry = BASE_TIME + timedelta(minutes=offset_minutes)
    exit_ = entry + timedelta(minutes=5)
    return TradeRecord(
        engine_type=engine_type,
        asset_ticker="MOCK",
        entry_timestamp=entry,
        exit_timestamp=exit_,
        entry_price=100.0,
        exit_price=100.0 + net_profit,
        position_size=5000.0,
        fees=2.5,
        net_profit=net_profit,
    )


@pytest.fixture
async def db() -> Database:
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
