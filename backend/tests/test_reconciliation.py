"""Tests for boot-time reconciliation: healing DB records lost in a crash
between gateway fill and database write, and clearing stale rows for positions
already closed at the broker."""

from __future__ import annotations

import random
from collections.abc import AsyncGenerator
from datetime import datetime, timezone

import pytest
import pytest_asyncio

from backend.db import Database
from backend.execution_gateway import MockExecutionGateway
from backend.models import OpenPositionRecord, SignalAction
from backend.reconciliation import reconcile_on_boot


@pytest_asyncio.fixture
async def db() -> AsyncGenerator[Database, None]:
    database = Database("sqlite+aiosqlite:///:memory:")
    await database.init()
    yield database
    await database.dispose()


def gateway() -> MockExecutionGateway:
    return MockExecutionGateway(latency_range_ms=(0.0, 0.5), rng=random.Random(9))


def stale_position(order_id: str = "stale-1") -> OpenPositionRecord:
    return OpenPositionRecord(
        engine_type="swing",
        asset_ticker="GONE",
        side="long",
        entry_timestamp=datetime.now(timezone.utc),
        entry_price=10.0,
        requested_entry_price=10.0,
        position_size=1_000.0,
        entry_fees=0.5,
        entry_order_id=order_id,
    )


@pytest.mark.asyncio
async def test_clean_state_reconciles_with_no_discrepancies(db: Database) -> None:
    report = await reconcile_on_boot(gateway(), db)
    assert report.discrepancies == 0
    assert report.matched == []


@pytest.mark.asyncio
async def test_missing_db_record_is_healed_from_gateway_order(db: Database) -> None:
    """Crash after the entry filled but before the DB write: the gateway still
    holds the open order, so boot reconciliation must reconstruct the row."""
    gw = gateway()
    fill = await gw.execute_order(SignalAction.BUY, 5_000.0, "MOCK", 100.0)

    report = await reconcile_on_boot(gw, db)

    assert [p.entry_order_id for p in report.healed] == [fill.order_id]
    positions = await db.list_open_positions()
    assert len(positions) == 1
    healed = positions[0]
    assert healed.asset_ticker == "MOCK"
    assert healed.side == "long"
    assert healed.entry_price == pytest.approx(fill.filled_price)
    assert healed.position_size == pytest.approx(fill.filled_size)
    assert healed.engine_type == "recovered"  # flagged so operators can spot it


@pytest.mark.asyncio
async def test_short_entry_heals_with_short_side(db: Database) -> None:
    gw = gateway()
    await gw.execute_order(SignalAction.SHORT, 3_000.0, "MOCK", 50.0)
    await reconcile_on_boot(gw, db)
    positions = await db.list_open_positions()
    assert positions[0].side == "short"


@pytest.mark.asyncio
async def test_stale_db_record_is_cleared_when_broker_has_no_order(db: Database) -> None:
    """Crash after the broker closed the position but before the local close
    was recorded: the stale open_positions row must be removed."""
    await db.record_open_position(stale_position())

    report = await reconcile_on_boot(gateway(), db)

    assert [p.entry_order_id for p in report.cleared] == ["stale-1"]
    assert await db.list_open_positions() == []


@pytest.mark.asyncio
async def test_matched_positions_are_left_untouched(db: Database) -> None:
    gw = gateway()
    fill = await gw.execute_order(SignalAction.BUY, 5_000.0, "MOCK", 100.0)
    await db.record_open_position(
        OpenPositionRecord(
            engine_type="momentum",
            asset_ticker="MOCK",
            side="long",
            entry_timestamp=fill.timestamp,
            entry_price=fill.filled_price,
            requested_entry_price=fill.requested_price,
            position_size=fill.filled_size,
            entry_fees=fill.fees,
            entry_order_id=fill.order_id,
        )
    )

    report = await reconcile_on_boot(gw, db)

    assert report.matched == [fill.order_id]
    assert report.discrepancies == 0
    positions = await db.list_open_positions()
    assert len(positions) == 1
    assert positions[0].engine_type == "momentum"  # not relabeled or duplicated


@pytest.mark.asyncio
async def test_second_pass_is_idempotent(db: Database) -> None:
    """Reconciliation must converge: a second boot right after the first finds
    nothing left to fix."""
    gw = gateway()
    await gw.execute_order(SignalAction.BUY, 5_000.0, "MOCK", 100.0)
    await db.record_open_position(stale_position())

    first = await reconcile_on_boot(gw, db)
    assert first.discrepancies == 2  # one healed + one cleared

    second = await reconcile_on_boot(gw, db)
    assert second.discrepancies == 0
    assert len(second.matched) == 1


@pytest.mark.asyncio
async def test_mixed_state_heals_and_clears_in_one_pass(db: Database) -> None:
    gw = gateway()
    healed_fill = await gw.execute_order(SignalAction.BUY, 5_000.0, "AAA", 100.0)
    matched_fill = await gw.execute_order(SignalAction.SHORT, 2_000.0, "BBB", 40.0)
    await db.record_open_position(
        OpenPositionRecord(
            engine_type="momentum",
            asset_ticker="BBB",
            side="short",
            entry_timestamp=matched_fill.timestamp,
            entry_price=matched_fill.filled_price,
            requested_entry_price=matched_fill.requested_price,
            position_size=matched_fill.filled_size,
            entry_fees=matched_fill.fees,
            entry_order_id=matched_fill.order_id,
        )
    )
    await db.record_open_position(stale_position("stale-9"))

    report = await reconcile_on_boot(gw, db)

    assert report.matched == [matched_fill.order_id]
    assert [p.entry_order_id for p in report.healed] == [healed_fill.order_id]
    assert [p.entry_order_id for p in report.cleared] == ["stale-9"]
    tickers = {p.asset_ticker for p in await db.list_open_positions()}
    assert tickers == {"AAA", "BBB"}
