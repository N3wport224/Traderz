"""Async SQLAlchemy persistence layer.

Defaults to a local SQLite file for development; point `DATABASE_URL` at a
Postgres DSN (e.g. `postgresql+asyncpg://user:pass@host/db`) to scale up — no
code changes needed elsewhere, since callers only ever touch `Database`.

`Database` implements `backend.models.TradePersistence`, so it can be handed
directly to a strategy engine's constructor without the engine importing this
module (or SQLAlchemy) at all.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator, Coroutine
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import Boolean, Float, Integer, String, delete, event, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.pool import StaticPool

from backend.models import OpenPositionRecord, TradeRecord
from backend.utils.paths import default_database_url


class Base(DeclarativeBase):
    pass


class Trade(Base):
    __tablename__ = "trades"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    engine_type: Mapped[str] = mapped_column(String(32), index=True)
    asset_ticker: Mapped[str] = mapped_column(String(16))
    entry_timestamp: Mapped[datetime] = mapped_column()
    exit_timestamp: Mapped[datetime] = mapped_column()
    entry_price: Mapped[float] = mapped_column(Float)
    exit_price: Mapped[float] = mapped_column(Float)
    position_size: Mapped[float] = mapped_column(Float)
    fees: Mapped[float] = mapped_column(Float)
    net_profit: Mapped[float] = mapped_column(Float)
    # Phase 3 slippage tracking: what the engine asked for vs. what the gateway
    # actually filled at the entry, plus the combined entry+exit dollar cost.
    requested_price: Mapped[float] = mapped_column(Float, default=0.0)
    actual_filled_price: Mapped[float] = mapped_column(Float, default=0.0)
    slippage_cost: Mapped[float] = mapped_column(Float, default=0.0)
    # Phase 5 bracket levels: the SL/TP the trade ran with (post-trailing) and
    # how it exited (ACTIVE/HIT_SL/HIT_TP/TIME_EXITED; "" = legacy bracketless).
    stop_loss_price: Mapped[float] = mapped_column(Float, default=0.0)
    take_profit_price: Mapped[float] = mapped_column(Float, default=0.0)
    bracket_status: Mapped[str] = mapped_column(String(16), default="")


class OpenPosition(Base):
    """A live position awaiting its close, persisted the moment the entry fills.

    This is the database side of crash recovery: `backend/reconciliation.py`
    diffs these rows against the gateway's open orders at boot and self-heals
    whichever side is missing a record.
    """

    __tablename__ = "open_positions"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    engine_type: Mapped[str] = mapped_column(String(32), index=True)
    asset_ticker: Mapped[str] = mapped_column(String(16))
    side: Mapped[str] = mapped_column(String(8))
    entry_timestamp: Mapped[datetime] = mapped_column()
    entry_price: Mapped[float] = mapped_column(Float)
    requested_entry_price: Mapped[float] = mapped_column(Float)
    position_size: Mapped[float] = mapped_column(Float)
    entry_fees: Mapped[float] = mapped_column(Float)
    entry_order_id: Mapped[str] = mapped_column(String(64), index=True)


class EquitySnapshot(Base):
    __tablename__ = "equity_snapshots"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    engine_type: Mapped[str] = mapped_column(String(32), index=True)
    timestamp: Mapped[datetime] = mapped_column()
    equity: Mapped[float] = mapped_column(Float)


class SystemState(Base):
    """Phase 7 crash-safety: the RiskGuard's daily counters, serialized.

    One row per calendar date, upserted on every guard mutation (entry booked,
    PnL booked, trip, manual breaker flip, reset). At boot the guard reads the
    latest row back and reconstructs its state — a mid-day crash or reboot
    keeps the financial seatbelts fastened with zero amnesia.
    """

    __tablename__ = "system_state"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    state_date: Mapped[str] = mapped_column(String(10), unique=True, index=True)  # YYYY-MM-DD
    daily_realized_pnl: Mapped[float] = mapped_column(Float, default=0.0)
    daily_entry_count: Mapped[int] = mapped_column(Integer, default=0)
    locked_reason: Mapped[str] = mapped_column(String(160), default="")
    circuit_breaker_active: Mapped[bool] = mapped_column(Boolean, default=False)
    updated_at: Mapped[datetime] = mapped_column()


class WatchedTrader(Base):
    """Phase 10 copy-trading: a person whose trades the user mirrors or logs.

    `auto_follow` + `budget_amount` drive the optional mirroring: when enabled,
    every incoming BUY/SELL event for this trader immediately places a
    `budget_amount`-notional paper order through the execution gateway (subject
    to the RiskGuard, exactly like an engine entry). When disabled the event is
    recorded + notified only — the manual mode.
    """

    __tablename__ = "watched_traders"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(48), unique=True, index=True)
    asset_class: Mapped[str] = mapped_column(String(8), default="stock")  # stock | crypto
    notes: Mapped[str] = mapped_column(String(200), default="")
    auto_follow: Mapped[bool] = mapped_column(Boolean, default=False)
    budget_amount: Mapped[float] = mapped_column(Float, default=0.0)  # $ notional per copied trade
    created_at: Mapped[datetime] = mapped_column()


class TraderTradeEvent(Base):
    """One observed buy/sell by a watched trader (manually logged in the UI or
    pushed by an external integration via the webhook endpoint), plus what the
    platform did about it (notified only, or auto-followed with a fill)."""

    __tablename__ = "trader_trade_events"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    trader_id: Mapped[int] = mapped_column(Integer, index=True)
    ticker: Mapped[str] = mapped_column(String(16))
    action: Mapped[str] = mapped_column(String(8))  # BUY | SELL
    price: Mapped[float] = mapped_column(Float)
    source: Mapped[str] = mapped_column(String(16), default="manual")  # manual | webhook
    note: Mapped[str] = mapped_column(String(200), default="")
    timestamp: Mapped[datetime] = mapped_column(index=True)
    followed: Mapped[bool] = mapped_column(Boolean, default=False)
    follow_detail: Mapped[str] = mapped_column(String(200), default="")


def _engine_kwargs(database_url: str) -> dict[str, object]:
    if ":memory:" in database_url:
        # A single shared in-memory connection so all sessions see the same data.
        return {"poolclass": StaticPool, "connect_args": {"check_same_thread": False}}
    return {}


def _is_sqlite(database_url: str) -> bool:
    return database_url.startswith("sqlite")


def _install_sqlite_pragmas(engine: Any) -> None:
    """Configures every new SQLite connection for concurrent operation.

    - `journal_mode=WAL`: readers proceed while a writer commits — the fix for
      'database is locked' under live streaming + frontend analytics polling.
      (In-memory databases silently keep their 'memory' journal; WAL only
      applies to file-backed databases.)
    - `synchronous=NORMAL`: the recommended pairing with WAL — fsync at
      checkpoints instead of every commit.
    - `busy_timeout`: writers briefly wait instead of failing when the single
      writer slot is momentarily held.
    """

    @event.listens_for(engine.sync_engine, "connect")
    def _set_pragmas(dbapi_connection: Any, _record: Any) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=NORMAL")
        cursor.execute("PRAGMA busy_timeout=5000")
        cursor.close()


class Database:
    """Owns the async engine/session factory and implements `TradePersistence`."""

    def __init__(self, database_url: str | None = None) -> None:
        # Default resolves per runtime: repo-local file in development, the
        # user's app-data folder when frozen (never write inside the bundle).
        self.database_url = database_url or os.environ.get("DATABASE_URL") or default_database_url()
        self._engine = create_async_engine(self.database_url, **_engine_kwargs(self.database_url))
        if _is_sqlite(self.database_url):
            _install_sqlite_pragmas(self._engine)
        self._session_factory = async_sessionmaker(self._engine, expire_on_commit=False)
        self._pending_writes: set[asyncio.Task[None]] = set()
        # In-memory databases run on ONE shared connection (StaticPool), where
        # two sessions interleaving BEGIN/COMMIT across await points corrupt
        # each other's transaction state (transient "cannot start a
        # transaction within a transaction" 500s). Serialize session scopes at
        # the event-loop level for that case only — file-backed WAL databases
        # get a connection per session and keep their full concurrency.
        self._session_lock: asyncio.Lock | None = (
            asyncio.Lock() if ":memory:" in self.database_url else None
        )

    async def init(self) -> None:
        """Creates all tables if they don't already exist."""
        async with self._engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    @asynccontextmanager
    async def get_db_session(self) -> AsyncIterator[AsyncSession]:
        """The one blessed way to touch the database: an explicit, task-safe
        session scope. Commits on clean exit, rolls back on any error, always
        releases the connection — every repository method below runs inside it,
        which (together with WAL mode) is what keeps concurrent live-stream
        writes and frontend analytics reads from ever colliding. In-memory
        (single-connection) databases additionally serialize scopes — see
        `_session_lock` in `__init__`."""
        if self._session_lock is not None:
            async with self._session_lock:
                async with self._session_scope() as session:
                    yield session
        else:
            async with self._session_scope() as session:
                yield session

    @asynccontextmanager
    async def _session_scope(self) -> AsyncIterator[AsyncSession]:
        async with self._session_factory() as session:
            try:
                yield session
                await session.commit()
            except BaseException:
                await session.rollback()
                raise

    async def journal_mode(self) -> str:
        """The active SQLite journal mode ('wal' for file DBs, 'memory' for
        in-memory) — surfaced on the frontend's Database Mode badge. Returns
        the driver name for non-SQLite backends."""
        if not _is_sqlite(self.database_url):
            return self._engine.dialect.name
        async with self.get_db_session() as session:
            result = await session.execute(text("PRAGMA journal_mode"))
            return str(result.scalar_one()).lower()

    async def dispose(self) -> None:
        # Let any shielded writes land before tearing the engine down.
        if self._pending_writes:
            await asyncio.gather(*list(self._pending_writes), return_exceptions=True)
        await self._engine.dispose()

    async def _shielded_write(self, coro: Coroutine[Any, Any, None]) -> None:
        """Runs a write so that cancelling the *caller* cannot interrupt it.

        Engine workers get hard-cancelled on shutdown and on watchlist switches.
        A task cancelled mid-commit severs the underlying DBAPI connection —
        and under SQLite's StaticPool (notably the in-memory databases tests
        use) the replacement connection is a brand-new empty database. Shielding
        lets an in-flight commit finish even though the worker is going away;
        `dispose()` waits for stragglers.
        """
        task = asyncio.ensure_future(coro)
        self._pending_writes.add(task)
        task.add_done_callback(self._pending_writes.discard)
        await asyncio.shield(task)

    async def record_trade(self, trade: TradeRecord) -> None:
        await self._shielded_write(self._record_trade(trade))

    async def _record_trade(self, trade: TradeRecord) -> None:
        async with self.get_db_session() as session:
            session.add(
                Trade(
                    engine_type=trade.engine_type,
                    asset_ticker=trade.asset_ticker,
                    entry_timestamp=trade.entry_timestamp,
                    exit_timestamp=trade.exit_timestamp,
                    entry_price=trade.entry_price,
                    exit_price=trade.exit_price,
                    position_size=trade.position_size,
                    fees=trade.fees,
                    net_profit=trade.net_profit,
                    requested_price=trade.requested_price,
                    actual_filled_price=trade.actual_filled_price,
                    slippage_cost=trade.slippage_cost,
                    stop_loss_price=trade.stop_loss_price,
                    take_profit_price=trade.take_profit_price,
                    bracket_status=trade.bracket_status,
                )
            )

    async def record_open_position(self, position: OpenPositionRecord) -> None:
        await self._shielded_write(self._record_open_position(position))

    async def _record_open_position(self, position: OpenPositionRecord) -> None:
        async with self.get_db_session() as session:
            session.add(
                OpenPosition(
                    engine_type=position.engine_type,
                    asset_ticker=position.asset_ticker,
                    side=position.side,
                    entry_timestamp=position.entry_timestamp,
                    entry_price=position.entry_price,
                    requested_entry_price=position.requested_entry_price,
                    position_size=position.position_size,
                    entry_fees=position.entry_fees,
                    entry_order_id=position.entry_order_id,
                )
            )

    async def clear_open_position(self, engine_type: str, asset_ticker: str) -> None:
        await self._shielded_write(self._clear_open_position(engine_type, asset_ticker))

    async def _clear_open_position(self, engine_type: str, asset_ticker: str) -> None:
        async with self.get_db_session() as session:
            await session.execute(
                delete(OpenPosition)
                .where(OpenPosition.engine_type == engine_type)
                .where(OpenPosition.asset_ticker == asset_ticker)
            )

    async def list_open_positions(self, engine_type: str | None = None) -> list[OpenPositionRecord]:
        async with self.get_db_session() as session:
            query = select(OpenPosition).order_by(OpenPosition.entry_timestamp)
            if engine_type is not None:
                query = query.where(OpenPosition.engine_type == engine_type)
            result = await session.execute(query)
            return [
                OpenPositionRecord(
                    engine_type=row.engine_type,
                    asset_ticker=row.asset_ticker,
                    side=row.side,
                    entry_timestamp=row.entry_timestamp,
                    entry_price=row.entry_price,
                    requested_entry_price=row.requested_entry_price,
                    position_size=row.position_size,
                    entry_fees=row.entry_fees,
                    entry_order_id=row.entry_order_id,
                )
                for row in result.scalars().all()
            ]

    async def total_slippage_cost(self) -> float:
        """Cumulative dollars lost to slippage across all closed trades."""
        async with self.get_db_session() as session:
            result = await session.execute(select(func.coalesce(func.sum(Trade.slippage_cost), 0.0)))
            return float(result.scalar_one())

    async def record_equity_snapshot(self, engine_type: str, timestamp: datetime, equity: float) -> None:
        await self._shielded_write(self._record_equity_snapshot(engine_type, timestamp, equity))

    async def _record_equity_snapshot(self, engine_type: str, timestamp: datetime, equity: float) -> None:
        async with self.get_db_session() as session:
            session.add(EquitySnapshot(engine_type=engine_type, timestamp=timestamp, equity=equity))

    async def get_trades(self, engine_type: str, asset_ticker: str | None = None) -> list[Trade]:
        async with self.get_db_session() as session:
            query = select(Trade).where(Trade.engine_type == engine_type).order_by(Trade.exit_timestamp)
            if asset_ticker is not None:
                query = query.where(Trade.asset_ticker == asset_ticker)
            result = await session.execute(query)
            return list(result.scalars().all())

    async def get_equity_curve(self, engine_type: str) -> list[EquitySnapshot]:
        async with self.get_db_session() as session:
            result = await session.execute(
                select(EquitySnapshot)
                .where(EquitySnapshot.engine_type == engine_type)
                .order_by(EquitySnapshot.timestamp)
            )
            return list(result.scalars().all())

    # --- Phase 7: RiskGuard state serialization --------------------------------

    async def save_system_state(self, state: dict[str, Any]) -> None:
        """Upserts the guard's daily counters (one row per calendar date).

        Shielded like every other write: a worker cancelled mid-persist cannot
        sever the connection or lose the row."""
        await self._shielded_write(self._save_system_state(state))

    async def _save_system_state(self, state: dict[str, Any]) -> None:
        async with self.get_db_session() as session:
            existing = await session.execute(
                select(SystemState).where(SystemState.state_date == str(state["state_date"]))
            )
            row = existing.scalar_one_or_none()
            if row is None:
                row = SystemState(state_date=str(state["state_date"]))
                session.add(row)
            row.daily_realized_pnl = float(state["daily_realized_pnl"])
            row.daily_entry_count = int(state["daily_entry_count"])
            row.locked_reason = str(state.get("locked_reason") or "")
            row.circuit_breaker_active = bool(state["circuit_breaker_active"])
            row.updated_at = datetime.now(timezone.utc)

    @staticmethod
    def _system_state_to_dict(row: SystemState) -> dict[str, Any]:
        return {
            "state_date": row.state_date,
            "daily_realized_pnl": row.daily_realized_pnl,
            "daily_entry_count": row.daily_entry_count,
            "locked_reason": row.locked_reason,
            "circuit_breaker_active": row.circuit_breaker_active,
            "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        }

    async def load_system_state(self, state_date: str) -> dict[str, Any] | None:
        async with self.get_db_session() as session:
            result = await session.execute(select(SystemState).where(SystemState.state_date == state_date))
            row = result.scalar_one_or_none()
            return self._system_state_to_dict(row) if row is not None else None

    async def load_latest_system_state(self) -> dict[str, Any] | None:
        """The most recent persisted guard state (boot-time crash recovery)."""
        async with self.get_db_session() as session:
            result = await session.execute(
                select(SystemState).order_by(SystemState.state_date.desc()).limit(1)
            )
            row = result.scalar_one_or_none()
            return self._system_state_to_dict(row) if row is not None else None

    async def get_latest_equity(self, engine_type: str) -> float:
        """Returns the most recent persisted equity for `engine_type`, or 0.0 if none exists.

        Used at startup so a restarted engine resumes its running equity instead
        of silently resetting to zero.
        """
        async with self.get_db_session() as session:
            result = await session.execute(
                select(EquitySnapshot.equity)
                .where(EquitySnapshot.engine_type == engine_type)
                .order_by(EquitySnapshot.timestamp.desc())
                .limit(1)
            )
            row = result.scalar_one_or_none()
            return row if row is not None else 0.0

    # --- Phase 10: copy-trading (watched traders + their trade events) ---------

    @staticmethod
    def _trader_to_dict(row: WatchedTrader) -> dict[str, Any]:
        return {
            "id": row.id,
            "name": row.name,
            "asset_class": row.asset_class,
            "notes": row.notes,
            "auto_follow": row.auto_follow,
            "budget_amount": row.budget_amount,
            "created_at": row.created_at.isoformat() if row.created_at else None,
        }

    @staticmethod
    def _event_to_dict(row: TraderTradeEvent) -> dict[str, Any]:
        return {
            "id": row.id,
            "trader_id": row.trader_id,
            "ticker": row.ticker,
            "action": row.action,
            "price": row.price,
            "source": row.source,
            "note": row.note,
            "timestamp": row.timestamp.isoformat() if row.timestamp else None,
            "followed": row.followed,
            "follow_detail": row.follow_detail,
        }

    async def add_trader(self, name: str, asset_class: str, notes: str) -> dict[str, Any]:
        async with self.get_db_session() as session:
            row = WatchedTrader(
                name=name,
                asset_class=asset_class,
                notes=notes,
                created_at=datetime.now(timezone.utc),
            )
            session.add(row)
            await session.flush()  # populate the autoincrement id before commit
            return self._trader_to_dict(row)

    async def list_traders(self) -> list[dict[str, Any]]:
        async with self.get_db_session() as session:
            result = await session.execute(select(WatchedTrader).order_by(WatchedTrader.created_at))
            return [self._trader_to_dict(row) for row in result.scalars().all()]

    async def get_trader(self, trader_id: int) -> dict[str, Any] | None:
        async with self.get_db_session() as session:
            row = await session.get(WatchedTrader, trader_id)
            return self._trader_to_dict(row) if row is not None else None

    async def update_trader_follow(
        self, trader_id: int, auto_follow: bool, budget_amount: float
    ) -> dict[str, Any] | None:
        async with self.get_db_session() as session:
            row = await session.get(WatchedTrader, trader_id)
            if row is None:
                return None
            row.auto_follow = auto_follow
            row.budget_amount = budget_amount
            return self._trader_to_dict(row)

    async def delete_trader(self, trader_id: int) -> bool:
        """Removes the trader and their event history in one transaction."""
        async with self.get_db_session() as session:
            row = await session.get(WatchedTrader, trader_id)
            if row is None:
                return False
            await session.execute(
                delete(TraderTradeEvent).where(TraderTradeEvent.trader_id == trader_id)
            )
            await session.delete(row)
            return True

    async def record_trader_event(
        self,
        trader_id: int,
        ticker: str,
        action: str,
        price: float,
        source: str,
        note: str,
        *,
        followed: bool,
        follow_detail: str,
    ) -> dict[str, Any]:
        async with self.get_db_session() as session:
            row = TraderTradeEvent(
                trader_id=trader_id,
                ticker=ticker,
                action=action,
                price=price,
                source=source,
                note=note,
                timestamp=datetime.now(timezone.utc),
                followed=followed,
                follow_detail=follow_detail,
            )
            session.add(row)
            await session.flush()
            return self._event_to_dict(row)

    async def list_trader_events(
        self, trader_id: int | None = None, limit: int = 50
    ) -> list[dict[str, Any]]:
        """Newest-first event feed, optionally scoped to one trader."""
        async with self.get_db_session() as session:
            query = select(TraderTradeEvent).order_by(TraderTradeEvent.id.desc()).limit(limit)
            if trader_id is not None:
                query = query.where(TraderTradeEvent.trader_id == trader_id)
            result = await session.execute(query)
            return [self._event_to_dict(row) for row in result.scalars().all()]
