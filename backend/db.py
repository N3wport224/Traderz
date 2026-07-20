"""Async SQLAlchemy persistence layer.

Defaults to a local SQLite file for development; point `DATABASE_URL` at a
Postgres DSN (e.g. `postgresql+asyncpg://user:pass@host/db`) to scale up — no
code changes needed elsewhere, since callers only ever touch `Database`.

`Database` implements `backend.models.TradePersistence`, so it can be handed
directly to a strategy engine's constructor without the engine importing this
module (or SQLAlchemy) at all.
"""

from __future__ import annotations

import os
from datetime import datetime

from sqlalchemy import Float, String, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.pool import StaticPool

from backend.models import TradeRecord

DEFAULT_DATABASE_URL = "sqlite+aiosqlite:///./traderz.db"


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


class EquitySnapshot(Base):
    __tablename__ = "equity_snapshots"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    engine_type: Mapped[str] = mapped_column(String(32), index=True)
    timestamp: Mapped[datetime] = mapped_column()
    equity: Mapped[float] = mapped_column(Float)


def _engine_kwargs(database_url: str) -> dict[str, object]:
    if ":memory:" in database_url:
        # A single shared in-memory connection so all sessions see the same data.
        return {"poolclass": StaticPool, "connect_args": {"check_same_thread": False}}
    return {}


class Database:
    """Owns the async engine/session factory and implements `TradePersistence`."""

    def __init__(self, database_url: str | None = None) -> None:
        self.database_url = database_url or os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL)
        self._engine = create_async_engine(self.database_url, **_engine_kwargs(self.database_url))
        self._session_factory = async_sessionmaker(self._engine, expire_on_commit=False)

    async def init(self) -> None:
        """Creates all tables if they don't already exist."""
        async with self._engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def dispose(self) -> None:
        await self._engine.dispose()

    async def record_trade(self, trade: TradeRecord) -> None:
        async with self._session_factory() as session:
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
                )
            )
            await session.commit()

    async def record_equity_snapshot(self, engine_type: str, timestamp: datetime, equity: float) -> None:
        async with self._session_factory() as session:
            session.add(EquitySnapshot(engine_type=engine_type, timestamp=timestamp, equity=equity))
            await session.commit()

    async def get_trades(self, engine_type: str) -> list[Trade]:
        async with self._session_factory() as session:
            result = await session.execute(
                select(Trade).where(Trade.engine_type == engine_type).order_by(Trade.exit_timestamp)
            )
            return list(result.scalars().all())

    async def get_equity_curve(self, engine_type: str) -> list[EquitySnapshot]:
        async with self._session_factory() as session:
            result = await session.execute(
                select(EquitySnapshot)
                .where(EquitySnapshot.engine_type == engine_type)
                .order_by(EquitySnapshot.timestamp)
            )
            return list(result.scalars().all())

    async def get_latest_equity(self, engine_type: str) -> float:
        """Returns the most recent persisted equity for `engine_type`, or 0.0 if none exists.

        Used at startup so a restarted engine resumes its running equity instead
        of silently resetting to zero.
        """
        async with self._session_factory() as session:
            result = await session.execute(
                select(EquitySnapshot.equity)
                .where(EquitySnapshot.engine_type == engine_type)
                .order_by(EquitySnapshot.timestamp.desc())
                .limit(1)
            )
            row = result.scalar_one_or_none()
            return row if row is not None else 0.0
