"""Global operational risk guard — the financial seatbelt.

A `RiskGuard` sits directly in front of the execution path inside
`BaseExecutionGateway`: every ENTRY order must pass it before it can fill
(exits are always allowed — flattening must never be blocked). It enforces
three env-configurable limits:

- ``MAX_DAILY_LOSS_PCT``    — if realized PnL booked within one calendar day
  drops below ``-pct * total_capital``, the guard hard-locks: open positions
  are flattened (via the ``on_trip`` callback, wired to the shared
  `RiskManager`'s halt so engines force-close on their next bar) and every
  subsequent entry is rejected.
- ``MAX_DAILY_TRADE_COUNT`` — hard cap on entries per calendar day, guarding
  against runaway-loop bugs and over-trading.
- ``CIRCUIT_BREAKER_ACTIVE``— a manual master switch (env default, flippable
  at runtime via the kill-switch endpoint) that halts everything.

Daily counters reset when a new calendar date is observed on the bar clock;
a tripped daily-loss/trade-count lock clears with the new day, but the manual
circuit breaker stays engaged until explicitly reset by an operator.
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Awaitable, Callable
from datetime import date, datetime
from typing import Any

logger = logging.getLogger("traderz.risk_guard")

DEFAULT_MAX_DAILY_LOSS_PCT = 0.03
DEFAULT_MAX_DAILY_TRADE_COUNT = 50


class RiskGuardTripped(Exception):
    """Raised by the gateway when an entry order is rejected by the guard."""


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


class RiskGuard:
    """Immutable-limit, per-day operational guard over the execution path."""

    def __init__(
        self,
        total_capital: float,
        *,
        max_daily_loss_pct: float = DEFAULT_MAX_DAILY_LOSS_PCT,
        max_daily_trade_count: int = DEFAULT_MAX_DAILY_TRADE_COUNT,
        circuit_breaker_active: bool = False,
        on_trip: Callable[[str], None] | None = None,
    ) -> None:
        if total_capital <= 0:
            raise ValueError("total_capital must be positive")
        if max_daily_loss_pct <= 0:
            raise ValueError("max_daily_loss_pct must be positive (e.g. 0.03 for 3%)")
        if max_daily_trade_count < 1:
            raise ValueError("max_daily_trade_count must be >= 1")
        self.total_capital = total_capital
        self.max_daily_loss_pct = max_daily_loss_pct
        self.max_daily_trade_count = max_daily_trade_count
        self.circuit_breaker_active = circuit_breaker_active
        self.on_trip = on_trip
        # Phase 7 crash-safety: when set, every state mutation schedules an
        # async persist of the snapshot (wired to Database.save_system_state).
        self.persist_hook: Callable[[dict[str, Any]], Awaitable[None]] | None = None
        self._pending_persists: set[asyncio.Task[None]] = set()
        self._persist_tail: asyncio.Task[None] | None = None  # ordering chain

        self._current_date: date | None = None
        self._daily_realized_pnl = 0.0
        self._daily_entry_count = 0
        self._locked_reason: str | None = None

    @classmethod
    def from_env(
        cls,
        total_capital: float,
        *,
        on_trip: Callable[[str], None] | None = None,
    ) -> RiskGuard:
        """Builds a guard from MAX_DAILY_LOSS_PCT / MAX_DAILY_TRADE_COUNT /
        CIRCUIT_BREAKER_ACTIVE environment variables (safe defaults)."""
        return cls(
            total_capital,
            max_daily_loss_pct=float(os.environ.get("MAX_DAILY_LOSS_PCT", DEFAULT_MAX_DAILY_LOSS_PCT)),
            max_daily_trade_count=int(os.environ.get("MAX_DAILY_TRADE_COUNT", DEFAULT_MAX_DAILY_TRADE_COUNT)),
            circuit_breaker_active=_env_flag("CIRCUIT_BREAKER_ACTIVE"),
            on_trip=on_trip,
        )

    # --- state serialization (Phase 7 crash recovery) --------------------------

    def snapshot(self) -> dict[str, Any]:
        """The guard's complete persistable state, one calendar day's worth."""
        return {
            "state_date": self._current_date.isoformat() if self._current_date else None,
            "daily_realized_pnl": round(self._daily_realized_pnl, 4),
            "daily_entry_count": self._daily_entry_count,
            "locked_reason": self._locked_reason or "",
            "circuit_breaker_active": self.circuit_breaker_active,
        }

    def restore(self, snapshot: dict[str, Any]) -> bool:
        """Boot-time reconstruction from a persisted snapshot.

        Returns True when the state was adopted. A snapshot from an *earlier*
        calendar day only restores the manual circuit breaker (which survives
        day rolls) — its daily counters are stale and stay discarded once the
        bar clock rolls forward anyway, so adopting them is still safe and
        keeps mid-day crash recovery exact.
        """
        raw_date = snapshot.get("state_date")
        if not raw_date:
            return False
        self._current_date = date.fromisoformat(str(raw_date))
        self._daily_realized_pnl = float(snapshot.get("daily_realized_pnl", 0.0))
        self._daily_entry_count = int(snapshot.get("daily_entry_count", 0))
        self._locked_reason = str(snapshot.get("locked_reason") or "") or None
        # env=true must not be overridden to false by an old row; either side locks
        self.circuit_breaker_active = self.circuit_breaker_active or bool(
            snapshot.get("circuit_breaker_active", False)
        )
        logger.info(
            "risk guard state restored from persistence",
            extra={
                "event": "risk_guard_restored",
                "state_date": raw_date,
                "daily_realized_pnl": self._daily_realized_pnl,
                "daily_entry_count": self._daily_entry_count,
                "locked": self.locked,
            },
        )
        return True

    def _schedule_persist(self) -> None:
        """Fire-and-track an async persist of the current snapshot.

        Called on every mutation. Synchronous by design (the guard sits on the
        order hot path); outside an event loop (pure-sync unit tests) it is a
        no-op — persistence is an app-level concern wired by the composition
        root, which always runs inside the loop."""
        if self.persist_hook is None:
            return
        snapshot = self.snapshot()
        if snapshot["state_date"] is None:
            return  # no bar clock yet — nothing meaningful to serialize
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return

        # Persists are CHAINED, not raced: two concurrent sessions upserting
        # the same row each flush only their *changed* fields, so interleaved
        # commits can merge into a row matching no real snapshot. Each persist
        # awaits its predecessor, guaranteeing the final row is the newest
        # snapshot exactly.
        hook = self.persist_hook
        previous = self._persist_tail

        async def _persist_in_order() -> None:
            if previous is not None:
                try:
                    await asyncio.shield(previous)
                except Exception:  # a failed earlier persist must not wedge the chain
                    pass
            await hook(snapshot)

        task = loop.create_task(_persist_in_order())
        self._persist_tail = task
        self._pending_persists.add(task)
        task.add_done_callback(self._pending_persists.discard)

    async def flush_persists(self) -> None:
        """Awaits every in-flight persist (tests and orderly shutdown)."""
        if self._pending_persists:
            await asyncio.gather(*list(self._pending_persists), return_exceptions=True)

    # --- clock / state ---------------------------------------------------------

    def roll_day(self, timestamp: datetime) -> None:
        """Resets the daily counters when the clock crosses into a NEW calendar
        date. Strictly forward-only: an out-of-order or earlier timestamp (e.g.
        historical bars replayed alongside wall-clock fill stamps) must never
        reset the day's loss budget or clear a lock — a safety device only
        relaxes when the day genuinely advances.

        A daily-limit lock clears with the new day; the manual circuit breaker
        does not."""
        today = timestamp.date()
        if self._current_date is None:  # first observation adopts the date as-is
            self._current_date = today
            self._schedule_persist()
            return
        if today > self._current_date:
            self._current_date = today
            self._daily_realized_pnl = 0.0
            self._daily_entry_count = 0
            if self._locked_reason is not None and not self.circuit_breaker_active:
                logger.info("risk guard daily lock cleared on new calendar day %s", today)
                self._locked_reason = None
            self._schedule_persist()

    @property
    def locked(self) -> bool:
        return self.circuit_breaker_active or self._locked_reason is not None

    @property
    def locked_reason(self) -> str | None:
        if self.circuit_breaker_active:
            return "circuit_breaker_forced"
        return self._locked_reason

    def _trip(self, reason: str) -> None:
        if self._locked_reason is not None:
            return
        self._locked_reason = reason
        logger.warning(
            "RISK GUARD TRIPPED: %s (daily pnl %.2f, entries %d)",
            reason,
            self._daily_realized_pnl,
            self._daily_entry_count,
            extra={
                "event": "risk_guard_tripped",
                "reason": reason,
                "daily_realized_pnl": round(self._daily_realized_pnl, 4),
                "daily_entry_count": self._daily_entry_count,
            },
        )
        if self.on_trip is not None:
            self.on_trip(reason)
        self._schedule_persist()

    # --- hooks called by the execution gateway ---------------------------------

    def validate_entry(self, timestamp: datetime | None = None) -> None:
        """Gate in front of every ENTRY order. Raises `RiskGuardTripped` when
        the bot is locked or the entry would exceed the daily trade cap."""
        if timestamp is not None:
            self.roll_day(timestamp)
        if self.circuit_breaker_active:
            raise RiskGuardTripped("circuit_breaker_forced")
        if self._locked_reason is not None:
            raise RiskGuardTripped(self._locked_reason)
        if self._daily_entry_count >= self.max_daily_trade_count:
            self._trip(f"max_daily_trade_count_{self.max_daily_trade_count}_reached")
            raise RiskGuardTripped(self._locked_reason or "max_daily_trade_count_reached")

    def register_entry(self, timestamp: datetime | None = None) -> None:
        """Books a filled entry against the daily trade budget."""
        if timestamp is not None:
            self.roll_day(timestamp)
        self._daily_entry_count += 1
        self._schedule_persist()

    def record_realized_pnl(self, pnl: float, timestamp: datetime | None = None) -> None:
        """Books a completed round-trip's realized PnL against the daily loss
        budget; trips the hard lock the moment the threshold is crossed."""
        if timestamp is not None:
            self.roll_day(timestamp)
        self._daily_realized_pnl += pnl
        threshold = -self.max_daily_loss_pct * self.total_capital
        if self._daily_realized_pnl <= threshold:
            self._trip(
                f"max_daily_loss_{self.max_daily_loss_pct:.2%}_exceeded"
                f" (realized {self._daily_realized_pnl:.2f} <= {threshold:.2f})"
            )
        else:
            self._schedule_persist()

    # --- operator controls -----------------------------------------------------

    def force_circuit_breaker(self, active: bool) -> None:
        """The manual master switch (kill-switch endpoint / env flag)."""
        self.circuit_breaker_active = active
        if active:
            logger.warning("circuit breaker FORCED ACTIVE — all entries halted")
            if self.on_trip is not None:
                self.on_trip("circuit_breaker_forced")
        self._schedule_persist()

    def reset(self) -> None:
        """Operator reset: clears the lock and daily counters, releases the
        manual breaker. Deliberate action only — never called automatically."""
        self.circuit_breaker_active = False
        self._locked_reason = None
        self._daily_realized_pnl = 0.0
        self._daily_entry_count = 0
        logger.info("risk guard reset by operator")
        self._schedule_persist()

    def status(self) -> dict[str, Any]:
        return {
            "locked": self.locked,
            "locked_reason": self.locked_reason,
            "circuit_breaker_active": self.circuit_breaker_active,
            "daily_realized_pnl": round(self._daily_realized_pnl, 4),
            "daily_entry_count": self._daily_entry_count,
            "max_daily_loss_pct": self.max_daily_loss_pct,
            "max_daily_trade_count": self.max_daily_trade_count,
            "current_date": self._current_date.isoformat() if self._current_date else None,
        }
