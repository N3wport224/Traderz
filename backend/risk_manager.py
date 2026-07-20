"""Centralized risk manager: daily drawdown circuit breaker + capital allocation.

A single `RiskManager` instance is shared by both strategy engines (injected at
construction, same as `ConfigStore` and `TradePersistence`), so a loss booked by
one engine can trip a halt that also blocks the other engine's new entries and
forces its open position closed — "total net loss across both engines" is
tracked in one place.

The breaker resets automatically the first time it sees a bar timestamped on a
new calendar date (UTC) — "halt until the next calendar day" — since this
simulated system has no real wall-clock trading calendar of its own.
"""

from __future__ import annotations

from datetime import date, datetime

DEFAULT_TOTAL_CAPITAL = 100_000.0
DEFAULT_MAX_DAILY_DRAWDOWN_PCT = 0.02
DEFAULT_FEE_RATE = 0.0005  # 5 bps of notional, per trade

DEFAULT_ALLOCATION_PCT: dict[str, float] = {
    "momentum": 0.05,
    "swing": 0.15,
}


class RiskManager:
    """Tracks daily realized PnL across engines and enforces allocation/drawdown limits."""

    def __init__(
        self,
        total_capital: float = DEFAULT_TOTAL_CAPITAL,
        max_daily_drawdown_pct: float = DEFAULT_MAX_DAILY_DRAWDOWN_PCT,
        allocation_pct: dict[str, float] | None = None,
        fee_rate: float = DEFAULT_FEE_RATE,
    ) -> None:
        self.total_capital = total_capital
        self.max_daily_drawdown_pct = max_daily_drawdown_pct
        self.allocation_pct = allocation_pct or dict(DEFAULT_ALLOCATION_PCT)
        self.fee_rate = fee_rate

        self._daily_pnl: float = 0.0
        self._current_date: date | None = None
        self._halted: bool = False
        self._halted_reason: str | None = None
        self._paused: bool = False
        # Tickers whose live data stream is currently down (set by the data
        # pipeline's reconnection state machine, cleared once integrity is
        # re-verified). Engines must not evaluate bars for a broken ticker.
        self._disconnected_tickers: set[str] = set()

    def _roll_day(self, timestamp: datetime) -> None:
        today = timestamp.date()
        if self._current_date is None or today != self._current_date:
            self._current_date = today
            self._daily_pnl = 0.0
            self._halted = False
            self._halted_reason = None

    def position_size(self, engine_type: str) -> float:
        """Max notional capital `engine_type` may allocate to a single position."""
        return self.total_capital * self.allocation_pct[engine_type]

    def compute_fees(self, notional: float) -> float:
        return abs(notional) * self.fee_rate

    def can_open_position(self, timestamp: datetime, ticker: str | None = None) -> bool:
        self._roll_day(timestamp)
        if ticker is not None and ticker in self._disconnected_tickers:
            return False
        return not (self._halted or self._paused)

    def mark_data_disconnected(self, ticker: str) -> None:
        """Flag `ticker`'s stream as down: engines must stop evaluating it."""
        self._disconnected_tickers.add(ticker)

    def mark_data_verified(self, ticker: str) -> None:
        """Clear the disconnection flag once the stream's integrity is verified."""
        self._disconnected_tickers.discard(ticker)

    def is_data_disconnected(self, ticker: str | None = None) -> bool:
        if ticker is not None:
            return ticker in self._disconnected_tickers
        return bool(self._disconnected_tickers)

    @property
    def disconnected_tickers(self) -> list[str]:
        return sorted(self._disconnected_tickers)

    def is_halted(self, timestamp: datetime | None = None) -> bool:
        if timestamp is not None:
            self._roll_day(timestamp)
        return self._halted

    def record_realized_pnl(self, engine_type: str, pnl: float, timestamp: datetime) -> bool:
        """Books realized PnL against the daily drawdown budget.

        Returns True the moment this call causes the breaker to trip (so the
        caller can emit a single CIRCUIT_BREAKER_TRIGGERED signal/notification).
        """
        self._roll_day(timestamp)
        self._daily_pnl += pnl

        drawdown_threshold = -abs(self.max_daily_drawdown_pct * self.total_capital)
        if not self._halted and self._daily_pnl <= drawdown_threshold:
            self._halted = True
            self._halted_reason = f"max_daily_drawdown_exceeded (triggered by {engine_type})"
            return True
        return False

    def pause(self) -> None:
        self._paused = True

    def resume(self) -> None:
        self._paused = False

    def system_status(self, timestamp: datetime | None = None) -> str:
        if timestamp is not None:
            self._roll_day(timestamp)
        if self._halted:
            return "HALTED_BY_DRAWDOWN"
        if self._disconnected_tickers:
            return "DATA_DISCONNECTED"
        if self._paused:
            return "PAUSED"
        return "RUNNING"

    def status(self, timestamp: datetime | None = None) -> dict[str, object]:
        if timestamp is not None:
            self._roll_day(timestamp)
        daily_drawdown_pct = max(0.0, -self._daily_pnl / self.total_capital)
        return {
            "system_status": self.system_status(),
            "halted": self._halted,
            "halted_reason": self._halted_reason,
            "paused": self._paused,
            "data_disconnected": bool(self._disconnected_tickers),
            "disconnected_tickers": self.disconnected_tickers,
            "daily_pnl": round(self._daily_pnl, 4),
            "daily_drawdown_pct": round(daily_drawdown_pct, 6),
            "max_daily_drawdown_pct": self.max_daily_drawdown_pct,
            "total_capital": self.total_capital,
            "allocation_pct": dict(self.allocation_pct),
            "fee_rate": self.fee_rate,
            "current_date": self._current_date.isoformat() if self._current_date else None,
        }
