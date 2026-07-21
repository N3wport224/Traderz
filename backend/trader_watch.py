"""Phase 10 — Copy-Trading service ("Trader Watch").

Tracks other people's stock/crypto trades and reacts the moment an event is
logged. Events arrive from two sources:

- **manual**: the user logs "trader X bought AAPL at 231.20" in the dashboard;
- **webhook**: an external integration POSTs the same shape to the events
  endpoint (the door future automated feeds — on-chain wallet watchers,
  disclosure scrapers — plug into).

For every event the service:

1. persists it to the `trader_trade_events` table,
2. pushes a notification through the `SystemNotifier` (dashboard feed always;
   Discord/Slack webhook when configured),
3. and, when the trader's **auto-follow** toggle is on with a positive budget,
   mirrors the trade immediately through the injected `ExecutionGateway` with
   `budget_amount` notional — flowing through the exact same RiskGuard
   enforcement, slippage model, and paper-trading posture as the engines. A
   locked guard (or any gateway rejection) downgrades the follow to a recorded,
   explained skip — copy-trading must never bypass the platform's seatbelts.

The service keeps its open copied positions in an in-memory book keyed by
(trader, ticker): a SELL event only routes an exit order if we actually hold a
copied position from that trader, sized to the original fill. (The book is
process-local; positions opened before a restart age out of mirroring and are
visible in the event history. Engine-side reconciliation still sees every
gateway order.)
"""

from __future__ import annotations

import logging
from typing import Any

from backend.models import ExecutionGateway, OrderFill, SignalAction
from backend.utils.notifier import SystemNotifier
from backend.utils.risk_guard import RiskGuardTripped

logger = logging.getLogger("traderz.trader_watch")


class TraderWatchService:
    """Coordinates event persistence, notifications, and auto-follow orders."""

    def __init__(
        self,
        database: Any,  # Database (duck-typed to keep this module import-light)
        gateway: ExecutionGateway,
        system_notifier: SystemNotifier,
    ) -> None:
        self._db = database
        self._gateway = gateway
        self._notifier = system_notifier
        # Open copied positions: (trader_id, ticker) -> entry fill.
        self._open_follows: dict[tuple[int, str], OrderFill] = {}

    # --- event intake -----------------------------------------------------------

    async def log_event(
        self,
        trader: dict[str, Any],
        ticker: str,
        action: str,
        price: float,
        source: str,
        note: str,
    ) -> dict[str, Any]:
        """Records one observed trade, notifies, and (optionally) mirrors it."""
        followed = False
        follow_detail = ""

        if trader["auto_follow"] and trader["budget_amount"] > 0:
            followed, follow_detail = await self._follow(trader, ticker, action, price)
        else:
            follow_detail = "notification only (auto-follow off)"

        event = await self._db.record_trader_event(
            int(trader["id"]),
            ticker,
            action,
            price,
            source,
            note,
            followed=followed,
            follow_detail=follow_detail,
        )

        # Notify AFTER persisting so the alert always refers to a stored event.
        # ALERT level when we moved (paper) money, INFO for watch-only events.
        title = f"Watched trader {trader['name']}: {action} {ticker}"
        message = f"{action} {ticker} @ {price:g} — {follow_detail}"
        if followed:
            await self._notifier.alert(title, message, source=source, budget=trader["budget_amount"])
        else:
            await self._notifier.info(title, message, source=source)
        return event

    # --- auto-follow ------------------------------------------------------------

    async def _follow(
        self, trader: dict[str, Any], ticker: str, action: str, price: float
    ) -> tuple[bool, str]:
        """Mirrors one event through the gateway. Never raises: every failure
        path returns (False, reason) so the event is still recorded/notified."""
        trader_id = int(trader["id"])
        budget = float(trader["budget_amount"])
        key = (trader_id, ticker)

        try:
            if action == "BUY":
                if key in self._open_follows:
                    return False, "skipped: already holding a copied position in this ticker"
                fill = await self._gateway.execute_order(SignalAction.BUY, budget, ticker, price)
                self._open_follows[key] = fill
                return True, (
                    f"auto-followed: bought ${fill.filled_size:,.2f} @ {fill.filled_price:g} "
                    f"(order {fill.order_id})"
                )

            # SELL: only exit what we actually copied from this trader.
            open_fill = self._open_follows.get(key)
            if open_fill is None:
                return False, "skipped: no copied position to sell for this trader"
            fill = await self._gateway.execute_order(
                SignalAction.SELL, open_fill.filled_size, ticker, price, is_exit=True
            )
            del self._open_follows[key]
            pct = (fill.filled_price - open_fill.filled_price) / open_fill.filled_price * 100.0
            return True, (
                f"auto-followed: sold @ {fill.filled_price:g} "
                f"({pct:+.2f}% vs copied entry, order {fill.order_id})"
            )
        except RiskGuardTripped as exc:
            return False, f"blocked by risk guard: {exc}"
        except Exception as exc:  # gateway rejection — record, never crash intake
            logger.warning("auto-follow failed for trader %s: %s", trader["name"], exc)
            return False, f"follow failed: {exc}"

    # --- introspection ----------------------------------------------------------

    def open_follow_count(self) -> int:
        return len(self._open_follows)

    def status(self) -> dict[str, Any]:
        return {
            "open_copied_positions": [
                {"trader_id": tid, "ticker": ticker, "size": fill.filled_size, "entry": fill.filled_price}
                for (tid, ticker), fill in self._open_follows.items()
            ],
        }
