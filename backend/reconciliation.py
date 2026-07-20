"""Boot-time order/position reconciliation (crash recovery).

If the backend dies between a gateway fill and the matching database write (or
between a broker-side close and clearing the local record), the two sides
disagree about what is open. On every boot `reconcile_on_boot` queries the
execution gateway's open orders, diffs them against the `open_positions` table
by `entry_order_id`, and self-heals:

- Broker order with no database row  -> the entry filled but the write was
  lost: reconstruct an `OpenPositionRecord` from the fill and insert it.
- Database row with no broker order  -> the position was closed at the broker
  but the close never landed locally: clear the stale row (the round-trip PnL
  is unknowable without the exit fill, so it is logged loudly rather than
  fabricated).
- Matched on both sides -> nothing to do.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from backend.models import (
    ExecutionGateway,
    OpenPositionRecord,
    OrderFill,
    SignalAction,
    TradePersistence,
)

logger = logging.getLogger("traderz.reconciliation")


@dataclass(slots=True)
class ReconciliationReport:
    """Outcome of one boot-time reconciliation pass."""

    matched: list[str] = field(default_factory=list)  # entry_order_ids intact on both sides
    healed: list[OpenPositionRecord] = field(default_factory=list)  # inserted from gateway
    cleared: list[OpenPositionRecord] = field(default_factory=list)  # stale rows removed

    @property
    def discrepancies(self) -> int:
        return len(self.healed) + len(self.cleared)

    def as_dict(self) -> dict[str, Any]:
        return {
            "matched": list(self.matched),
            "healed": [p.entry_order_id for p in self.healed],
            "cleared": [p.entry_order_id for p in self.cleared],
            "discrepancies": self.discrepancies,
        }


def _position_from_fill(fill: OrderFill, engine_type: str) -> OpenPositionRecord:
    side = "long" if fill.signal_type is SignalAction.BUY else "short"
    return OpenPositionRecord(
        engine_type=engine_type,
        asset_ticker=fill.ticker,
        side=side,
        entry_timestamp=fill.timestamp,
        entry_price=fill.filled_price,
        requested_entry_price=fill.requested_price,
        position_size=fill.filled_size,
        entry_fees=fill.fees,
        entry_order_id=fill.order_id,
    )


async def reconcile_on_boot(
    gateway: ExecutionGateway,
    persistence: TradePersistence,
    *,
    default_engine_type: str = "recovered",
) -> ReconciliationReport:
    """Diff gateway open orders against persisted open positions and self-heal.

    `default_engine_type` labels healed rows whose owning engine is unknowable
    from the broker side alone, so operators can spot recovered positions.
    """
    report = ReconciliationReport()
    gateway_orders = {order.order_id: order for order in await gateway.fetch_open_orders()}
    db_positions = {pos.entry_order_id: pos for pos in await persistence.list_open_positions()}

    for order_id, order in gateway_orders.items():
        if order_id in db_positions:
            report.matched.append(order_id)
            continue
        healed = _position_from_fill(order, default_engine_type)
        await persistence.record_open_position(healed)
        report.healed.append(healed)
        logger.warning(
            "reconciliation healed missing open position",
            extra={"event": "reconciliation_heal", "order_id": order_id, "ticker": order.ticker},
        )

    for order_id, position in db_positions.items():
        if order_id in gateway_orders:
            continue
        await persistence.clear_open_position(position.engine_type, position.asset_ticker)
        report.cleared.append(position)
        logger.warning(
            "reconciliation cleared stale open position (closed at broker, exit never recorded)",
            extra={
                "event": "reconciliation_clear",
                "order_id": order_id,
                "engine_type": position.engine_type,
                "ticker": position.asset_ticker,
            },
        )

    logger.info(
        "boot reconciliation complete",
        extra={"event": "reconciliation_complete", **report.as_dict()},
    )
    return report
