"""Structured JSON logging and order-flow latency telemetry.

Two responsibilities:

1. `configure_json_logging()` — attaches a JSON-lines handler (default file:
   `logging.json`) to the `traderz` logger namespace so every log record across
   the app is machine-parseable: timestamp, level, logger, message, plus any
   `extra={...}` fields flattened into the line.
2. `TelemetryTracker` — implements the `OrderFlowTelemetry` protocol from
   `backend/models.py`. Engines report each order's
   signal-generation -> risk-approval -> gateway-fill latencies here; the
   tracker aggregates them (averages, last values, cumulative slippage) for
   `/api/telemetry` and emits one structured JSON log line per flow.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend.models import OrderFill

logger = logging.getLogger("traderz.telemetry")

# logging.LogRecord attributes that are bookkeeping, not user-supplied extras.
_RESERVED_RECORD_FIELDS = frozenset(
    logging.LogRecord("", 0, "", 0, "", (), None).__dict__.keys()
) | {"message", "asctime", "taskName"}


class JsonLogFormatter(logging.Formatter):
    """Formats every record as a single JSON object per line."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _RESERVED_RECORD_FIELDS and not key.startswith("_"):
                payload[key] = value
        if record.exc_info and record.exc_info[0] is not None:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure_json_logging(
    path: str | Path = "logging.json",
    level: int = logging.INFO,
    logger_name: str = "traderz",
) -> logging.Handler:
    """Attach a JSON-lines file handler to the `traderz` logger namespace.

    Idempotent per path: calling twice with the same path won't double-attach.
    Returns the handler so callers (tests, shutdown hooks) can detach it.
    """
    target = logging.getLogger(logger_name)
    target.setLevel(level)
    resolved = str(Path(path).resolve())
    for existing in target.handlers:
        if isinstance(existing, logging.FileHandler) and existing.baseFilename == resolved:
            return existing
    handler = logging.FileHandler(resolved)
    handler.setFormatter(JsonLogFormatter())
    handler.setLevel(level)
    target.addHandler(handler)
    return handler


class TelemetryTracker:
    """In-memory aggregation of order-flow latencies and slippage costs.

    Implements `backend.models.OrderFlowTelemetry`. All methods are synchronous
    and allocation-light so engines can call them inline on the hot path.
    """

    def __init__(self) -> None:
        self._flows: list[dict[str, Any]] = []
        self._total_signal_to_approval_ms = 0.0
        self._total_approval_to_fill_ms = 0.0
        self._total_gateway_latency_ms = 0.0
        self._cumulative_slippage_cost = 0.0

    def record_order_flow(
        self,
        engine_type: str,
        ticker: str,
        signal_to_approval_ms: float,
        approval_to_fill_ms: float,
        fill: OrderFill,
    ) -> None:
        flow: dict[str, Any] = {
            "engine_type": engine_type,
            "ticker": ticker,
            "order_id": fill.order_id,
            "signal_type": fill.signal_type.value,
            "signal_to_approval_ms": round(signal_to_approval_ms, 3),
            "approval_to_fill_ms": round(approval_to_fill_ms, 3),
            "signal_to_fill_ms": round(signal_to_approval_ms + approval_to_fill_ms, 3),
            "gateway_latency_ms": fill.latency_ms,
            "requested_price": fill.requested_price,
            "filled_price": fill.filled_price,
            "slippage_cost": fill.slippage_cost,
            "status": fill.status.value,
            "timestamp": fill.timestamp.isoformat(),
        }
        self._flows.append(flow)
        self._total_signal_to_approval_ms += signal_to_approval_ms
        self._total_approval_to_fill_ms += approval_to_fill_ms
        self._total_gateway_latency_ms += fill.latency_ms
        self._cumulative_slippage_cost += fill.slippage_cost
        logger.info("order_flow", extra={"event": "order_flow", **flow})

    @property
    def order_count(self) -> int:
        return len(self._flows)

    @property
    def cumulative_slippage_cost(self) -> float:
        return round(self._cumulative_slippage_cost, 6)

    @property
    def last_flow(self) -> dict[str, Any] | None:
        return dict(self._flows[-1]) if self._flows else None

    def stats(self) -> dict[str, Any]:
        """Aggregate view served by `/api/telemetry`."""
        count = len(self._flows)
        last = self._flows[-1] if self._flows else None
        return {
            "order_count": count,
            "cumulative_slippage_cost": self.cumulative_slippage_cost,
            "avg_signal_to_approval_ms": round(self._total_signal_to_approval_ms / count, 3) if count else 0.0,
            "avg_approval_to_fill_ms": round(self._total_approval_to_fill_ms / count, 3) if count else 0.0,
            "avg_gateway_latency_ms": round(self._total_gateway_latency_ms / count, 3) if count else 0.0,
            "connection_latency_ms": last["gateway_latency_ms"] if last else 0.0,
            "last_flow": dict(last) if last else None,
        }
