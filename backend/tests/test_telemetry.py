"""Tests for structured JSON logging and the order-flow latency tracker."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import pytest

from backend.models import OrderFill, OrderStatus, SignalAction
from backend.telemetry import JsonLogFormatter, TelemetryTracker, configure_json_logging


def make_fill(
    order_id: str = "mock-1",
    latency_ms: float = 4.2,
    slippage_cost: float = 3.5,
) -> OrderFill:
    return OrderFill(
        order_id=order_id,
        ticker="MOCK",
        signal_type=SignalAction.BUY,
        requested_size=5_000.0,
        filled_size=5_000.0,
        requested_price=100.0,
        filled_price=100.07,
        fees=2.5,
        slippage_cost=slippage_cost,
        status=OrderStatus.FILLED,
        latency_ms=latency_ms,
        timestamp=datetime(2026, 7, 20, 12, 0, tzinfo=timezone.utc),
    )


# --- JsonLogFormatter ---------------------------------------------------------


def test_formatter_emits_valid_json_with_core_fields() -> None:
    formatter = JsonLogFormatter()
    record = logging.LogRecord("traderz.test", logging.WARNING, __file__, 1, "stream %s dropped", ("MOCK",), None)
    payload = json.loads(formatter.format(record))
    assert payload["level"] == "WARNING"
    assert payload["logger"] == "traderz.test"
    assert payload["message"] == "stream MOCK dropped"
    assert "timestamp" in payload


def test_formatter_flattens_extra_fields() -> None:
    formatter = JsonLogFormatter()
    record = logging.LogRecord("traderz.test", logging.INFO, __file__, 1, "order_flow", (), None)
    record.__dict__["signal_to_fill_ms"] = 12.5
    record.__dict__["order_id"] = "mock-7"
    payload = json.loads(formatter.format(record))
    assert payload["signal_to_fill_ms"] == 12.5
    assert payload["order_id"] == "mock-7"


def test_configure_json_logging_writes_parseable_lines(tmp_path: Path) -> None:
    log_path = tmp_path / "logging.json"
    handler = configure_json_logging(log_path, logger_name="traderz-test-iso")
    try:
        logging.getLogger("traderz-test-iso.sub").info("hello", extra={"latency_ms": 9.1})
        handler.flush()
        lines = [json.loads(line) for line in log_path.read_text().splitlines()]
        assert lines and lines[-1]["message"] == "hello"
        assert lines[-1]["latency_ms"] == 9.1
    finally:
        logging.getLogger("traderz-test-iso").removeHandler(handler)
        handler.close()


def test_configure_json_logging_is_idempotent_per_path(tmp_path: Path) -> None:
    log_path = tmp_path / "logging.json"
    first = configure_json_logging(log_path, logger_name="traderz-test-idem")
    second = configure_json_logging(log_path, logger_name="traderz-test-idem")
    try:
        assert first is second
        assert len(logging.getLogger("traderz-test-idem").handlers) == 1
    finally:
        logging.getLogger("traderz-test-idem").removeHandler(first)
        first.close()


# --- TelemetryTracker ----------------------------------------------------------


def test_tracker_records_flow_and_aggregates() -> None:
    tracker = TelemetryTracker()
    tracker.record_order_flow("momentum", "MOCK", 0.5, 4.5, make_fill(latency_ms=4.0))
    tracker.record_order_flow("swing", "MOCK", 1.5, 6.5, make_fill("mock-2", latency_ms=6.0, slippage_cost=1.5))

    stats = tracker.stats()
    assert stats["order_count"] == 2
    assert stats["avg_signal_to_approval_ms"] == pytest.approx(1.0)
    assert stats["avg_approval_to_fill_ms"] == pytest.approx(5.5)
    assert stats["avg_gateway_latency_ms"] == pytest.approx(5.0)
    assert stats["cumulative_slippage_cost"] == pytest.approx(5.0)
    assert stats["connection_latency_ms"] == 6.0  # most recent fill's gateway latency


def test_tracker_flow_computes_end_to_end_latency() -> None:
    tracker = TelemetryTracker()
    tracker.record_order_flow("momentum", "MOCK", 2.0, 10.0, make_fill())
    last = tracker.last_flow
    assert last is not None
    assert last["signal_to_fill_ms"] == pytest.approx(12.0)
    assert last["signal_to_approval_ms"] == pytest.approx(2.0)
    assert last["approval_to_fill_ms"] == pytest.approx(10.0)


def test_empty_tracker_stats_are_zeroed() -> None:
    stats = TelemetryTracker().stats()
    assert stats["order_count"] == 0
    assert stats["cumulative_slippage_cost"] == 0.0
    assert stats["connection_latency_ms"] == 0.0
    assert stats["last_flow"] is None


def test_tracker_emits_structured_log_line(tmp_path: Path) -> None:
    """The full pipeline: a recorded flow must land in the JSON log file with
    its latency breakdown intact."""
    log_path = tmp_path / "logging.json"
    handler = configure_json_logging(log_path)  # tracker logs under "traderz.telemetry"
    try:
        TelemetryTracker().record_order_flow("momentum", "MOCK", 0.4, 3.6, make_fill())
        handler.flush()
        lines = [json.loads(line) for line in log_path.read_text().splitlines()]
        flows = [line for line in lines if line.get("event") == "order_flow"]
        assert flows
        flow = flows[-1]
        assert flow["engine_type"] == "momentum"
        assert flow["signal_to_fill_ms"] == pytest.approx(4.0)
        assert flow["requested_price"] == 100.0
        assert flow["filled_price"] == 100.07
    finally:
        logging.getLogger("traderz").removeHandler(handler)
        handler.close()
