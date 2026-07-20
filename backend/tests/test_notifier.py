"""Verification tests for backend/notifier.py: alert formatting and dispatch."""

from __future__ import annotations

from datetime import datetime, timezone

import httpx
import pytest

from backend.models import SignalAction, TradeSignal
from backend.notifier import ConsoleNotifier, Notifier, WebhookNotifier, format_signal

BASE_TIME = datetime(2026, 7, 20, 14, 23, 0, tzinfo=timezone.utc)


def make_signal(action: SignalAction, reason: str = "orb_breakout_above_high") -> TradeSignal:
    return TradeSignal(
        engine="momentum_engine",
        symbol="MOCK",
        action=action,
        price=106.5,
        timestamp=BASE_TIME,
        reason=reason,
    )


class RecordingSink:
    def __init__(self) -> None:
        self.messages: list[str] = []

    async def send(self, message: str) -> None:
        self.messages.append(message)


# --- Formatting --------------------------------------------------------------


def test_format_signal_includes_action_engine_symbol_price_reason() -> None:
    text = format_signal(make_signal(SignalAction.BUY, "orb_breakout_above_high"))
    assert "BUY" in text
    assert "momentum_engine" in text
    assert "MOCK" in text
    assert "106.5" in text
    assert "orb breakout above high" in text  # underscores humanized


def test_format_signal_includes_timestamp() -> None:
    text = format_signal(make_signal(SignalAction.SHORT))
    assert BASE_TIME.isoformat() in text


# --- Dispatch filtering --------------------------------------------------------


@pytest.mark.asyncio
async def test_notifier_dispatches_buy_signals() -> None:
    sink = RecordingSink()
    notifier = Notifier([sink])
    await notifier.notify_signal(make_signal(SignalAction.BUY))
    assert len(sink.messages) == 1


@pytest.mark.asyncio
async def test_notifier_dispatches_short_signals() -> None:
    sink = RecordingSink()
    notifier = Notifier([sink])
    await notifier.notify_signal(make_signal(SignalAction.SHORT))
    assert len(sink.messages) == 1


@pytest.mark.asyncio
async def test_notifier_dispatches_circuit_breaker_signals() -> None:
    sink = RecordingSink()
    notifier = Notifier([sink])
    await notifier.notify_signal(make_signal(SignalAction.CIRCUIT_BREAKER, "max_daily_drawdown_exceeded"))
    assert len(sink.messages) == 1


@pytest.mark.asyncio
async def test_notifier_does_not_dispatch_exit_or_alert_signals() -> None:
    sink = RecordingSink()
    notifier = Notifier([sink])
    await notifier.notify_signal(make_signal(SignalAction.EXIT))
    await notifier.notify_signal(make_signal(SignalAction.ALERT))
    assert sink.messages == []


@pytest.mark.asyncio
async def test_notifier_fans_out_to_multiple_sinks() -> None:
    sink_a, sink_b = RecordingSink(), RecordingSink()
    notifier = Notifier([sink_a, sink_b])
    await notifier.notify_signal(make_signal(SignalAction.BUY))
    assert len(sink_a.messages) == 1
    assert len(sink_b.messages) == 1


@pytest.mark.asyncio
async def test_default_notifier_uses_console_sink() -> None:
    notifier = Notifier()
    assert len(notifier.sinks) == 1
    assert isinstance(notifier.sinks[0], ConsoleNotifier)


# --- ConsoleNotifier -----------------------------------------------------------


@pytest.mark.asyncio
async def test_console_notifier_does_not_raise() -> None:
    await ConsoleNotifier().send("hello")


# --- WebhookNotifier -----------------------------------------------------------


@pytest.mark.asyncio
async def test_webhook_notifier_posts_content_json() -> None:
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, json={"ok": True})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    sink = WebhookNotifier("https://example.invalid/webhook", client=client)
    await sink.send("test message")

    assert len(captured) == 1
    assert captured[0].url == "https://example.invalid/webhook"
    import json

    body = json.loads(captured[0].content)
    assert body == {"content": "test message"}
    await client.aclose()


@pytest.mark.asyncio
async def test_webhook_notifier_swallows_http_errors() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    sink = WebhookNotifier("https://example.invalid/webhook", client=client)
    await sink.send("should not raise")  # must not propagate the HTTP error
    await client.aclose()


# --- Phase 3: data-integrity events -------------------------------------------


@pytest.mark.asyncio
async def test_notifier_dispatches_data_disconnected_and_reconnected() -> None:
    sink = RecordingSink()
    notifier = Notifier([sink])
    await notifier.notify_data_event(
        SignalAction.DATA_DISCONNECTED, "MOCK", BASE_TIME, "stream_disconnected: boom"
    )
    await notifier.notify_data_event(
        SignalAction.DATA_RECONNECTED, "MOCK", BASE_TIME, "stream_verified_after_1_disconnects"
    )

    assert len(sink.messages) == 2
    assert "DATA_DISCONNECTED" in sink.messages[0]
    assert "MOCK" in sink.messages[0]
    assert "stream disconnected: boom" in sink.messages[0]  # reason with underscores prettified
    assert "DATA_RECONNECTED" in sink.messages[1]


@pytest.mark.asyncio
async def test_data_event_signals_pass_the_notifiable_filter() -> None:
    """DATA_* actions are in NOTIFIABLE_ACTIONS — a plain notify_signal with a
    synthesized data signal must not be silently dropped like EXIT/ALERT are."""
    sink = RecordingSink()
    notifier = Notifier([sink])
    await notifier.notify_signal(make_signal(SignalAction.DATA_DISCONNECTED, "stream_disconnected"))
    assert len(sink.messages) == 1
