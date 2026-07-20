"""Tests for the production SystemNotifier: webhook dispatch, log-only mode,
failure tolerance, history, and the connectivity test ping."""

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest

from backend.utils.notifier import SystemNotifier


def capture_transport(status_code: int = 204) -> tuple[httpx.AsyncClient, list[dict[str, Any]]]:
    received: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        received.append({"url": str(request.url), "body": json.loads(request.content.decode())})
        return httpx.Response(status_code)

    return httpx.AsyncClient(transport=httpx.MockTransport(handler)), received


def make_notifier(status_code: int = 204) -> tuple[SystemNotifier, list[dict[str, Any]]]:
    client, received = capture_transport(status_code)
    notifier = SystemNotifier("https://hooks.example.test/system", client=client)
    return notifier, received


@pytest.mark.asyncio
async def test_alert_delivers_discord_compatible_payload() -> None:
    notifier, received = make_notifier()
    delivered = await notifier.alert(
        "Risk guard tripped", "Daily loss limit exceeded", reason="max_daily_loss", ticker="MOCK"
    )

    assert delivered is True
    assert len(received) == 1
    content = received[0]["body"]["content"]
    assert "ALERT" in content
    assert "Risk guard tripped" in content
    assert "max_daily_loss" in content  # context lines included
    assert notifier.delivered_count == 1


@pytest.mark.asyncio
async def test_info_level_uses_info_formatting() -> None:
    notifier, received = make_notifier()
    assert await notifier.info("Engine online", "Momentum engine initialized", ticker="BTC/USDT") is True
    assert "INFO: Engine online" in received[0]["body"]["content"]


@pytest.mark.asyncio
async def test_unconfigured_notifier_runs_log_only(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SYSTEM_WEBHOOK_URL", raising=False)
    notifier = SystemNotifier()
    assert notifier.configured is False
    delivered = await notifier.alert("Something", "happened")
    assert delivered is False  # nothing left the machine
    assert len(notifier.history) == 1  # ...but the event is still recorded
    assert notifier.history[0]["delivered"] is False


@pytest.mark.asyncio
async def test_webhook_failure_never_raises(caplog: pytest.LogCaptureFixture) -> None:
    notifier, received = make_notifier(status_code=500)
    delivered = await notifier.alert("Guard trip", "critical")
    assert delivered is False  # swallowed, logged, counted
    assert notifier.failed_count == 1
    assert len(received) == 1  # the attempt was made


@pytest.mark.asyncio
async def test_network_error_never_raises() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    notifier = SystemNotifier("https://hooks.example.test/system", client=client)
    assert await notifier.alert("Guard trip", "critical") is False
    assert notifier.failed_count == 1


@pytest.mark.asyncio
async def test_env_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SYSTEM_WEBHOOK_URL", "https://hooks.example.test/from-env")
    notifier = SystemNotifier(client=httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(204))))
    assert notifier.configured is True
    assert notifier.webhook_url == "https://hooks.example.test/from-env"


@pytest.mark.asyncio
async def test_test_ping_reports_routing_state() -> None:
    notifier, received = make_notifier()
    result = await notifier.test_ping()
    assert result == {"configured": True, "delivered": True, "detail": "webhook accepted the test ping"}
    assert "Connectivity test" in received[0]["body"]["content"]

    broken, _ = make_notifier(status_code=503)
    result = await broken.test_ping()
    assert result["configured"] is True and result["delivered"] is False

    unconfigured = SystemNotifier(webhook_url=None)
    unconfigured.webhook_url = None  # ensure no env leakage
    result = await unconfigured.test_ping()
    assert result["configured"] is False and "log-only" in result["detail"]


@pytest.mark.asyncio
async def test_history_is_bounded_and_status_summarizes() -> None:
    notifier = SystemNotifier(webhook_url=None)
    notifier.webhook_url = None
    for i in range(150):
        await notifier.info("tick", f"event {i}")
    assert len(notifier.history) == 100  # bounded
    status = notifier.status()
    assert status["configured"] is False
    assert len(status["recent_events"]) == 10
    assert status["recent_events"][-1]["message"] == "event 149"
