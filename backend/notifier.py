"""Alert dispatch pipeline.

Formats BUY / SHORT / CIRCUIT_BREAKER_TRIGGERED trade signals into clean text
and fans each one out to every registered notification sink the instant it is
received from an engine's signal stream — no batching, no delay.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Protocol

import httpx

from backend.models import SignalAction, TradeSignal

logger = logging.getLogger("traderz.notifier")

NOTIFIABLE_ACTIONS = frozenset(
    {
        SignalAction.BUY,
        SignalAction.SHORT,
        SignalAction.CIRCUIT_BREAKER,
        SignalAction.DATA_DISCONNECTED,
        SignalAction.DATA_RECONNECTED,
    }
)

_ICONS: dict[SignalAction, str] = {
    SignalAction.BUY: "\U0001f7e2",
    SignalAction.SHORT: "\U0001f534",
    SignalAction.CIRCUIT_BREAKER: "\U0001f6d1",
    SignalAction.DATA_DISCONNECTED: "⚠️",
    SignalAction.DATA_RECONNECTED: "✅",
}


def format_signal(signal: TradeSignal) -> str:
    icon = _ICONS.get(signal.action, "ℹ️")
    header = f"{icon} {signal.action.value.upper()} — {signal.engine}"
    body = f"{signal.symbol} @ ${signal.price:,.2f} | {signal.reason.replace('_', ' ')}"
    return f"{header}\n{body}\n{signal.timestamp.isoformat()}"


class NotificationSink(Protocol):
    async def send(self, message: str) -> None: ...


class ConsoleNotifier:
    """Default sink: logs the alert. Always available, needs no external credentials."""

    async def send(self, message: str) -> None:
        logger.info("ALERT: %s", message.replace("\n", " | "))


class WebhookNotifier:
    """Posts a Discord-compatible `{"content": ...}` JSON payload to a webhook URL.

    Works unmodified with Discord incoming webhooks. Slack-compatible webhooks
    accept the same shape under a `text` key instead of `content`. Telegram's
    Bot API has a different auth/URL shape entirely — implement a small sibling
    class satisfying `NotificationSink` and register it alongside this one
    rather than overloading this class with multiple wire formats.
    """

    def __init__(self, webhook_url: str, client: httpx.AsyncClient | None = None) -> None:
        self.webhook_url = webhook_url
        self._client = client or httpx.AsyncClient(timeout=5.0)

    async def send(self, message: str) -> None:
        try:
            response = await self._client.post(self.webhook_url, json={"content": message})
            response.raise_for_status()
        except httpx.HTTPError:
            logger.warning("Failed to deliver webhook notification", exc_info=True)


class Notifier:
    """Dispatches notifiable signals to every registered sink."""

    def __init__(self, sinks: list[NotificationSink] | None = None) -> None:
        self.sinks: list[NotificationSink] = sinks if sinks is not None else [ConsoleNotifier()]

    async def notify_signal(self, signal: TradeSignal) -> None:
        if signal.action not in NOTIFIABLE_ACTIONS:
            return
        message = format_signal(signal)
        for sink in self.sinks:
            await sink.send(message)

    async def notify_data_event(
        self,
        action: SignalAction,
        ticker: str,
        timestamp: datetime,
        reason: str,
    ) -> None:
        """Dispatch a DATA_DISCONNECTED / DATA_RECONNECTED alert for `ticker`.

        Used by the data pipeline's reconnection state machine, which has no
        TradeSignal of its own — this synthesizes one so data-integrity events
        flow through the exact same sink fan-out as trade alerts.
        """
        await self.notify_signal(
            TradeSignal(
                engine="data_pipeline",
                symbol=ticker,
                action=action,
                price=0.0,
                timestamp=timestamp,
                reason=reason,
            )
        )
