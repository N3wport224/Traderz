"""Production system notifier — out-of-browser operational alerting.

Distinct from `backend/notifier.py` (which fans out per-trade signals): this is
the *system health* channel. A single async `SystemNotifier` pushes ALERT/INFO
events to an outbound webhook (Discord or Slack incoming-webhook compatible,
configured via ``SYSTEM_WEBHOOK_URL``) so operators hear about guard trips,
kill-switch presses, and boot events without watching the dashboard.

Delivery is deliberately best-effort and never raises: a dead webhook must not
take down the trading loop it is reporting on. Every event also lands in the
structured JSON log and in a bounded in-memory history that the API exposes.
"""

from __future__ import annotations

import logging
import os
from collections import deque
from datetime import datetime, timezone
from typing import Any

import httpx

logger = logging.getLogger("traderz.system_notifier")

MAX_HISTORY = 100

_LEVEL_ICONS = {
    "ALERT": "\U0001f6a8",  # rotating light
    "INFO": "\U00002705",  # check mark
}


class SystemNotifier:
    """Async multi-purpose system notifier with webhook dispatch.

    `webhook_url=None` (and no ``SYSTEM_WEBHOOK_URL``) runs in log-only mode:
    events are recorded and logged but nothing leaves the machine — the safe
    default for development and tests.
    """

    def __init__(
        self,
        webhook_url: str | None = None,
        *,
        client: httpx.AsyncClient | None = None,
        source: str = "traderz",
        timeout_seconds: float = 5.0,
    ) -> None:
        self.webhook_url = webhook_url if webhook_url is not None else os.environ.get("SYSTEM_WEBHOOK_URL") or None
        self.source = source
        self._client = client or httpx.AsyncClient(timeout=timeout_seconds)
        self.history: deque[dict[str, Any]] = deque(maxlen=MAX_HISTORY)
        self.delivered_count = 0
        self.failed_count = 0

    @property
    def configured(self) -> bool:
        return bool(self.webhook_url)

    def _format(self, level: str, title: str, message: str, context: dict[str, Any]) -> str:
        icon = _LEVEL_ICONS.get(level, "\U00002139\U0000fe0f")
        lines = [f"{icon} **[{self.source}] {level}: {title}**", message]
        for key, value in context.items():
            lines.append(f"- {key}: {value}")
        return "\n".join(lines)

    async def send(self, level: str, title: str, message: str, **context: Any) -> bool:
        """Records + logs the event and dispatches it to the webhook if one is
        configured. Returns True only when the webhook accepted the payload.
        Never raises — alerting failures are logged, not propagated."""
        event: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": level,
            "title": title,
            "message": message,
            "context": dict(context),
            "delivered": False,
        }
        log_fn = logger.warning if level == "ALERT" else logger.info
        log_fn(
            "system notification: %s — %s",
            title,
            message,
            extra={"event": "system_notification", "level": level, "title": title, **context},
        )

        delivered = False
        if self.configured and self.webhook_url:
            try:
                response = await self._client.post(
                    self.webhook_url, json={"content": self._format(level, title, message, dict(context))}
                )
                response.raise_for_status()
                delivered = True
                self.delivered_count += 1
            except httpx.HTTPError as exc:
                self.failed_count += 1
                logger.warning("system notification delivery failed: %s", exc)
        event["delivered"] = delivered
        self.history.append(event)
        return delivered

    async def alert(self, title: str, message: str, **context: Any) -> bool:
        """Critical operator-attention event (guard trip, kill switch, ...)."""
        return await self.send("ALERT", title, message, **context)

    async def info(self, title: str, message: str, **context: Any) -> bool:
        """Informational heartbeat (engine online, state restored, ...)."""
        return await self.send("INFO", title, message, **context)

    async def test_ping(self) -> dict[str, Any]:
        """Connectivity check for the dashboard's 'test webhook' button."""
        if not self.configured:
            return {
                "configured": False,
                "delivered": False,
                "detail": "no SYSTEM_WEBHOOK_URL configured — running in log-only mode",
            }
        delivered = await self.send(
            "INFO",
            "Connectivity test",
            "Test ping from the Traderz dashboard — endpoint routing verified.",
        )
        return {
            "configured": True,
            "delivered": delivered,
            "detail": "webhook accepted the test ping" if delivered else "webhook rejected or unreachable",
        }

    def status(self) -> dict[str, Any]:
        return {
            "configured": self.configured,
            "delivered_count": self.delivered_count,
            "failed_count": self.failed_count,
            "recent_events": list(self.history)[-10:],
        }

    async def aclose(self) -> None:
        await self._client.aclose()
