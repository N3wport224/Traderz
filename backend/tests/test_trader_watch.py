"""Phase 10 copy-trading tests: trader CRUD, the auto-follow toggle + budget
validation, manual event logging with notifications, gateway mirroring for
BUY/SELL round trips, and RiskGuard enforcement on copied entries.

Apps are built with an injected zero-slippage mock gateway (deterministic
fills) and an unconfigured SystemNotifier (log-only mode) whose in-memory
history doubles as the notification assertion surface.
"""

from __future__ import annotations

from typing import Any

from fastapi.testclient import TestClient

from backend.execution_gateway import MockExecutionGateway
from backend.main import create_app
from backend.utils.notifier import SystemNotifier


def make_app() -> tuple[Any, MockExecutionGateway, SystemNotifier]:
    gateway = MockExecutionGateway(
        fee_rate=0.0, min_slippage_pct=0.0, max_slippage_pct=0.0, latency_range_ms=(0.0, 0.0)
    )
    notifier = SystemNotifier(webhook_url=None)
    notifier.webhook_url = None  # belt-and-braces against env leakage
    app = create_app("sqlite+aiosqlite:///:memory:", gateway=gateway, system_notifier=notifier)
    return app, gateway, notifier


def add_trader(client: TestClient, name: str = "Dave", **overrides: Any) -> dict[str, Any]:
    payload = {"name": name, "asset_class": "stock", **overrides}
    response = client.post("/api/traders", json=payload)
    assert response.status_code == 201, response.text
    return response.json()


def watch_events(notifier: SystemNotifier) -> list[dict[str, Any]]:
    return [e for e in notifier.history if str(e["title"]).startswith("Watched trader")]


# --- trader CRUD ---------------------------------------------------------------


def test_create_list_and_delete_traders() -> None:
    app, _, _ = make_app()
    with TestClient(app) as client:
        created = add_trader(client, "Dave", notes="my cousin the day trader")
        assert created["auto_follow"] is False  # manual mode by default
        assert created["budget_amount"] == 0.0

        listed = client.get("/api/traders").json()
        assert [t["name"] for t in listed] == ["Dave"]

        assert client.post("/api/traders", json={"name": "dave"}).status_code == 409  # dupe

        assert client.delete(f"/api/traders/{created['id']}").status_code == 200
        assert client.get("/api/traders").json() == []
        assert client.delete("/api/traders/999").status_code == 404


def test_blank_or_bad_payloads_rejected() -> None:
    app, _, _ = make_app()
    with TestClient(app) as client:
        assert client.post("/api/traders", json={"name": "   "}).status_code == 422
        trader = add_trader(client)
        bad_ticker = client.post(
            f"/api/traders/{trader['id']}/events",
            json={"ticker": "not a ticker!!", "action": "BUY", "price": 10.0},
        )
        assert bad_ticker.status_code == 422
        bad_action = client.post(
            f"/api/traders/{trader['id']}/events",
            json={"ticker": "AAPL", "action": "HOLD", "price": 10.0},
        )
        assert bad_action.status_code == 422


# --- follow toggle -------------------------------------------------------------


def test_enabling_auto_follow_requires_positive_budget() -> None:
    app, _, _ = make_app()
    with TestClient(app) as client:
        trader = add_trader(client)
        rejected = client.put(
            f"/api/traders/{trader['id']}/follow",
            json={"auto_follow": True, "budget_amount": 0},
        )
        assert rejected.status_code == 422
        assert "budget" in rejected.json()["detail"]

        enabled = client.put(
            f"/api/traders/{trader['id']}/follow",
            json={"auto_follow": True, "budget_amount": 500.0},
        )
        assert enabled.status_code == 200
        assert enabled.json()["auto_follow"] is True
        assert enabled.json()["budget_amount"] == 500.0

        # toggling off keeps the remembered budget for next time
        disabled = client.put(
            f"/api/traders/{trader['id']}/follow",
            json={"auto_follow": False, "budget_amount": 500.0},
        ).json()
        assert disabled["auto_follow"] is False and disabled["budget_amount"] == 500.0


# --- manual (watch-only) events ------------------------------------------------


def test_manual_event_records_and_notifies_without_trading() -> None:
    app, gateway, notifier = make_app()
    with TestClient(app) as client:
        trader = add_trader(client)
        event = client.post(
            f"/api/traders/{trader['id']}/events",
            json={"ticker": "AAPL", "action": "BUY", "price": 231.2, "note": "posted on X"},
        ).json()
        assert event["followed"] is False
        assert "auto-follow off" in event["follow_detail"]

        feed = client.get("/api/traders/feed").json()
        assert feed["events"][0]["ticker"] == "AAPL"
        assert feed["events"][0]["trader_name"] == "Dave"
        assert feed["open_copied_positions"] == []

        notes = watch_events(notifier)
        assert notes and notes[-1]["level"] == "INFO"  # watch-only => INFO
        assert "BUY AAPL" in notes[-1]["title"]
        # nothing was routed to the broker for this ticker
        assert all(f.ticker != "AAPL" for f in gateway.fills)


def test_event_for_unknown_trader_is_404() -> None:
    app, _, _ = make_app()
    with TestClient(app) as client:
        response = client.post(
            "/api/traders/42/events", json={"ticker": "AAPL", "action": "BUY", "price": 10.0}
        )
        assert response.status_code == 404


# --- auto-follow mirroring ------------------------------------------------------


def test_auto_follow_buy_mirrors_with_budget_then_sell_closes() -> None:
    app, gateway, notifier = make_app()
    with TestClient(app) as client:
        trader = add_trader(client)
        client.put(
            f"/api/traders/{trader['id']}/follow",
            json={"auto_follow": True, "budget_amount": 1_000.0},
        )

        buy = client.post(
            f"/api/traders/{trader['id']}/events",
            json={"ticker": "TSLA", "action": "BUY", "price": 200.0},
        ).json()
        assert buy["followed"] is True
        assert "auto-followed: bought $1,000.00 @ 200" in buy["follow_detail"]

        # the copy order is live in the gateway's broker-side book
        feed = client.get("/api/traders/feed").json()
        assert feed["open_copied_positions"] == [
            {"trader_id": trader["id"], "ticker": "TSLA", "size": 1_000.0, "entry": 200.0}
        ]
        alerts = watch_events(notifier)
        assert alerts[-1]["level"] == "ALERT"  # money (paper) moved => ALERT

        # duplicate BUY while holding: skipped, not doubled
        dupe = client.post(
            f"/api/traders/{trader['id']}/events",
            json={"ticker": "TSLA", "action": "BUY", "price": 210.0},
        ).json()
        assert dupe["followed"] is False
        assert "already holding" in dupe["follow_detail"]

        sell = client.post(
            f"/api/traders/{trader['id']}/events",
            json={"ticker": "TSLA", "action": "SELL", "price": 220.0},
        ).json()
        assert sell["followed"] is True
        assert "+10.00% vs copied entry" in sell["follow_detail"]
        assert client.get("/api/traders/feed").json()["open_copied_positions"] == []
        # round trip landed in the gateway book and closed cleanly
        tsla_fills = [f for f in gateway.fills if f.ticker == "TSLA"]
        assert len(tsla_fills) == 2


def test_sell_without_copied_position_is_skipped_gracefully() -> None:
    app, gateway, _ = make_app()
    with TestClient(app) as client:
        trader = add_trader(client)
        client.put(
            f"/api/traders/{trader['id']}/follow",
            json={"auto_follow": True, "budget_amount": 750.0},
        )
        sell = client.post(
            f"/api/traders/{trader['id']}/events",
            json={"ticker": "NVDA", "action": "SELL", "price": 100.0},
        ).json()
        assert sell["followed"] is False
        assert "no copied position" in sell["follow_detail"]
        assert all(f.ticker != "NVDA" for f in gateway.fills)


def test_risk_guard_lock_blocks_copies_but_still_notifies() -> None:
    app, gateway, notifier = make_app()
    with TestClient(app) as client:
        trader = add_trader(client)
        client.put(
            f"/api/traders/{trader['id']}/follow",
            json={"auto_follow": True, "budget_amount": 1_000.0},
        )
        client.post("/api/system/kill")  # trips the guard: entries must be rejected

        event = client.post(
            f"/api/traders/{trader['id']}/events",
            json={"ticker": "TSLA", "action": "BUY", "price": 200.0},
        ).json()
        assert event["followed"] is False
        assert "blocked by risk guard" in event["follow_detail"]
        assert all(f.ticker != "TSLA" for f in gateway.fills)  # nothing slipped through
        # the user still hears about the trader's move
        assert any("BUY TSLA" in str(e["title"]) for e in watch_events(notifier))


def test_feed_limit_is_clamped_and_ordered_newest_first() -> None:
    app, _, _ = make_app()
    with TestClient(app) as client:
        trader = add_trader(client)
        for i in range(5):
            client.post(
                f"/api/traders/{trader['id']}/events",
                json={"ticker": "AAPL", "action": "BUY", "price": 100.0 + i},
            )
        feed = client.get("/api/traders/feed", params={"limit": 3}).json()
        assert len(feed["events"]) == 3
        prices = [e["price"] for e in feed["events"]]
        assert prices == [104.0, 103.0, 102.0]  # newest first
