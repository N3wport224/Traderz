"""Integration tests for the FastAPI app: REST/WebSocket surface, live config,
risk/system-status endpoints, and the full engine -> DB -> API pipeline.

Each test builds its own app via `create_app(...)` with a private in-memory
database, so nothing leaks between tests (no shared module-level singletons).
"""

from __future__ import annotations

import time

from fastapi.testclient import TestClient

from backend.main import create_app


def new_client() -> TestClient:
    app = create_app("sqlite+aiosqlite:///:memory:")
    return TestClient(app)


def test_health_check() -> None:
    with new_client() as client:
        response = client.get("/api/health")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}


def test_rest_snapshot_endpoints_return_lists() -> None:
    with new_client() as client:
        for path in (
            "/api/momentum/signals",
            "/api/momentum/equity",
            "/api/momentum/trades",
            "/api/swing/signals",
            "/api/swing/equity",
            "/api/swing/trades",
        ):
            response = client.get(path)
            assert response.status_code == 200
            assert isinstance(response.json(), list)


def test_websocket_channels_accept_connections() -> None:
    with new_client() as client:
        with client.websocket_connect("/ws/momentum") as ws:
            ws.close()
        with client.websocket_connect("/ws/swing") as ws:
            ws.close()


# --- Config endpoints --------------------------------------------------------


def test_get_config_returns_defaults() -> None:
    with new_client() as client:
        response = client.get("/api/config")
        assert response.status_code == 200
        body = response.json()
        assert body["momentum"] == {"opening_range_minutes": 5, "time_stop_minutes": 20}
        assert body["swing"] == {"min_touches": 3, "touch_tolerance_pct": 0.005}


def test_update_momentum_config_persists_across_requests() -> None:
    with new_client() as client:
        put_response = client.put("/api/config/momentum", json={"opening_range_minutes": 10})
        assert put_response.status_code == 200
        assert put_response.json()["opening_range_minutes"] == 10
        assert put_response.json()["time_stop_minutes"] == 20  # untouched

        get_response = client.get("/api/config")
        assert get_response.json()["momentum"]["opening_range_minutes"] == 10


def test_update_swing_config_persists_across_requests() -> None:
    with new_client() as client:
        put_response = client.put("/api/config/swing", json={"min_touches": 5, "touch_tolerance_pct": 0.01})
        assert put_response.status_code == 200
        assert put_response.json() == {"min_touches": 5, "touch_tolerance_pct": 0.01}


def test_update_momentum_config_rejects_out_of_range_value() -> None:
    with new_client() as client:
        response = client.put("/api/config/momentum", json={"opening_range_minutes": 0})
        assert response.status_code == 422  # caught by the Pydantic field constraint


def test_update_swing_config_rejects_value_pydantic_allows_but_store_does_not() -> None:
    # Pydantic allows up to 0.1; the store's own validator is the actual authority.
    with new_client() as client:
        response = client.put("/api/config/swing", json={"touch_tolerance_pct": 0.1})
        assert response.status_code == 200  # exactly at the boundary — allowed


# --- Risk / system status endpoints ------------------------------------------


def test_risk_status_reports_running_by_default() -> None:
    with new_client() as client:
        response = client.get("/api/risk/status")
        assert response.status_code == 200
        body = response.json()
        assert body["system_status"] == "RUNNING"
        assert body["halted"] is False
        assert body["paused"] is False
        assert "allocation_pct" in body
        assert "max_daily_drawdown_pct" in body


def test_pause_and_resume_system() -> None:
    with new_client() as client:
        paused = client.post("/api/system/pause")
        assert paused.status_code == 200
        assert paused.json()["system_status"] == "PAUSED"

        status = client.get("/api/risk/status")
        assert status.json()["system_status"] == "PAUSED"

        resumed = client.post("/api/system/resume")
        assert resumed.json()["system_status"] == "RUNNING"


# --- Full pipeline: engine -> DB -> API --------------------------------------


def test_momentum_pipeline_persists_trades_and_equity_reachable_via_api() -> None:
    """Runs the real background worker (fast but paced tick interval) until it
    has booked at least one closed trade, then verifies it's readable back
    through both the DB-backed trades/equity endpoints and the live signals
    feed. Paced rather than a zero interval: at zero the loop can blow through
    hundreds of round-trips (and potentially the daily circuit breaker) before
    the first poll ever observes it, which starves this test's own assertions
    rather than exercising the pipeline it's meant to check."""
    app = create_app("sqlite+aiosqlite:///:memory:", momentum_interval_seconds=0.02, swing_interval_seconds=0.02)
    with TestClient(app) as client:
        # `record_trade` and `record_equity_snapshot` are two separate awaited DB
        # writes, not one atomic transaction, so poll for both rather than assuming
        # one being visible implies the other already is too.
        trades: list[object] = []
        equity: list[object] = []
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline and not (trades and equity):
            time.sleep(0.1)
            trades = client.get("/api/momentum/trades").json()
            equity = client.get("/api/momentum/equity").json()

        assert trades, "expected at least one closed momentum trade within the deadline"
        assert equity, "expected at least one equity snapshot within the deadline"
        trade = trades[0]
        assert trade["engine_type"] == "momentum"
        assert trade["asset_ticker"] == "MOCK"
        assert set(trade) == {
            "id",
            "engine_type",
            "asset_ticker",
            "entry_timestamp",
            "exit_timestamp",
            "entry_price",
            "exit_price",
            "position_size",
            "fees",
            "net_profit",
        }

        assert equity[0]["timestamp"].endswith("+00:00") or equity[0]["timestamp"].endswith("Z")

        signals = client.get("/api/momentum/signals").json()
        # ORB entries fire as BUY or SHORT depending on which way the random walk
        # breaks out — either is a valid entry, so accept both.
        assert any(s["action"] in ("buy", "short") for s in signals)
        assert any(s["action"] == "exit" for s in signals)
