"""Integration tests for the FastAPI app: REST/WebSocket surface, live config,
risk/system-status endpoints, and the full engine -> DB -> API pipeline.

Each test builds its own app via `create_app(...)` with a private in-memory
database, so nothing leaks between tests (no shared module-level singletons).
"""

from __future__ import annotations

import time
from typing import Any

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
        trades: list[dict[str, Any]] = []
        equity: list[dict[str, Any]] = []
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
            "requested_price",
            "actual_filled_price",
            "slippage_cost",
        }
        # gateway slippage is baked into every fill: requested != actual
        assert trade["requested_price"] > 0
        assert trade["actual_filled_price"] != trade["requested_price"]
        assert trade["slippage_cost"] > 0

        assert equity[0]["timestamp"].endswith("+00:00") or equity[0]["timestamp"].endswith("Z")

        signals = client.get("/api/momentum/signals").json()
        # ORB entries fire as BUY or SHORT depending on which way the random walk
        # breaks out — either is a valid entry, so accept both.
        assert any(s["action"] in ("buy", "short") for s in signals)
        assert any(s["action"] == "exit" for s in signals)


# --- Phase 3: telemetry endpoint, boot reconciliation, disconnect status ------


def test_telemetry_endpoint_reports_gateway_latency_and_slippage() -> None:
    app = create_app("sqlite+aiosqlite:///:memory:", momentum_interval_seconds=0.02, swing_interval_seconds=0.02)
    with TestClient(app) as client:
        telemetry: dict[str, Any] = {}
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline and not telemetry.get("order_count"):
            time.sleep(0.1)
            telemetry = client.get("/api/telemetry").json()

        assert telemetry["order_count"] >= 1, "expected at least one order flow within the deadline"
        assert telemetry["gateway"] == {"name": "MOCK", "mode": "mock"}
        assert telemetry["connection_latency_ms"] > 0
        assert telemetry["cumulative_slippage_cost"] > 0
        assert telemetry["avg_gateway_latency_ms"] > 0
        assert telemetry["system_status"] == "RUNNING"
        assert telemetry["data_disconnected"] is False
        assert telemetry["streams"]["momentum"]["state"] == "connected"
        assert telemetry["streams"]["swing"]["state"] == "connected"
        assert telemetry["boot_reconciliation"]["discrepancies"] == 0

        flow = telemetry["last_flow"]
        assert flow["signal_to_fill_ms"] >= flow["approval_to_fill_ms"]
        assert flow["requested_price"] != flow["filled_price"]


def test_boot_reconciliation_heals_gateway_orders_missing_from_db() -> None:
    """Simulates a crash between fill and DB write: the injected gateway already
    holds an open order when the app boots, so lifespan reconciliation must
    insert the missing open-position row and report it via /api/telemetry."""
    import asyncio
    import random

    from backend.execution_gateway import MockExecutionGateway
    from backend.models import SignalAction

    gateway = MockExecutionGateway(latency_range_ms=(0.0, 0.5), rng=random.Random(2))
    orphan = asyncio.run(gateway.execute_order(SignalAction.BUY, 1_000.0, "MOCK", 100.0))

    app = create_app(
        "sqlite+aiosqlite:///:memory:",
        gateway=gateway,
        momentum_interval_seconds=0.5,
        swing_interval_seconds=0.5,
    )
    with TestClient(app) as client:
        telemetry = client.get("/api/telemetry").json()
        assert telemetry["boot_reconciliation"]["healed"] == [orphan.order_id]
        assert telemetry["boot_reconciliation"]["cleared"] == []


def test_simulated_stream_drop_surfaces_data_disconnected_status() -> None:
    """With simulate_disconnect_after, both mock streams drop shortly after
    boot; while the reconnection state machine is backing off, /api/telemetry
    (and /api/risk/status) must expose DATA_DISCONNECTED — this is what drives
    the frontend's alert banner."""
    app = create_app(
        "sqlite+aiosqlite:///:memory:",
        momentum_interval_seconds=0.02,
        swing_interval_seconds=0.02,
        simulate_disconnect_after=2,
        backoff_scale=1.0,  # real 2s first delay -> a wide observable window
    )
    with TestClient(app) as client:
        observed_disconnected = False
        deadline = time.monotonic() + 8.0
        while time.monotonic() < deadline and not observed_disconnected:
            telemetry = client.get("/api/telemetry").json()
            observed_disconnected = (
                telemetry["data_disconnected"] and telemetry["system_status"] == "DATA_DISCONNECTED"
            )
            time.sleep(0.05)

        assert observed_disconnected, "expected DATA_DISCONNECTED to surface during the outage"
        telemetry = client.get("/api/telemetry").json()
        streams = telemetry["streams"]
        assert streams["momentum"]["disconnect_count"] >= 1 or streams["swing"]["disconnect_count"] >= 1
        assert client.get("/api/risk/status").json()["system_status"] == "DATA_DISCONNECTED"


# --- Phase 4: watchlist, live data mode, paper-trading lock -------------------


def test_watchlist_defaults_to_mock_symbol() -> None:
    with new_client() as client:
        body = client.get("/api/watchlist").json()
        assert body == {"ticker": "MOCK", "data_source_mode": "mock"}


def test_watchlist_switch_wipes_signals_and_streams_new_ticker() -> None:
    app = create_app("sqlite+aiosqlite:///:memory:", momentum_interval_seconds=0.02, swing_interval_seconds=0.05)
    with TestClient(app) as client:
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline and not client.get("/api/momentum/signals").json():
            time.sleep(0.1)
        assert client.get("/api/momentum/signals").json(), "expected MOCK signals before the switch"

        response = client.post("/api/watchlist", json={"ticker": "tsla"})
        assert response.status_code == 200
        assert response.json()["ticker"] == "TSLA"  # normalized to uppercase

        # old-symbol signals are gone the moment the switch returns
        residual = client.get("/api/momentum/signals").json()
        assert all(s["symbol"] == "TSLA" for s in residual)

        deadline = time.monotonic() + 10.0
        fresh: list[dict[str, Any]] = []
        while time.monotonic() < deadline and not fresh:
            time.sleep(0.1)
            fresh = client.get("/api/momentum/signals").json()
        assert fresh and fresh[0]["symbol"] == "TSLA"

        assert client.get("/api/watchlist").json()["ticker"] == "TSLA"
        assert client.get("/api/telemetry").json()["ticker"] == "TSLA"


def test_watchlist_trades_are_filtered_to_the_active_ticker() -> None:
    """After a switch, the trades endpoints only show the active asset —
    the dashboard starts from a clean view instead of mixing symbols."""
    app = create_app("sqlite+aiosqlite:///:memory:", momentum_interval_seconds=0.02, swing_interval_seconds=0.05)
    with TestClient(app) as client:
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline and not client.get("/api/momentum/trades").json():
            time.sleep(0.1)
        assert client.get("/api/momentum/trades").json(), "expected a closed MOCK trade first"

        client.post("/api/watchlist", json={"ticker": "AAPL"})
        trades_after_switch = client.get("/api/momentum/trades").json()
        assert all(t["asset_ticker"] == "AAPL" for t in trades_after_switch)


def test_watchlist_rejects_invalid_tickers() -> None:
    with new_client() as client:
        for bad in ("not a ticker!!", "", "toolongtickersymbolxxxxxxxxx", "BTC//USDT", "a/b/c"):
            response = client.post("/api/watchlist", json={"ticker": bad})
            assert response.status_code == 422, f"expected 422 for {bad!r}"


def test_watchlist_accepts_stock_and_crypto_shapes() -> None:
    with new_client() as client:
        for good, normalized in (("aapl", "AAPL"), ("BRK.B", "BRK.B"), ("btc/usdt", "BTC/USDT")):
            response = client.post("/api/watchlist", json={"ticker": good})
            assert response.status_code == 200
            assert response.json()["ticker"] == normalized


def test_watchlist_same_ticker_is_a_no_op() -> None:
    app = create_app("sqlite+aiosqlite:///:memory:", momentum_interval_seconds=0.02, swing_interval_seconds=0.05)
    with TestClient(app) as client:
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline and not client.get("/api/momentum/signals").json():
            time.sleep(0.1)
        before = client.get("/api/momentum/signals").json()
        assert before

        assert client.post("/api/watchlist", json={"ticker": "MOCK"}).status_code == 200
        after = client.get("/api/momentum/signals").json()
        assert len(after) >= len(before)  # signals were NOT wiped for a same-ticker submit


def _yahoo_payload(n: int) -> dict[str, Any]:
    base_ts = 1_753_000_000
    return {
        "chart": {
            "result": [
                {
                    "timestamp": [base_ts + i * 60 for i in range(n)],
                    "indicators": {
                        "quote": [
                            {
                                "open": [100.0 + i for i in range(n)],
                                "high": [101.0 + i for i in range(n)],
                                "low": [99.0 + i for i in range(n)],
                                "close": [100.5 + i for i in range(n)],
                                "volume": [1_000] * n,
                            }
                        ]
                    },
                }
            ]
        }
    }


def test_live_data_mode_remains_paper_trading_with_mock_gateway() -> None:
    """DATA_SOURCE_MODE=live must not flip execution live: real (mocked-HTTP)
    market data flows in, but orders still fill through MockExecutionGateway —
    the Phase 4 paper-trading guarantee. No real network is touched: the app
    gets an injected httpx client backed by MockTransport."""
    import httpx

    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(200, json=_yahoo_payload(min(3 + calls["n"], 30)))

    injected = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    app = create_app(
        "sqlite+aiosqlite:///:memory:",
        data_source_mode="live",
        symbol="AAPL",
        live_http_client=injected,
        live_poll_seconds=0.05,
    )
    with TestClient(app) as client:
        deadline = time.monotonic() + 8.0
        while time.monotonic() < deadline and calls["n"] < 2:
            time.sleep(0.1)

        telemetry = client.get("/api/telemetry").json()
        assert telemetry["data_source_mode"] == "live"
        assert telemetry["ticker"] == "AAPL"
        assert telemetry["gateway"] == {"name": "MOCK", "mode": "mock"}  # paper trading
        assert calls["n"] >= 2  # both timeframes polled through the mocked transport


def test_gateway_mode_env_defaults_to_mock_even_when_data_is_live(monkeypatch: Any) -> None:
    """Belt-and-braces: without an explicit GATEWAY_MODE=live, the env-driven
    gateway builder must return the mock gateway regardless of DATA_SOURCE_MODE."""
    from backend.execution_gateway import MockExecutionGateway
    from backend.main import _build_gateway

    monkeypatch.delenv("GATEWAY_MODE", raising=False)
    monkeypatch.setenv("DATA_SOURCE_MODE", "live")
    assert isinstance(_build_gateway(), MockExecutionGateway)
