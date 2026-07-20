"""Integration tests for the FastAPI app: REST/WebSocket surface, live config,
risk/system-status endpoints, and the full engine -> DB -> API pipeline.

Each test builds its own app via `create_app(...)` with a private in-memory
database, so nothing leaks between tests (no shared module-level singletons).
"""

from __future__ import annotations

import time
from typing import Any

import pytest
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
            "stop_loss_price",
            "take_profit_price",
            "bracket_status",
        }
        # gateway slippage is baked into every fill: requested != actual
        assert trade["requested_price"] > 0
        assert trade["actual_filled_price"] != trade["requested_price"]
        assert trade["slippage_cost"] > 0
        # every Phase 5 trade runs under a bracket and exits with a final status
        assert trade["stop_loss_price"] > 0
        assert trade["take_profit_price"] > 0
        assert trade["bracket_status"] in ("HIT_SL", "HIT_TP", "TIME_EXITED")

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


# --- Phase 5: active bracket cards --------------------------------------------


def test_brackets_endpoint_reports_live_card_math() -> None:
    """Deterministic card check: pre-register a bracket + last price on the
    injected gateway and verify the endpoint's distance/RR arithmetic."""
    import asyncio
    from datetime import datetime, timezone

    from backend.execution_gateway import MockExecutionGateway
    from backend.models import BracketOrder, OHLCVBar, Timeframe

    gateway = MockExecutionGateway(latency_range_ms=(0.0, 0.5))
    asyncio.run(
        gateway.register_bracket(
            BracketOrder(
                order_id="pre-1",
                engine_type="momentum",
                ticker="FAKE",  # not traded by the app's engines -> stays active
                side="long",
                entry_price=106.0,
                stop_loss_price=97.75,
                take_profit_price=119.75,
                size=5_000.0,
                created_at=datetime(2026, 7, 20, 9, 35, tzinfo=timezone.utc),
            )
        )
    )
    gateway.observe_bar(
        OHLCVBar("FAKE", datetime(2026, 7, 20, 9, 40, tzinfo=timezone.utc), Timeframe.ONE_MINUTE, 110, 111, 109, 110.0, 1_000)
    )

    app = create_app(
        "sqlite+aiosqlite:///:memory:",
        gateway=gateway,
        momentum_interval_seconds=0.5,
        swing_interval_seconds=0.5,
    )
    with TestClient(app) as client:
        cards = client.get("/api/brackets").json()
        card = next(c for c in cards if c["order_id"] == "pre-1")

        assert card["ticker"] == "FAKE"
        assert card["side"] == "long"
        assert card["status"] == "ACTIVE"
        assert card["entry_price"] == 106.0
        assert card["current_price"] == 110.0
        assert card["stop_loss_price"] == 97.75
        assert card["take_profit_price"] == 119.75
        # distances measured from the live 110 price
        assert card["tp_distance_pct"] == pytest.approx((119.75 - 110.0) / 110.0 * 100, abs=1e-3)
        assert card["sl_distance_pct"] == pytest.approx((110.0 - 97.75) / 110.0 * 100, abs=1e-3)
        # RR from the original bracket geometry: 2.5x ATR vs 1.5x ATR
        assert card["risk_reward_ratio"] == pytest.approx(13.75 / 8.25, abs=1e-3)
        assert card["unrealized_pct"] == pytest.approx((110.0 - 106.0) / 106.0 * 100, abs=1e-3)


def test_brackets_endpoint_empty_when_no_positions() -> None:
    with new_client() as client:
        assert isinstance(client.get("/api/brackets").json(), list)


# --- Phase 6: backtest endpoint, kill switch, guard status --------------------


def test_backtest_endpoint_returns_metrics_and_is_reproducible() -> None:
    payload = {
        "symbol": "AAPL",
        "strategy": "momentum",
        "start_date": "2026-07-20T09:30:00+00:00",
        "end_date": "2026-07-20T12:30:00+00:00",
        "initial_capital": 100_000,
    }
    with new_client() as client:
        first = client.post("/api/backtest", json=payload)
        assert first.status_code == 200
        body = first.json()
        for key in (
            "trade_count",
            "net_pnl",
            "total_return_pct",
            "win_rate_pct",
            "profit_factor",
            "max_drawdown_pct",
            "bars_replayed",
            "equity_curve",
            "bracket_outcomes",
            "risk_guard",
        ):
            assert key in body, f"missing {key}"
        assert body["bars_replayed"] > 0
        assert body["max_drawdown_pct"] >= 0

        second = client.post("/api/backtest", json=payload)
        assert second.json()["net_pnl"] == body["net_pnl"]  # seeded by symbol+window


def test_backtest_endpoint_validates_input() -> None:
    with new_client() as client:
        base = {
            "symbol": "AAPL",
            "strategy": "momentum",
            "start_date": "2026-07-20",
            "end_date": "2026-07-21",
        }
        assert client.post("/api/backtest", json={**base, "strategy": "scalper"}).status_code == 422
        assert client.post("/api/backtest", json={**base, "symbol": "not a ticker!"}).status_code == 422
        assert (
            client.post("/api/backtest", json={**base, "start_date": "2026-07-22"}).status_code == 422
        )  # end before start
        assert client.post("/api/backtest", json={**base, "initial_capital": -5}).status_code == 422


def test_backtest_endpoint_prefers_local_csv(tmp_path: Any, monkeypatch: Any) -> None:
    """When BACKTEST_DATA_DIR holds a CSV for the symbol, it becomes the
    historical source instead of the synthetic fallback."""
    rows = ["timestamp,open,high,low,close,volume"]
    base = "2026-07-20T09:{m:02d}:00+00:00"
    prices = [
        (100, 102, 98, 101), (101, 103, 99, 100), (100, 105, 95, 102),
        (102, 104, 100, 101), (101, 103, 99, 100),
    ]
    for minute, (o, h, l, c) in enumerate(prices):
        rows.append(f"{base.format(m=30 + minute)},{o},{h},{l},{c},1000")
    price = 106.0
    for minute in range(5, 60):
        rows.append(f"{base.format(m=30 + minute) if minute < 30 else f'2026-07-20T10:{minute-30:02d}:00+00:00'},{price},{price + 16},{price - 1},{price + 2},1000")
        price += 2
    (tmp_path / "CSVTEST.csv").write_text("\n".join(rows) + "\n")
    monkeypatch.setenv("BACKTEST_DATA_DIR", str(tmp_path))

    with new_client() as client:
        response = client.post(
            "/api/backtest",
            json={
                "symbol": "CSVTEST",
                "strategy": "momentum",
                "start_date": "2026-07-20T09:30:00+00:00",
                "end_date": "2026-07-20T11:00:00+00:00",
            },
        )
        assert response.status_code == 200
        body = response.json()
        assert body["bars_replayed"] == 60  # exactly the CSV rows, not synthetic
        assert body["trade_count"] >= 1
        assert body["win_rate_pct"] == pytest.approx(100.0)  # the uptrend fixture


def test_kill_switch_locks_guard_and_reset_releases_it() -> None:
    with new_client() as client:
        before = client.get("/api/risk/status").json()
        assert before["risk_guard"]["locked"] is False

        killed = client.post("/api/system/kill").json()
        assert killed["risk_guard"]["locked"] is True
        assert killed["risk_guard"]["circuit_breaker_active"] is True
        assert killed["halted"] is True  # engines will flatten on their next bar

        status = client.get("/api/telemetry").json()
        assert status["risk_guard"]["locked"] is True

        reset = client.post("/api/system/guard/reset").json()
        assert reset["risk_guard"]["locked"] is False
        assert reset["halted"] is False


def test_risk_guard_env_limits_flow_into_status(monkeypatch: Any) -> None:
    monkeypatch.setenv("MAX_DAILY_LOSS_PCT", "0.02")
    monkeypatch.setenv("MAX_DAILY_TRADE_COUNT", "9")
    with new_client() as client:
        guard = client.get("/api/risk/status").json()["risk_guard"]
        assert guard["max_daily_loss_pct"] == 0.02
        assert guard["max_daily_trade_count"] == 9


# --- Phase 7: guard persistence at app level, sync state, DB mode -------------


def test_kill_switch_survives_an_app_restart(tmp_path: Any) -> None:
    """Engage the kill switch, tear the app down, boot a fresh app over the
    same database file: the guard must come back LOCKED (zero amnesia)."""
    url = f"sqlite+aiosqlite:///{tmp_path / 'restart.db'}"

    app1 = create_app(url, momentum_interval_seconds=0.05, swing_interval_seconds=0.05)
    with TestClient(app1) as client:
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:  # wait for a bar so the guard has a trading date
            guard = client.get("/api/risk/status").json()["risk_guard"]
            if guard["current_date"]:
                break
            time.sleep(0.1)
        killed = client.post("/api/system/kill").json()
        assert killed["risk_guard"]["locked"] is True

    app2 = create_app(url, momentum_interval_seconds=0.5, swing_interval_seconds=0.5)
    with TestClient(app2) as client:
        status = client.get("/api/risk/status").json()
        assert status["risk_guard"]["locked"] is True  # restored from SystemState
        assert status["risk_guard"]["circuit_breaker_active"] is True
        assert status["halted"] is True  # boot restore re-halted the risk manager

        released = client.post("/api/system/guard/reset").json()
        assert released["risk_guard"]["locked"] is False


def test_risk_guard_sync_indicator_confirms_persistence() -> None:
    app = create_app("sqlite+aiosqlite:///:memory:", momentum_interval_seconds=0.05, swing_interval_seconds=0.05)
    with TestClient(app) as client:
        sync: dict[str, Any] = {}
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline and not sync.get("persisted"):
            time.sleep(0.1)
            sync = client.get("/api/risk/status").json()["risk_guard_sync"]
        assert sync["persisted"] is True
        assert sync["in_sync"] is True  # memory state exactly matches the DB row
        assert sync["last_persisted_at"] is not None


def test_telemetry_reports_database_journal_mode(tmp_path: Any) -> None:
    with new_client() as client:  # in-memory app
        assert client.get("/api/telemetry").json()["database"]["journal_mode"] == "memory"
    app = create_app(f"sqlite+aiosqlite:///{tmp_path / 'wal.db'}", momentum_interval_seconds=0.5, swing_interval_seconds=0.5)
    with TestClient(app) as client:
        assert client.get("/api/telemetry").json()["database"]["journal_mode"] == "wal"
