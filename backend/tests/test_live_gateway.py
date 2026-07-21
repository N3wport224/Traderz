"""Safety tests for the Phase 8 LiveExecutionGateway (Alpaca-blueprint broker).

Every broker interaction runs against an `httpx.MockTransport` — no test here
ever touches the real network. Covered: payload/credential mapping, successful
and partial fills, broker timeouts and 5xx rejections, invalid credentials,
the exit-order retry loop that halts the platform via `RiskManager.halt()` on
total failure, RiskGuard parity with the mock gateway, and the structural
PROD_LIVE + I_AM_RISKING_REAL_MONEY double lock in the composition root.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Callable

import httpx
import pytest

from backend.execution.live_gateway import LiveExecutionGateway
from backend.execution_gateway import GatewayConfigError, GatewayError
from backend.models import OrderStatus, SignalAction
from backend.risk_manager import RiskManager
from backend.utils.risk_guard import RiskGuard, RiskGuardTripped

NOW = datetime(2026, 7, 20, 14, 30, tzinfo=timezone.utc)


def broker_response(request: httpx.Request, *, filled_qty: float | None = None,
                    filled_avg_price: float | None = None) -> httpx.Response:
    payload = json.loads(request.content.decode())
    qty = float(payload["qty"])
    return httpx.Response(
        200,
        json={
            "id": "broker-order-1",
            "symbol": payload["symbol"],
            "status": "filled",
            "filled_qty": str(filled_qty if filled_qty is not None else qty),
            "filled_avg_price": str(filled_avg_price if filled_avg_price is not None else 100.0),
        },
    )


def make_gateway(
    handler: Callable[[httpx.Request], httpx.Response],
    *,
    risk_manager: RiskManager | None = None,
    risk_guard: RiskGuard | None = None,
    exit_retry_attempts: int = 3,
) -> LiveExecutionGateway:
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    gateway = LiveExecutionGateway(
        "test-key",
        "test-secret",
        "https://broker.example.test",
        risk_manager=risk_manager,
        client=client,
        exit_retry_attempts=exit_retry_attempts,
        exit_retry_backoff_seconds=0.001,  # keep retry tests fast
    )
    gateway.risk_guard = risk_guard
    return gateway


# --- construction / credentials -----------------------------------------------


def test_missing_credentials_refuse_construction(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LIVE_BROKER_API_KEY", raising=False)
    monkeypatch.delenv("LIVE_BROKER_SECRET", raising=False)
    with pytest.raises(GatewayConfigError, match="LIVE_BROKER_API_KEY"):
        LiveExecutionGateway()


def test_env_credentials_and_url_are_read(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LIVE_BROKER_API_KEY", "env-key")
    monkeypatch.setenv("LIVE_BROKER_SECRET", "env-secret")
    monkeypatch.setenv("LIVE_BROKER_URL", "https://paper.example.test/")
    gateway = LiveExecutionGateway()
    assert gateway.broker_url == "https://paper.example.test"  # trailing slash stripped
    assert gateway.name == "PROD_LIVE"


# --- order mapping + successful fills -----------------------------------------


@pytest.mark.asyncio
async def test_successful_entry_maps_internal_order_to_broker_payload() -> None:
    seen: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(
            {
                "url": str(request.url),
                "headers": dict(request.headers),
                "body": json.loads(request.content.decode()),
            }
        )
        return broker_response(request, filled_avg_price=100.5)

    gateway = make_gateway(handler)
    fill = await gateway.execute_order(SignalAction.BUY, 10_000.0, "BTC/USD", 100.0)

    assert seen[0]["url"] == "https://broker.example.test/v2/orders"
    assert seen[0]["headers"]["apca-api-key-id"] == "test-key"
    assert seen[0]["headers"]["apca-api-secret-key"] == "test-secret"
    body = seen[0]["body"]
    assert body["symbol"] == "BTCUSD"  # internal BTC/USD -> broker symbology
    assert body == {"symbol": "BTCUSD", "qty": "100", "side": "buy", "type": "market", "time_in_force": "gtc"}

    assert fill.order_id == "broker-order-1"
    assert fill.status is OrderStatus.FILLED
    assert fill.filled_price == 100.5
    assert fill.filled_size == pytest.approx(100 * 100.5)
    # $0.50 adverse on 100.5 shares (shares derived from filled notional, the
    # same convention as the mock gateway's slippage accounting)
    assert fill.slippage_cost == pytest.approx(0.5 * 100.5)
    assert fill.latency_ms >= 0.0


@pytest.mark.asyncio
async def test_short_maps_to_sell_and_partial_fill_is_flagged() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return broker_response(request, filled_qty=60.0, filled_avg_price=99.8)

    gateway = make_gateway(handler)
    fill = await gateway.execute_order(SignalAction.SHORT, 10_000.0, "AAPL", 100.0)
    assert fill.status is OrderStatus.PARTIALLY_FILLED  # 60 of 100 units
    assert fill.filled_size == pytest.approx(60 * 99.8)


# --- broker failure modes ------------------------------------------------------


@pytest.mark.asyncio
async def test_broker_timeout_is_captured_as_gateway_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("broker took too long", request=request)

    gateway = make_gateway(handler)
    with pytest.raises(GatewayError, match="timed out"):
        await gateway.execute_order(SignalAction.BUY, 1_000.0, "AAPL", 100.0)


@pytest.mark.asyncio
async def test_gateway_504_rejection_raises_gateway_error() -> None:
    gateway = make_gateway(lambda request: httpx.Response(504, text="upstream broker timeout"))
    with pytest.raises(GatewayError, match="HTTP 504"):
        await gateway.execute_order(SignalAction.BUY, 1_000.0, "AAPL", 100.0)


@pytest.mark.asyncio
async def test_invalid_credentials_raise_config_error() -> None:
    gateway = make_gateway(lambda request: httpx.Response(401, json={"message": "unauthorized"}))
    with pytest.raises(GatewayConfigError, match="credentials"):
        await gateway.execute_order(SignalAction.BUY, 1_000.0, "AAPL", 100.0)


@pytest.mark.asyncio
async def test_failed_entry_never_halts_the_platform() -> None:
    """An entry that can't route is safe — no position, no exposure, no lock."""
    risk_manager = RiskManager()
    gateway = make_gateway(lambda request: httpx.Response(504), risk_manager=risk_manager)
    with pytest.raises(GatewayError):
        await gateway.execute_order(SignalAction.BUY, 1_000.0, "AAPL", 100.0)
    assert risk_manager.status()["halted"] is False


# --- exit-order retry + platform lock -----------------------------------------


@pytest.mark.asyncio
async def test_exit_retries_transient_failures_then_succeeds_without_halt() -> None:
    attempts: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        attempts.append(1)
        if len(attempts) < 3:
            return httpx.Response(504, text="flaky broker")
        return broker_response(request)

    risk_manager = RiskManager()
    gateway = make_gateway(handler, risk_manager=risk_manager, exit_retry_attempts=3)
    fill = await gateway.execute_order(SignalAction.SELL, 1_000.0, "AAPL", 100.0, is_exit=True)

    assert len(attempts) == 3  # two failures absorbed, third attempt landed
    assert fill.status is OrderStatus.FILLED
    assert risk_manager.status()["halted"] is False


@pytest.mark.asyncio
async def test_exit_total_failure_halts_platform_via_risk_manager() -> None:
    attempts: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        attempts.append(1)
        return httpx.Response(504, text="broker down")

    risk_manager = RiskManager()
    gateway = make_gateway(handler, risk_manager=risk_manager, exit_retry_attempts=3)
    with pytest.raises(GatewayError, match="platform halted"):
        await gateway.execute_order(SignalAction.SELL, 1_000.0, "AAPL", 100.0, is_exit=True)

    assert len(attempts) == 3  # every retry was actually attempted
    status = risk_manager.status()
    assert status["halted"] is True
    assert "live_gateway" in str(status["halted_reason"])
    assert "3 attempts" in str(status["halted_reason"])


@pytest.mark.asyncio
async def test_exit_credential_rejection_halts_immediately_without_retries() -> None:
    attempts: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        attempts.append(1)
        return httpx.Response(403, json={"message": "forbidden"})

    risk_manager = RiskManager()
    gateway = make_gateway(handler, risk_manager=risk_manager, exit_retry_attempts=3)
    with pytest.raises(GatewayConfigError):
        await gateway.execute_order(SignalAction.SELL, 1_000.0, "AAPL", 100.0, is_exit=True)

    assert len(attempts) == 1  # a dead key won't heal — no retry spin
    assert risk_manager.status()["halted"] is True


# --- risk guard parity with the mock gateway -----------------------------------


@pytest.mark.asyncio
async def test_entries_register_with_guard_and_round_trip_books_pnl() -> None:
    prices = iter([100.0, 90.0])  # buy at 100, forced to sell at 90

    def handler(request: httpx.Request) -> httpx.Response:
        return broker_response(request, filled_avg_price=next(prices))

    guard = RiskGuard(total_capital=100_000.0, max_daily_loss_pct=0.03, max_daily_trade_count=10)
    gateway = make_gateway(handler, risk_guard=guard)

    entry = await gateway.execute_order(SignalAction.BUY, 10_000.0, "AAPL", 100.0)
    assert guard.status()["daily_entry_count"] == 1
    assert list(gateway._open_orders) == [entry.order_id]  # local broker-book mirror

    await gateway.execute_order(SignalAction.SELL, 10_000.0, "AAPL", 90.0, is_exit=True)
    assert gateway._open_orders == {}  # round trip closed the mirrored entry
    # -10% move on $10k filled notional = -$1000 realized (zero fees), well
    # inside the 3%-of-$100k budget, so the guard stays unlocked.
    assert guard.status()["daily_realized_pnl"] == pytest.approx(-1_000.0)
    assert guard.locked is False


@pytest.mark.asyncio
async def test_guard_trip_blocks_further_live_entries() -> None:
    gateway = make_gateway(broker_response)
    guard = RiskGuard(total_capital=100_000.0, max_daily_loss_pct=0.03, max_daily_trade_count=1)
    gateway.risk_guard = guard

    await gateway.execute_order(SignalAction.BUY, 1_000.0, "AAPL", 100.0)
    with pytest.raises(RiskGuardTripped):
        await gateway.execute_order(SignalAction.BUY, 1_000.0, "AAPL", 100.0)
    # ...but the exit that flattens the book still passes
    fill = await gateway.execute_order(SignalAction.SELL, 1_000.0, "AAPL", 100.0, is_exit=True)
    assert fill.status is OrderStatus.FILLED


# --- open orders ---------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_open_orders_maps_broker_rows() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["status"] == "open"
        assert request.url.params["symbols"] == "AAPL"
        return httpx.Response(
            200,
            json=[
                {
                    "id": "open-1",
                    "symbol": "AAPL",
                    "side": "buy",
                    "qty": "10",
                    "filled_qty": "4",
                    "limit_price": "100.0",
                }
            ],
        )

    gateway = make_gateway(handler)
    orders = await gateway.fetch_open_orders("AAPL")
    assert len(orders) == 1
    assert orders[0].order_id == "open-1"
    assert orders[0].signal_type is SignalAction.BUY
    assert orders[0].requested_size == pytest.approx(1_000.0)
    assert orders[0].filled_size == pytest.approx(400.0)


# --- the PROD_LIVE structural double lock --------------------------------------


def test_prod_live_without_ack_flag_refuses_to_boot(monkeypatch: pytest.MonkeyPatch) -> None:
    from backend.main import create_app

    monkeypatch.setenv("GATEWAY_MODE", "PROD_LIVE")
    monkeypatch.delenv("I_AM_RISKING_REAL_MONEY", raising=False)
    monkeypatch.setenv("LIVE_BROKER_API_KEY", "k")
    monkeypatch.setenv("LIVE_BROKER_SECRET", "s")
    with pytest.raises(GatewayConfigError, match="I_AM_RISKING_REAL_MONEY"):
        create_app("sqlite+aiosqlite:///:memory:")


def test_prod_live_with_wrong_ack_value_still_refuses(monkeypatch: pytest.MonkeyPatch) -> None:
    from backend.main import create_app

    monkeypatch.setenv("GATEWAY_MODE", "PROD_LIVE")
    monkeypatch.setenv("I_AM_RISKING_REAL_MONEY", "yes")  # must be exactly TRUE
    monkeypatch.setenv("LIVE_BROKER_API_KEY", "k")
    monkeypatch.setenv("LIVE_BROKER_SECRET", "s")
    with pytest.raises(GatewayConfigError, match="I_AM_RISKING_REAL_MONEY"):
        create_app("sqlite+aiosqlite:///:memory:")


def test_prod_live_with_both_locks_arms_the_live_gateway(monkeypatch: pytest.MonkeyPatch) -> None:
    from backend.main import _build_gateway

    monkeypatch.setenv("GATEWAY_MODE", "PROD_LIVE")
    monkeypatch.setenv("I_AM_RISKING_REAL_MONEY", "TRUE")
    monkeypatch.setenv("LIVE_BROKER_API_KEY", "k")
    monkeypatch.setenv("LIVE_BROKER_SECRET", "s")
    monkeypatch.setenv("LIVE_BROKER_URL", "https://broker.example.test")
    risk_manager = RiskManager()
    built = _build_gateway(risk_manager)
    assert isinstance(built, LiveExecutionGateway)
    assert built.broker_url == "https://broker.example.test"
    assert built.risk_manager is risk_manager  # exit-failure halt path is armed


def test_telemetry_reports_prod_live_gateway_metadata() -> None:
    """App-level: an armed live gateway surfaces provider metadata (but never
    credentials) through /api/telemetry for the dashboard's settings modal."""
    from fastapi.testclient import TestClient

    from backend.main import create_app

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":  # boot reconciliation: no broker-side orders
            return httpx.Response(200, json=[])
        return broker_response(request)

    gateway = make_gateway(handler)
    app = create_app("sqlite+aiosqlite:///:memory:", gateway=gateway)
    with TestClient(app) as client:
        stats = client.get("/api/telemetry").json()
        assert stats["gateway"]["name"] == "PROD_LIVE"
        assert stats["gateway"]["mode"] == "prod_live"
        assert stats["gateway"]["provider"] == "Alpaca-blueprint REST brokerage"
        assert stats["gateway"]["metadata"]["broker_url"] == "https://broker.example.test"
        dumped = json.dumps(stats["gateway"])
        assert "test-key" not in dumped and "test-secret" not in dumped


def test_mock_default_is_unaffected_by_ack_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    """The ack flag alone must never promote the gateway — paper trading stays
    the default posture unless GATEWAY_MODE itself is deliberately changed."""
    from backend.main import _build_gateway

    monkeypatch.delenv("GATEWAY_MODE", raising=False)
    monkeypatch.setenv("I_AM_RISKING_REAL_MONEY", "TRUE")
    from backend.execution_gateway import MockExecutionGateway

    assert isinstance(_build_gateway(), MockExecutionGateway)
