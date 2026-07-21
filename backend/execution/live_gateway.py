"""Production live-brokerage execution gateway (Phase 8).

`LiveExecutionGateway` maps the internal order shape onto a REST brokerage
following the Alpaca API blueprint (`POST /v2/orders` with
``APCA-API-KEY-ID`` / ``APCA-API-SECRET-KEY`` headers), configured through the
``LIVE_BROKER_API_KEY`` / ``LIVE_BROKER_SECRET`` / ``LIVE_BROKER_URL``
environment variables.

Safety posture:

- This gateway is only ever armed by the composition root behind a structural
  double lock: ``GATEWAY_MODE=PROD_LIVE`` **and** ``I_AM_RISKING_REAL_MONEY=TRUE``
  (see `backend/main.py`). It never self-selects.
- Entry orders fail fast: a broker timeout or rejection raises `GatewayError`
  and the engine simply doesn't get its position — no capital is at risk.
- EXIT orders are the dangerous direction (failing to flatten leaves real
  exposure), so they retry with backoff; if the broker still won't take the
  exit, the gateway calls `RiskManager.halt()` to lock the whole platform and
  then raises. An operator must intervene before anything else trades.
- The same `RiskGuard` enforcement and local open-order/realized-PnL
  bookkeeping as the mock gateway applies, so daily loss limits bind
  identically in production.
"""

from __future__ import annotations

import asyncio
import itertools
import os
import time
from datetime import datetime, timezone
from typing import Any

import httpx

from backend.execution_gateway import BaseExecutionGateway, GatewayConfigError, GatewayError
from backend.models import OrderFill, OrderStatus, SignalAction
from backend.risk_manager import RiskManager

DEFAULT_BROKER_URL = "https://paper-api.alpaca.markets"
DEFAULT_EXIT_RETRY_ATTEMPTS = 3
DEFAULT_EXIT_RETRY_BACKOFF_SECONDS = 0.5

_FILLED_STATUSES = {"filled"}


class LiveExecutionGateway(BaseExecutionGateway):
    """Routes orders to a real REST brokerage (Alpaca-blueprint API)."""

    def __init__(
        self,
        api_key: str | None = None,
        api_secret: str | None = None,
        broker_url: str | None = None,
        *,
        risk_manager: RiskManager | None = None,
        client: httpx.AsyncClient | None = None,
        timeout_seconds: float = 10.0,
        fee_rate: float = 0.0,
        exit_retry_attempts: int = DEFAULT_EXIT_RETRY_ATTEMPTS,
        exit_retry_backoff_seconds: float = DEFAULT_EXIT_RETRY_BACKOFF_SECONDS,
    ) -> None:
        super().__init__()
        key = api_key or os.environ.get("LIVE_BROKER_API_KEY")
        secret = api_secret or os.environ.get("LIVE_BROKER_SECRET")
        if not key or not secret:
            raise GatewayConfigError(
                "LiveExecutionGateway requires LIVE_BROKER_API_KEY and "
                "LIVE_BROKER_SECRET (constructor args or environment variables)"
            )
        self.broker_url = (broker_url or os.environ.get("LIVE_BROKER_URL") or DEFAULT_BROKER_URL).rstrip("/")
        self.risk_manager = risk_manager
        self.fee_rate = fee_rate
        self.exit_retry_attempts = max(1, exit_retry_attempts)
        self.exit_retry_backoff_seconds = exit_retry_backoff_seconds
        self._headers = {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret}
        self._client = client or httpx.AsyncClient(timeout=timeout_seconds)
        self._fallback_ids = itertools.count(1)
        # Local mirror of broker-side entries: drives RiskGuard entry counting
        # and round-trip realized-PnL booking, exactly like the mock gateway.
        self._open_orders: dict[str, OrderFill] = {}

    @property
    def name(self) -> str:
        return "PROD_LIVE"

    # --- broker payload mapping ------------------------------------------------

    @staticmethod
    def broker_symbol(ticker: str) -> str:
        """Internal ticker -> broker symbol (Alpaca uses `BTCUSD`, not `BTC/USD`)."""
        return ticker.replace("/", "").replace("-", "").upper()

    def order_payload(
        self, signal_type: SignalAction, size: float, ticker: str, requested_price: float
    ) -> dict[str, Any]:
        """Maps the internal order shape onto the external broker's contract."""
        side = "buy" if self.adverse_direction(signal_type) > 0 else "sell"
        qty = size / requested_price  # notional dollars -> asset units
        return {
            "symbol": self.broker_symbol(ticker),
            "qty": f"{qty:.9f}".rstrip("0").rstrip("."),
            "side": side,
            "type": "market",
            "time_in_force": "gtc",
        }

    async def _post_order(self, payload: dict[str, Any]) -> dict[str, Any]:
        """One order submission attempt, with network failures captured into
        typed gateway errors instead of leaking transport exceptions."""
        try:
            response = await self._client.post(
                f"{self.broker_url}/v2/orders", json=payload, headers=self._headers
            )
        except httpx.TimeoutException as exc:
            raise GatewayError(f"broker timed out routing order: {exc}") from exc
        except httpx.HTTPError as exc:
            raise GatewayError(f"broker unreachable: {exc}") from exc
        if response.status_code in (401, 403):
            raise GatewayConfigError(
                f"broker rejected credentials (HTTP {response.status_code}) — "
                "check LIVE_BROKER_API_KEY / LIVE_BROKER_SECRET"
            )
        if response.status_code >= 400:
            raise GatewayError(f"broker rejected order (HTTP {response.status_code}): {response.text}")
        body: Any = response.json()
        if not isinstance(body, dict):
            raise GatewayError(f"malformed broker response: {body!r}")
        return body

    def _fill_from_response(
        self,
        raw: dict[str, Any],
        signal_type: SignalAction,
        size: float,
        ticker: str,
        requested_price: float,
        latency_ms: float,
    ) -> OrderFill:
        requested_qty = size / requested_price
        filled_qty = float(raw.get("filled_qty") or requested_qty)
        filled_price = float(raw.get("filled_avg_price") or requested_price)
        filled_size = filled_qty * filled_price
        broker_status = str(raw.get("status") or "filled").lower()
        status = (
            OrderStatus.FILLED
            if broker_status in _FILLED_STATUSES and filled_qty >= requested_qty * 0.999
            else OrderStatus.PARTIALLY_FILLED
        )
        return OrderFill(
            order_id=str(raw.get("id") or f"live-{next(self._fallback_ids)}"),
            ticker=ticker,
            signal_type=signal_type,
            requested_size=size,
            filled_size=round(filled_size, 6),
            requested_price=requested_price,
            filled_price=filled_price,
            fees=round(abs(filled_size) * self.fee_rate, 6),
            slippage_cost=self.slippage_cost(requested_price, filled_price, filled_size),
            status=status,
            latency_ms=round(latency_ms, 3),
            timestamp=datetime.now(timezone.utc),
        )

    # --- order routing ----------------------------------------------------------

    async def execute_order(
        self,
        signal_type: SignalAction,
        size: float,
        ticker: str,
        requested_price: float,
        *,
        is_exit: bool = False,
    ) -> OrderFill:
        if size <= 0 or requested_price <= 0:
            raise GatewayError("order size and requested price must be positive")
        self.enforce_risk_guard(signal_type, is_exit)
        payload = self.order_payload(signal_type, size, ticker, requested_price)

        started = time.perf_counter()
        if is_exit:
            raw = await self._submit_exit_with_retries(payload)
        else:
            raw = await self._post_order(payload)  # entries fail fast: no fill, no risk
        latency_ms = (time.perf_counter() - started) * 1000.0

        fill = self._fill_from_response(raw, signal_type, size, ticker, requested_price, latency_ms)
        self._book_fill(fill, is_exit=is_exit)
        return fill

    async def _submit_exit_with_retries(self, payload: dict[str, Any]) -> dict[str, Any]:
        """EXIT orders must land: retry transient broker failures with backoff,
        and lock the whole platform via `RiskManager.halt()` if they never do.
        Credential rejections don't retry — a dead key won't heal mid-loop."""
        last_error: GatewayError | None = None
        for attempt in range(1, self.exit_retry_attempts + 1):
            try:
                return await self._post_order(payload)
            except GatewayConfigError as exc:
                self._halt_platform(f"exit order rejected by broker credentials: {exc}")
                raise
            except GatewayError as exc:
                last_error = exc
                if attempt < self.exit_retry_attempts:
                    await asyncio.sleep(self.exit_retry_backoff_seconds * attempt)
        self._halt_platform(
            f"exit order failed after {self.exit_retry_attempts} attempts: {last_error}"
        )
        raise GatewayError(
            f"exit order failed after {self.exit_retry_attempts} attempts — "
            f"platform halted: {last_error}"
        )

    def _halt_platform(self, reason: str) -> None:
        if self.risk_manager is not None:
            self.risk_manager.halt(f"live_gateway: {reason}")

    def _book_fill(self, fill: OrderFill, *, is_exit: bool) -> None:
        """Mirrors the broker book locally and keeps the RiskGuard's daily
        entry count / realized PnL identical to the mock gateway's semantics."""
        if fill.signal_type in (SignalAction.BUY, SignalAction.SHORT) and not is_exit:
            self._open_orders[fill.order_id] = fill
            if self.risk_guard is not None:
                self.risk_guard.register_entry(fill.timestamp)
            return
        for order_id, open_fill in list(self._open_orders.items()):
            if open_fill.ticker == fill.ticker:
                del self._open_orders[order_id]
                if self.risk_guard is not None:
                    entry_direction = 1.0 if open_fill.signal_type is SignalAction.BUY else -1.0
                    pct_move = (
                        entry_direction
                        * (fill.filled_price - open_fill.filled_price)
                        / open_fill.filled_price
                    )
                    realized = pct_move * open_fill.filled_size - open_fill.fees - fill.fees
                    self.risk_guard.record_realized_pnl(realized, fill.timestamp)
                break

    async def fetch_open_orders(self, ticker: str | None = None) -> list[OrderFill]:
        params: dict[str, str] = {"status": "open"}
        if ticker is not None:
            params["symbols"] = self.broker_symbol(ticker)
        try:
            response = await self._client.get(
                f"{self.broker_url}/v2/orders", params=params, headers=self._headers
            )
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise GatewayError(f"broker rejected open-orders query: {exc}") from exc
        except httpx.HTTPError as exc:
            raise GatewayError(f"broker unreachable listing open orders: {exc}") from exc
        rows: Any = response.json()
        if not isinstance(rows, list):
            raise GatewayError(f"malformed broker open-orders response: {rows!r}")

        fills: list[OrderFill] = []
        for raw in rows:
            price = float(raw.get("limit_price") or raw.get("filled_avg_price") or 0.0)
            qty = float(raw.get("qty") or 0.0)
            side = str(raw.get("side") or "buy").lower()
            fills.append(
                OrderFill(
                    order_id=str(raw.get("id") or ""),
                    ticker=str(raw.get("symbol") or (ticker or "")),
                    signal_type=SignalAction.BUY if side == "buy" else SignalAction.SHORT,
                    requested_size=qty * price,
                    filled_size=float(raw.get("filled_qty") or 0.0) * price,
                    requested_price=price,
                    filled_price=price,
                    fees=0.0,
                    slippage_cost=0.0,
                    status=OrderStatus.PARTIALLY_FILLED,
                    latency_ms=0.0,
                    timestamp=datetime.now(timezone.utc),
                )
            )
        return fills

    def describe(self) -> dict[str, Any]:
        """Provider metadata for the dashboard's execution-settings modal.
        Never includes credentials."""
        return {
            "provider": "Alpaca-blueprint REST brokerage",
            "broker_url": self.broker_url,
            "exit_retry_attempts": self.exit_retry_attempts,
            "risk_manager_attached": self.risk_manager is not None,
        }

    async def aclose(self) -> None:
        await self._client.aclose()
