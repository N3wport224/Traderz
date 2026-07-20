"""Smoke tests for the FastAPI app: boot, REST snapshots, and WebSocket connectivity."""

from __future__ import annotations

from fastapi.testclient import TestClient

from backend.main import app


def test_health_check() -> None:
    with TestClient(app) as client:
        response = client.get("/api/health")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}


def test_rest_snapshot_endpoints_return_lists() -> None:
    with TestClient(app) as client:
        for path in (
            "/api/momentum/signals",
            "/api/momentum/equity",
            "/api/swing/signals",
            "/api/swing/equity",
        ):
            response = client.get(path)
            assert response.status_code == 200
            assert isinstance(response.json(), list)


def test_websocket_channels_accept_connections() -> None:
    with TestClient(app) as client:
        with client.websocket_connect("/ws/momentum") as ws:
            ws.close()
        with client.websocket_connect("/ws/swing") as ws:
            ws.close()
