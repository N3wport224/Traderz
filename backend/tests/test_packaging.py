"""Phase 9 packaging tests: frozen-vs-dev path resolution, the app-data
redirection for mutable state, the `.env` auto-bootstrap, and unified
single-port static serving of the compiled dashboard.

"Frozen" is simulated by monkeypatching `sys.frozen`/`sys._MEIPASS` — the
exact attributes the PyInstaller bootloader sets — so the logic under test is
the same code path the real executable takes.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from backend.launcher import ensure_env_file, load_env_file
from backend.main import create_app
from backend.utils import paths


def freeze(monkeypatch: pytest.MonkeyPatch, meipass: Path, exe_dir: Path) -> None:
    """Simulates running inside a PyInstaller one-file bundle."""
    monkeypatch.setattr(sys, "frozen", True, raising=False)
    monkeypatch.setattr(sys, "_MEIPASS", str(meipass), raising=False)
    monkeypatch.setattr(sys, "executable", str(exe_dir / "Traderz.exe"))


# --- path resolution -----------------------------------------------------------


def test_dev_checkout_paths_are_repo_relative() -> None:
    assert paths.is_frozen() is False
    assert paths.default_database_url() == "sqlite+aiosqlite:///./traderz.db"
    assert paths.default_json_log_path() == Path("logging.json")
    assert paths.frontend_dist_dir() == paths.repo_root() / "frontend" / "out"
    assert paths.executable_dir() == paths.repo_root()
    # bundled resources resolve against the repo in development
    assert paths.resource_path(".env.example").is_file()


def test_frozen_redirects_mutable_state_into_app_data(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    freeze(monkeypatch, tmp_path / "bundle", tmp_path / "install")
    if sys.platform == "win32":
        monkeypatch.setenv("APPDATA", str(tmp_path / "Roaming"))
        expected_base = tmp_path / "Roaming" / paths.APP_NAME
    elif sys.platform == "darwin":
        expected_base = Path.home() / "Library" / "Application Support" / paths.APP_NAME
    else:
        monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "xdg"))
        expected_base = tmp_path / "xdg" / paths.APP_NAME

    assert paths.is_frozen() is True
    assert paths.app_data_dir() == expected_base
    assert expected_base.is_dir()  # created with write permissions on first use

    # database + logs land in app data — never inside the (read-only) bundle
    assert paths.default_database_url() == (
        f"sqlite+aiosqlite:///{(expected_base / 'traderz.db').as_posix()}"
    )
    log_path = paths.default_json_log_path()
    assert log_path == expected_base / "logs" / "logging.json"
    assert log_path.parent.is_dir()


def test_frozen_resources_resolve_inside_meipass(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    bundle = tmp_path / "bundle"
    install = tmp_path / "install"
    freeze(monkeypatch, bundle, install)
    assert paths.bundle_root() == bundle
    assert paths.frontend_dist_dir() == bundle / paths.FRONTEND_BUNDLE_DIR
    assert paths.resource_path(".env.example") == bundle / ".env.example"
    # user-editable config sits next to the .exe, not in the temp bundle
    assert paths.executable_dir() == install


def test_frozen_database_actually_writes_to_app_data(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """End-to-end: a Database built with frozen defaults creates its SQLite
    file inside the app-data folder, not the working directory."""
    from backend.db import Database

    freeze(monkeypatch, tmp_path / "bundle", tmp_path / "install")
    if sys.platform == "win32":
        monkeypatch.setenv("APPDATA", str(tmp_path / "Roaming"))
    else:
        monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "xdg"))
    monkeypatch.delenv("DATABASE_URL", raising=False)

    database = Database()
    db_file = paths.app_data_dir() / "traderz.db"
    assert db_file.as_posix() in database.database_url

    import asyncio

    async def roundtrip() -> None:
        await database.init()
        assert await database.journal_mode() == "wal"  # real file => WAL applies
        await database.dispose()

    asyncio.run(roundtrip())
    assert db_file.exists()


# --- .env bootstrap ------------------------------------------------------------


def test_missing_env_is_generated_with_paper_trading_defaults(tmp_path: Path) -> None:
    env_path, created = ensure_env_file(tmp_path)
    assert created is True
    content = env_path.read_text(encoding="utf-8")
    # the generated template is the tracked .env.example: paper trading, no creds
    assert "GATEWAY_MODE=mock" in content
    assert "DATA_SOURCE_MODE=mock" in content
    assert "API_KEY=\n" in content  # placeholders stay blank
    assert "\nI_AM_RISKING_REAL_MONEY=\n" in content  # ack flag NOT pre-armed


def test_existing_env_is_never_overwritten(tmp_path: Path) -> None:
    (tmp_path / ".env").write_text("GATEWAY_MODE=mock\n# my precious config\n")
    env_path, created = ensure_env_file(tmp_path)
    assert created is False
    assert "my precious config" in env_path.read_text()


def test_fallback_template_when_bundle_lacks_example(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    freeze(monkeypatch, tmp_path / "empty_bundle", tmp_path / "install")
    (tmp_path / "install").mkdir()
    env_path, created = ensure_env_file(tmp_path / "install")
    assert created is True
    assert "GATEWAY_MODE=mock" in env_path.read_text()


def test_load_env_file_applies_values_but_real_env_wins(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text(
        "# comment\n"
        "\n"
        "GATEWAY_MODE=live\n"
        'WATCHLIST_SYMBOL="AAPL"\n'
        "API_KEY=\n"  # blank placeholder must NOT be exported
        "not a kv line\n"
    )
    monkeypatch.setenv("GATEWAY_MODE", "mock")  # real environment pre-set
    monkeypatch.delenv("WATCHLIST_SYMBOL", raising=False)
    monkeypatch.delenv("API_KEY", raising=False)

    applied = load_env_file(env_path)
    assert os.environ["GATEWAY_MODE"] == "mock"  # file never overrides
    assert os.environ["WATCHLIST_SYMBOL"] == "AAPL"  # quotes stripped
    assert "API_KEY" not in os.environ
    assert applied == {"WATCHLIST_SYMBOL": "AAPL"}
    monkeypatch.delenv("WATCHLIST_SYMBOL", raising=False)


def test_load_env_file_missing_file_is_a_noop(tmp_path: Path) -> None:
    assert load_env_file(tmp_path / "absent.env") == {}


# --- unified single-port static serving ----------------------------------------


def make_dist(tmp_path: Path) -> Path:
    dist = tmp_path / "out"
    dist.mkdir()
    (dist / "index.html").write_text("<html><body>Traderz dashboard</body></html>")
    (dist / "app.js").write_text("console.log('dashboard bundle')")
    return dist


def test_static_dashboard_served_from_root_with_api_unblocked(tmp_path: Path) -> None:
    app = create_app("sqlite+aiosqlite:///:memory:", static_dir=str(make_dist(tmp_path)))
    with TestClient(app) as client:
        # root serves the compiled dashboard (html=True resolves index.html)
        root = client.get("/")
        assert root.status_code == 200
        assert "Traderz dashboard" in root.text
        assert client.get("/app.js").status_code == 200

        # every API family still routes to FastAPI, not the static mount
        assert client.get("/api/health").json() == {"status": "ok"}
        assert client.get("/api/telemetry").status_code == 200
        assert isinstance(client.get("/api/momentum/signals").json(), list)
        with client.websocket_connect("/ws/momentum") as ws:
            ws.close()


def test_app_without_built_frontend_stays_api_only(tmp_path: Path) -> None:
    missing = tmp_path / "never_built"
    app = create_app("sqlite+aiosqlite:///:memory:", static_dir=str(missing))
    with TestClient(app) as client:
        assert client.get("/api/health").status_code == 200
        assert client.get("/").status_code == 404  # no mount registered


def test_frozen_app_serves_bundled_frontend_from_meipass(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The packaged flow end-to-end: dashboard inside _MEIPASS/frontend_dist,
    DB redirected to app data, API and static both on the one port."""
    bundle = tmp_path / "bundle"
    dist = bundle / paths.FRONTEND_BUNDLE_DIR
    dist.mkdir(parents=True)
    (dist / "index.html").write_text("<html><body>bundled dashboard</body></html>")
    freeze(monkeypatch, bundle, tmp_path / "install")

    app = create_app("sqlite+aiosqlite:///:memory:")  # static resolves via _MEIPASS
    with TestClient(app) as client:
        assert "bundled dashboard" in client.get("/").text
        assert client.get("/api/health").json() == {"status": "ok"}


def test_launcher_browser_timer_is_daemon_and_cancellable() -> None:
    from backend.launcher import _open_browser_when_ready

    timer = _open_browser_when_ready("http://localhost:8000", delay_seconds=30.0)
    try:
        assert timer.daemon is True  # never blocks interpreter shutdown
    finally:
        timer.cancel()
