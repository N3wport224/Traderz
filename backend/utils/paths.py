"""Runtime path resolution for both development checkouts and the packaged
Windows executable (Phase 9).

Two very different worlds share this codebase:

- **Development**: the repo is the working directory; the SQLite file, the JSON
  log, and the built frontend all live inside the checkout.
- **Frozen** (PyInstaller): `sys.frozen` is set, read-only bundled resources
  (the compiled frontend, `.env.example`) live in the one-file bundle's
  extraction dir `sys._MEIPASS`, and NOTHING may be written next to them — the
  extraction dir is temporary and the install location (e.g. Program Files)
  may be read-only. All mutable state (database, logs) must therefore live in
  the user's per-OS application-data directory, which is writable on any
  machine without elevation.

Every function here is pure path math + `mkdir` — no trading logic — so the
rest of the backend simply asks "where does X live?" and never re-implements
frozen checks.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Product name used for the per-user application-data folder
# (e.g. C:\Users\<name>\AppData\Roaming\Traderz on Windows).
APP_NAME = "Traderz"

# Directory name the PyInstaller spec bundles the compiled frontend under
# (see `trading_platform.spec`: datas=[("frontend/out", "frontend_dist")]).
FRONTEND_BUNDLE_DIR = "frontend_dist"


def is_frozen() -> bool:
    """True when running as a PyInstaller-compiled executable."""
    return bool(getattr(sys, "frozen", False))


def repo_root() -> Path:
    """The checkout root in development (backend/utils/paths.py -> repo)."""
    return Path(__file__).resolve().parents[2]


def bundle_root() -> Path:
    """Root of read-only bundled resources.

    Frozen: PyInstaller's one-file bootloader extracts the archive to a temp
    dir and records it in `sys._MEIPASS`. Development: the repo root plays the
    same role, so `resource_path()` works identically in both worlds.
    """
    if is_frozen():
        return Path(getattr(sys, "_MEIPASS", Path(sys.executable).resolve().parent))
    return repo_root()


def resource_path(relative: str) -> Path:
    """Absolute path of a read-only bundled resource (frontend, templates)."""
    return bundle_root() / relative


def executable_dir() -> Path:
    """Directory the user launched from — where the `.exe` (and its `.env`)
    sits when frozen, the repo root in development. User-editable config
    belongs here; mutable state does not (it may be read-only)."""
    if is_frozen():
        return Path(sys.executable).resolve().parent
    return repo_root()


def app_data_dir() -> Path:
    """Per-user writable application-data directory, created on first use.

    Windows: %APPDATA%\\Traderz  (AppData/Roaming — survives reboots, roams
    with the profile, and is always writable without admin rights).
    macOS:   ~/Library/Application Support/Traderz
    Linux:   $XDG_DATA_HOME/Traderz  (default ~/.local/share/Traderz)
    """
    if sys.platform == "win32":
        base = Path(os.environ.get("APPDATA") or (Path.home() / "AppData" / "Roaming"))
    elif sys.platform == "darwin":
        base = Path.home() / "Library" / "Application Support"
    else:
        base = Path(os.environ.get("XDG_DATA_HOME") or (Path.home() / ".local" / "share"))
    target = base / APP_NAME
    target.mkdir(parents=True, exist_ok=True)
    return target


def default_database_url() -> str:
    """SQLite URL for the default (no DATABASE_URL) configuration.

    Frozen builds must never write inside the bundle/install dir, so the DB
    file is redirected into the app-data folder. `as_posix()` keeps the URL
    forward-slashed — SQLAlchemy URLs are URLs even on Windows.
    """
    if is_frozen():
        return f"sqlite+aiosqlite:///{(app_data_dir() / 'traderz.db').as_posix()}"
    return "sqlite+aiosqlite:///./traderz.db"


def default_json_log_path() -> Path:
    """Structured JSON-lines log destination (LOG_JSON_PATH overrides)."""
    if is_frozen():
        logs = app_data_dir() / "logs"
        logs.mkdir(parents=True, exist_ok=True)
        return logs / "logging.json"
    return Path("logging.json")


def frontend_dist_dir() -> Path:
    """Where the compiled static dashboard lives, if it has been built.

    Frozen: bundled into the executable under `frontend_dist/`.
    Development: `frontend/out` — the output of `next build` with
    `output: "export"`. Callers must check `.is_dir()`: a dev checkout that
    has never built the frontend simply runs API-only (the Next dev server
    on :3000 keeps working against it).
    """
    if is_frozen():
        return bundle_root() / FRONTEND_BUNDLE_DIR
    return repo_root() / "frontend" / "out"
