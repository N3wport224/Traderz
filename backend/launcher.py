"""Desktop launcher — the entry point compiled into the Windows executable.

Double-clicking the packaged `.exe` runs `main()` below, which:

1. Bootstraps configuration: if no `.env` sits next to the executable, one is
   generated automatically from the bundled `.env.example` template — loaded
   with paper-trading defaults (mock gateway, mock data, no credentials) so a
   user with zero terminal experience gets a safe, working install. The app
   NEVER crashes over a missing `.env`.
2. Loads that `.env` into the process environment (real environment variables
   always win, so power users can still override per-launch).
3. Starts the unified single-port server (FastAPI API + compiled dashboard,
   both on http://localhost:8000) via uvicorn.
4. Opens the user's default browser at the dashboard once the server is up,
   while the console window stays behind showing clean system-metric logs.

Everything here is also runnable from a dev checkout (`python -m
backend.launcher`) — the paths module transparently swaps bundle resources
for repo files.
"""

from __future__ import annotations

import logging
import os
import shutil
import sys
import threading
import webbrowser
from pathlib import Path

from backend.utils.paths import executable_dir, is_frozen, resource_path

logger = logging.getLogger("traderz.launcher")

DEFAULT_PORT = 8000

# Absolute-minimum safe configuration, used only if the bundled `.env.example`
# template is somehow missing from the build. Paper trading everywhere: mock
# fills, mock data, no credentials, conservative risk-guard limits.
FALLBACK_ENV_TEMPLATE = """\
# Traderz configuration — auto-generated with SAFE PAPER-TRADING defaults.
# Every value here keeps the platform in simulation: mock market data, mock
# order fills, no credentials, no real capital at risk. Edit deliberately.

DATA_SOURCE_MODE=mock
WATCHLIST_SYMBOL=MOCK
DATA_TRANSPORT=rest

# PAPER TRADING GUARD: leave as `mock` unless you know exactly what you are
# doing. Real-money execution additionally requires I_AM_RISKING_REAL_MONEY.
GATEWAY_MODE=mock
API_KEY=
API_SECRET=
LIVE_BROKER_API_KEY=
LIVE_BROKER_SECRET=
I_AM_RISKING_REAL_MONEY=

MAX_DAILY_LOSS_PCT=0.03
MAX_DAILY_TRADE_COUNT=50
CIRCUIT_BREAKER_ACTIVE=false
"""


def ensure_env_file(directory: Path) -> tuple[Path, bool]:
    """Guarantees a `.env` exists in `directory`; never raises over one missing.

    Returns `(path, created)`. A fresh file is copied from the bundled
    `.env.example` (placeholders only — safe paper-trading defaults) or, as a
    last resort, written from the embedded fallback template above.
    """
    env_path = directory / ".env"
    if env_path.exists():
        return env_path, False
    template = resource_path(".env.example")
    if template.is_file():
        shutil.copyfile(template, env_path)
    else:  # a build without the template still boots safely
        env_path.write_text(FALLBACK_ENV_TEMPLATE, encoding="utf-8")
    return env_path, True


def load_env_file(env_path: Path) -> dict[str, str]:
    """Minimal `.env` parser (KEY=VALUE lines; #-comments and blanks ignored).

    Values are applied with `setdefault`, so variables already present in the
    real environment always take precedence over the file. Returns only the
    variables this call actually introduced.
    """
    applied: dict[str, str] = {}
    if not env_path.is_file():
        return applied
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip("'\"")
        if not key or not value:  # blank placeholders stay unset
            continue
        if key not in os.environ:
            os.environ[key] = value
            applied[key] = value
    return applied


def _open_browser_when_ready(url: str, delay_seconds: float = 1.5) -> threading.Timer:
    """Schedules the default-browser launch shortly after uvicorn binds.

    A timer (not a blocking wait) keeps startup non-interactive: if no browser
    is available (headless box), `webbrowser.open` is simply a silent no-op.
    """
    timer = threading.Timer(delay_seconds, webbrowser.open, args=(url,))
    timer.daemon = True
    timer.start()
    return timer


def main() -> None:
    """Boot sequence for the packaged executable (and `python -m backend.launcher`)."""
    base_dir = executable_dir()
    env_path, created = ensure_env_file(base_dir)
    load_env_file(env_path)

    port = int(os.environ.get("PORT", str(DEFAULT_PORT)))
    url = f"http://localhost:{port}"

    # Friendly console banner — this window is the user's "server log".
    print("=" * 62)
    print("  Traderz — Multi-Engine Algorithmic Trading Platform")
    print("=" * 62)
    print(f"  Mode        : {'packaged executable' if is_frozen() else 'development checkout'}")
    print(f"  Config file : {env_path}{'  (auto-generated: paper trading)' if created else ''}")
    print(f"  Dashboard   : {url}")
    print("  Keep this window open — closing it stops the platform.")
    print("=" * 62)

    # Import AFTER the .env is loaded: create_app() reads GATEWAY_MODE etc.
    # from the environment at construction time.
    import uvicorn

    from backend.main import create_app

    app = create_app()
    _open_browser_when_ready(url)
    try:
        uvicorn.run(app, host="127.0.0.1", port=port, log_level="info")
    except Exception:  # keep the console readable instead of vanishing
        logger.exception("server crashed")
        if is_frozen():  # double-click launches close the window on exit
            input("\nThe server stopped unexpectedly. Press Enter to close...")
        raise


if __name__ == "__main__":
    main()
