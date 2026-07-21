# -*- mode: python ; coding: utf-8 -*-
"""PyInstaller build configuration for the Traderz standalone executable.

Compiles the entire platform — FastAPI backend, trading engines, and the
pre-built static dashboard — into ONE self-contained Windows .exe with no
Python, Node.js, or terminal knowledge required from the end user.

Build it via `build_executable.bat` (which builds the frontend first), or by
hand:

    cd frontend && npm run build && cd ..
    pyinstaller --noconfirm --clean trading_platform.spec

Layout inside the one-file bundle (extracted to sys._MEIPASS at launch):
    frontend_dist/   <- the compiled Next.js static export (frontend/out)
    .env.example     <- template copied next to the .exe on first run
The entry point is backend/launcher.py: it bootstraps a paper-trading .env,
starts the single-port server, and opens the user's browser.
"""

from PyInstaller.utils.hooks import collect_submodules

APP_NAME = "Traderz"

# ---------------------------------------------------------------------------
# Hidden imports.
#
# PyInstaller discovers dependencies by scanning `import` statements, which
# misses everything loaded dynamically by string name at runtime:
#   - uvicorn picks its event loop / http protocol / websocket / lifespan
#     implementations from config strings ("uvicorn.loops.auto", ...);
#   - SQLAlchemy resolves the "sqlite+aiosqlite" dialect from the URL;
#   - the app's own engine/strategy modules are imported through the
#     composition root, which static analysis follows fine, but we list the
#     packages wholesale anyway so a future dynamic import can't silently
#     produce a broken build.
# ---------------------------------------------------------------------------
hiddenimports = (
    collect_submodules("uvicorn")
    + collect_submodules("websockets")
    + collect_submodules("aiosqlite")
    + [
        "sqlalchemy.dialects.sqlite",
        "sqlalchemy.dialects.sqlite.aiosqlite",
        "sqlalchemy.ext.asyncio",
        "greenlet",
        # pandas/numpy have official PyInstaller hooks; listing the top-level
        # packages is enough for their C extensions to be collected.
        "pandas",
        "numpy",
    ]
)

# ---------------------------------------------------------------------------
# Bundled data files: (source on disk, destination inside sys._MEIPASS).
#
#   frontend/out  -> frontend_dist   the compiled dashboard; served by
#                                    FastAPI at / (backend/utils/paths.py
#                                    resolves this exact folder name).
#   .env.example  -> .               template for the auto-generated .env
#                                    (paper-trading defaults) on first run.
# ---------------------------------------------------------------------------
datas = [
    ("frontend/out", "frontend_dist"),
    (".env.example", "."),
]

a = Analysis(
    ["backend/launcher.py"],          # double-click entry point
    pathex=["."],                     # repo root on the import path
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    # Never let developer/test tooling bloat the executable.
    excludes=["pytest", "pytest_asyncio", "pip", "setuptools", "tkinter"],
    noarchive=False,
    # Bytecode optimization level 1: drops asserts/docstrings-adjacent debug
    # weight without breaking libraries that introspect docstrings.
    optimize=1,
)

pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,                       # binaries+datas inside EXE = one-file mode
    a.datas,
    [],
    name=APP_NAME,
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,                      # strip breaks some Windows binaries; keep off
    upx=True,                         # compress with UPX when available (optional)
    upx_exclude=[],
    runtime_tmpdir=None,              # default %TEMP% extraction for one-file mode
    console=True,                     # keep the console: it IS the log window
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
