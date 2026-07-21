@echo off
setlocal enabledelayedexpansion
rem ===========================================================================
rem  Traderz — one-shot Windows executable build
rem ===========================================================================
rem  Produces dist\Traderz.exe: a single self-contained file bundling the
rem  FastAPI backend, both trading engines, and the compiled dashboard.
rem  End users just double-click the .exe — no Python, no Node, no terminal.
rem
rem  Build-machine prerequisites (end users need NONE of these):
rem    - Python 3.11+ on PATH  (python --version)
rem    - Node.js 20+ on PATH   (node --version)
rem
rem  Steps performed:
rem    [1/5] wash out old build caches so nothing stale leaks into the exe
rem    [2/5] install backend dependencies + PyInstaller
rem    [3/5] production-build the frontend into frontend\out (static export)
rem    [4/5] compile everything with PyInstaller (trading_platform.spec)
rem    [5/5] report the finished artifact
rem ===========================================================================

rem Always operate from the repo root (the folder holding this script), no
rem matter where the user launched it from.
cd /d "%~dp0"

echo.
echo === [1/5] Cleaning previous build artifacts ===============================
rem Old PyInstaller output and Next.js caches can mask real build errors and
rem sneak outdated assets into the bundle — always start from a clean slate.
if exist build       rmdir /s /q build
if exist dist        rmdir /s /q dist
if exist frontend\out          rmdir /s /q frontend\out
if exist frontend\.next        rmdir /s /q frontend\.next
for /d /r backend %%d in (__pycache__) do @if exist "%%d" rmdir /s /q "%%d"

echo.
echo === [2/5] Installing backend dependencies + PyInstaller ==================
python -m pip install --upgrade pip                          || goto :fail
python -m pip install -r backend\requirements.txt            || goto :fail
python -m pip install pyinstaller                            || goto :fail

echo.
echo === [3/5] Building the frontend static export ============================
rem `output: "export"` in next.config.ts turns `next build` into a plain
rem HTML/JS/CSS export in frontend\out — exactly what FastAPI serves at /
rem and what the spec file bundles into the executable as frontend_dist.
pushd frontend
call npm install                                             || goto :fail_popd
call npm run build                                           || goto :fail_popd
popd
if not exist frontend\out\index.html (
    echo [ERROR] frontend\out\index.html missing — the static export failed.
    goto :fail
)

echo.
echo === [4/5] Compiling the single-file executable ===========================
rem --clean:     discard PyInstaller's own caches (belt and braces with step 1)
rem --noconfirm: overwrite dist\ without prompting
python -m PyInstaller --noconfirm --clean trading_platform.spec || goto :fail

echo.
echo === [5/5] Done ===========================================================
if not exist dist\Traderz.exe (
    echo [ERROR] Expected dist\Traderz.exe was not produced.
    goto :fail
)
for %%A in (dist\Traderz.exe) do echo   Artifact : %%~fA  (%%~zA bytes)
echo   Ship the single file dist\Traderz.exe. On first double-click it
echo   creates a paper-trading .env next to itself, starts the platform on
echo   http://localhost:8000 and opens the user's browser automatically.
echo   User data (database, logs) lives in %%APPDATA%%\Traderz.
echo.
exit /b 0

:fail_popd
popd
:fail
echo.
echo [BUILD FAILED] See the output above for the first failing step.
exit /b 1
