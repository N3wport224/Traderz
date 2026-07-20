# Traderz — Developer Rules

Multi-engine algorithmic trading system: a Day Trading (ORB/momentum) engine and a
Swing Trading (trendline) engine running concurrently behind a FastAPI backend, with
a Next.js dual-pane dashboard.

## Architecture

- `backend/` — Python (FastAPI, asyncio, pandas) core engine and API.
  - `backend/data_pipeline.py` — async mock OHLCV ingestion (1m and 4h streams).
  - `backend/strategies/momentum_engine.py` — Opening Range Breakout day-trading engine.
  - `backend/strategies/swing_engine.py` — trendline/pivot swing-trading engine.
  - `backend/main.py` — FastAPI app; runs both engines as concurrent background workers.
  - `backend/tests/` — all unit and system verification scripts.
- `frontend/` — Next.js + TypeScript + TailwindCSS dashboard.

## Non-negotiable rules

1. **Async only.** Every backend Python function that performs I/O, streams data, or
   participates in the engine loops must be `async def` and awaited. No blocking
   `time.sleep`, blocking network/file calls, or synchronous loops standing in for a
   stream — use `asyncio.sleep`, `async for`, and `async with`.
2. **Strict type-hinting everywhere.** All Python function signatures (params and
   return types) must be fully type-hinted; run `mypy`/`pyright`-clean code. All
   TypeScript must have `strict: true` in `tsconfig.json` — no implicit `any`.
3. **Tests must pass before any commit.** Run the backend test suite
   (`pytest backend/tests`) and it must be green before running `git commit`. Add or
   update a test alongside any behavioral change. Do not commit with failing or
   skipped-without-reason tests.
4. **Modular, decoupled design.** Strategy engines never import from each other or
   from `main.py`; they only depend on `backend/models.py` and `backend/data_pipeline.py`.
   The API layer composes the engines — it does not embed strategy logic.
