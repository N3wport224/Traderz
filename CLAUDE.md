# Traderz — Developer Rules

Multi-engine algorithmic trading system: a Day Trading (ORB/momentum) engine and a
Swing Trading (trendline) engine running concurrently behind a FastAPI backend, with
a Next.js dual-pane dashboard. Phase 2 adds database persistence, a shared risk
manager (capital allocation + daily drawdown circuit breaker), live-mutable strategy
config, and a notification pipeline — all reached by the engines through injected
interfaces rather than direct imports (see rule 4).

## Architecture

- `backend/` — Python (FastAPI, asyncio, pandas, SQLAlchemy async) core engine and API.
  - `backend/data_pipeline.py` — async mock OHLCV ingestion (1m and 4h streams).
  - `backend/models.py` — shared dataclasses/enums plus the `TradePersistence`
    protocol and `NullPersistence` default; the one module every engine may import.
  - `backend/config.py` — `ConfigStore`: live-mutable momentum/swing strategy
    parameters, injected into engines and updated via `/api/config`.
  - `backend/risk_manager.py` — `RiskManager`: per-engine capital allocation caps,
    fee computation, and the cross-engine daily drawdown circuit breaker. One
    instance is shared by both engines so a combined loss can halt both.
  - `backend/db.py` — `Database`: async SQLAlchemy persistence (SQLite by default,
    Postgres via `DATABASE_URL`). Implements `TradePersistence`; owns the `Trade`
    and `EquitySnapshot` ORM models and the read-side query helpers the API uses.
  - `backend/notifier.py` — `Notifier` + pluggable `NotificationSink`s (console,
    Discord/Slack-compatible webhook); dispatches BUY/SHORT/CIRCUIT_BREAKER_TRIGGERED
    signals the instant they're produced.
  - `backend/strategies/momentum_engine.py` — Opening Range Breakout day-trading engine.
  - `backend/strategies/swing_engine.py` — trendline/pivot swing-trading engine.
  - `backend/main.py` — `create_app()` factory; the composition root that wires one
    `ConfigStore` + `RiskManager` + `Database` + `Notifier` into both engines' background
    workers and exposes the REST/WebSocket API. Tests call `create_app(...)` directly
    for isolated instances rather than importing a shared module-level app.
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
   skipped-without-reason tests. Prefer a paced (nonzero) tick interval over `0.0` in
   any test that drives a real background worker loop — an unthrottled loop can blow
   through hundreds of round-trips (and the daily circuit breaker) before the test's
   own polling ever observes the state it's trying to assert on.
4. **Modular, decoupled design.** Strategy engines never import each other,
   `backend/db.py`, or `backend/main.py`. They depend only on `backend/models.py`
   (including the `TradePersistence` protocol) plus the standalone
   `backend/config.py` (`ConfigStore`) and `backend/risk_manager.py` (`RiskManager`)
   collaborators, all **injected** at construction — never imported as global
   singletons. This is what keeps engines unit-testable with fakes/in-memory
   databases while still reading live config and writing through to a real database
   once `main.py` wires them up. The API layer composes the engines and their
   collaborators — it does not embed strategy logic.
5. **Bound any per-bar recomputation.** An engine that re-scans its own history on
   every bar (e.g. the swing engine's pivot/trendline search) must cap that history
   (see `SwingEngine.max_bar_history`) rather than let it grow unboundedly — an
   O(n) or worse per-bar cost over an ever-growing buffer becomes a real hang under
   a fast or unpaced bar stream, not just a theoretical concern.
