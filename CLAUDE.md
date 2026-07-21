# Traderz — Developer Rules

Multi-engine algorithmic trading system: a Day Trading (ORB/momentum) engine and a
Swing Trading (trendline) engine running concurrently behind a FastAPI backend, with
a Next.js dual-pane dashboard. Phase 2 adds database persistence, a shared risk
manager (capital allocation + daily drawdown circuit breaker), live-mutable strategy
config, and a notification pipeline — all reached by the engines through injected
interfaces rather than direct imports (see rule 4). Phase 3 adds production
resilience: a broker/exchange execution gateway (mock with simulated
slippage/latency/partial fills by default, CCXT live mode via `GATEWAY_MODE=live` +
`API_KEY`/`API_SECRET`), a reconnection state machine over the data streams with a
`DATA_DISCONNECTED` risk state, boot-time gateway-vs-DB reconciliation, structured
JSON logging with signal→approval→fill latency telemetry, and a frontend telemetry
bar + outage banner. Phase 4 adds live market data behind `DATA_SOURCE_MODE=live`
(stock tickers poll Yahoo Finance's public chart API; crypto pairs poll public
CCXT OHLCV — both credential-free) parsed into the same 1m/4h bar shapes, a
dashboard Asset Selector backed by `POST /api/watchlist` that dynamically resubscribes
both engines to any ticker, and a root `.env.example` documenting all configuration.
Live data and execution are deliberately independent: with the default
`GATEWAY_MODE=mock` the system is a true paper trader — real charts, simulated fills,
no real capital at risk. Phase 5 adds intelligent bracket orders: every entry runs
with a calculated stop-loss/take-profit pair (momentum: 1.5x/2.5x ATR(14) from
`backend/utils/indicators.py`; swing: 1% below the latest support-pivot wick / the
nearest peak-pivot resistance ceiling, with a >2%-profit trail to break-even). The
*gateway* owns bracket monitoring — engines register levels at entry, feed every
candle through `check_bracket`, and the gateway detects touches (pessimistic HIT_SL
when both levels sit inside one candle; gap-throughs fill at the open) and executes
the exit itself. Trades persist `stop_loss_price`/`take_profit_price`/`bracket_status`
(ACTIVE/HIT_SL/HIT_TP/TIME_EXITED), `/api/brackets` serves live bracket cards, and
the dashboard shows them in the Active Target Signals panel with distance-to-target
and risk-reward readouts. Phase 6 adds `backend/engine/backtester.py` — an
event-driven historical replay (`HistoricalTransport` from CSV/JSON/seeded
synthetic) that feeds the *unmodified* live engines + gateway + bracket monitor and
folds results into `BacktestResult` (total return %, win rate %, profit factor, max
peak-to-trough drawdown %), exposed via `POST /api/backtest` — and
`backend/utils/risk_guard.py`, an env-configured operational guard
(`MAX_DAILY_LOSS_PCT`, `MAX_DAILY_TRADE_COUNT`, `CIRCUIT_BREAKER_ACTIVE`) enforced
inside the gateway in front of every ENTRY (exits always pass; a trip halts the
`RiskManager` so engines flatten on the next bar). Orders now carry an `is_exit`
flag so short covers match the broker-side book instead of registering as entries;
the gateway books round-trip realized PnL into the guard from its own fill stream.
`POST /api/system/kill` is the emergency kill switch; `/api/system/guard/reset`
releases it; the dashboard has a Strategy Analytics & Backtesting tab and a
flashing RISK GUARD TRIPPED header badge. Phase 7 adds production state
persistence and streaming: file-backed SQLite runs in WAL mode
(`journal_mode=WAL`, `synchronous=NORMAL`, `busy_timeout`) with every repository
method inside the explicit `Database.get_db_session()` commit/rollback scope, so
concurrent live writes and analytics reads never lock; the `RiskGuard`
serializes its daily counters to the `system_state` table on every mutation
(persists are *chained* — never raced — because concurrent same-row upserts
flush partial field diffs) and reconstructs them at boot with zero amnesia (a
restored lock re-halts the `RiskManager`; `_roll_day` no longer clears a halt on
the first observed bar). `DATA_TRANSPORT=websocket` (live crypto 1m) switches
ingestion from REST polling to `WebSocketStreamFactory`: a resilient asyncio
message loop over a keyless public kline stream that parses closed candles into
`OHLCVBar`s and multiplexes them to subscribed engines via bounded per-subscriber
queues (drop-oldest; subscription queues register eagerly at `subscribe()`).
Telemetry exposes `database.journal_mode`, `risk_guard_sync` (memory-vs-DB
comparison), and websocket stream health/latency; the dashboard header is an
Infrastructure & Connectivity bar. Phase 8 adds the live brokerage gateway and
production alerting: `backend/execution/live_gateway.py` (`LiveExecutionGateway`)
maps orders onto an Alpaca-blueprint REST brokerage (`LIVE_BROKER_API_KEY` /
`LIVE_BROKER_SECRET` / `LIVE_BROKER_URL`), armed ONLY behind the structural
double lock `GATEWAY_MODE=PROD_LIVE` **and** `I_AM_RISKING_REAL_MONEY=TRUE`
(anything else refuses to boot). Entries fail fast on broker errors
(timeouts/5xx → `GatewayError`, 401/403 → `GatewayConfigError`); EXIT orders
retry with backoff and, on total failure, call `RiskManager.halt()` so the
platform locks rather than run with unflattened real exposure. RiskGuard
enforcement and the local open-order/realized-PnL mirror match the mock gateway
exactly. `backend/utils/notifier.py` (`SystemNotifier`, distinct from the
per-trade `backend/notifier.py`) pushes ALERT/INFO events to a Discord/Slack
webhook (`SYSTEM_WEBHOOK_URL`; unset → log-only) on risk-guard trips, kill-switch
presses, and engine boot; delivery is best-effort and never raises.
`POST /api/system/notifier/test` fires a connectivity ping and telemetry carries
notifier + gateway-provider metadata; the dashboard's System Health bar gains a
settings cog (execution-provider modal) and a webhook test button. All broker
and webhook tests run against `httpx.MockTransport` — never the real network.
Phase 9 adds standalone packaging: the dashboard compiles to a static export
(`next.config.ts` `output: "export"` → `frontend/out`) that `create_app` mounts
at `/` via `StaticFiles(html=True)` — registered after every route so `/api/*`
and `/ws/*` always win — while `frontend/src/lib/api.ts` auto-detects
same-origin serving (any page not on the :3000 dev server uses relative URLs
and page-origin WebSockets), unifying the whole app on one port with no Node
server in production. `backend/utils/paths.py` is the single authority on
runtime paths: under PyInstaller (`sys.frozen`) read-only resources resolve
from `sys._MEIPASS` and ALL mutable state (SQLite DB, JSON logs) redirects to
the per-user app-data dir (`%APPDATA%\Traderz`); dev checkouts stay
repo-relative. `backend/launcher.py` is the exe entry point: it auto-generates
a paper-trading `.env` from the bundled `.env.example` when none exists (never
crashes on a missing file; real env vars always win over file values), starts
uvicorn on :8000 (PORT overrides), and opens the default browser while the
console stays as the log window. `trading_platform.spec` +
`build_executable.bat` produce the one-file `dist\Traderz.exe` (spec bundles
`frontend/out` → `frontend_dist` and `.env.example`; batch script cleans
caches, builds the export, runs PyInstaller). Packaging tests simulate frozen
mode by monkeypatching `sys.frozen`/`sys._MEIPASS` — never skip them for lack
of a real Windows box.

## Architecture

- `backend/` — Python (FastAPI, asyncio, pandas, SQLAlchemy async) core engine and API.
  - `backend/data_pipeline.py` — OHLCV ingestion (1m and 4h streams): mock random-walk
    generators plus, behind `DATA_SOURCE_MODE=live`, real feeds — Yahoo Finance public
    chart polling for stocks (60m candles aggregated into aligned 4h buckets, partial
    bucket held back) and public CCXT OHLCV polling for crypto pairs; both parse into
    the standard `OHLCVBar` and raise `StreamDisconnected` on any transport/parse
    failure. `build_stream_factory` routes symbol+timeframe+mode to the right stream.
    `ResilientStream`: the reconnection state machine (CONNECTED → DISCONNECTED →
    RECONNECTING with 2s→64s exponential backoff → VERIFYING → CONNECTED) that wraps
    each stream (mock or live), alerts the notifier, and flags the ticker on the shared
    `RiskManager` until integrity is re-verified. Backoff resets only after a *verified*
    reconnect. Tests must never hit the real network — inject `httpx.MockTransport`
    clients / fake CCXT exchanges (see `backend/tests/test_live_data.py`).
  - `backend/models.py` — shared dataclasses/enums plus the injected protocols
    (`TradePersistence`, `ExecutionGateway`, `OrderFlowTelemetry`) and the
    `NullPersistence` default; the one module every engine may import freely.
  - `backend/config.py` — `ConfigStore`: live-mutable momentum/swing strategy
    parameters, injected into engines and updated via `/api/config`.
  - `backend/risk_manager.py` — `RiskManager`: per-engine capital allocation caps,
    fee computation, the cross-engine daily drawdown circuit breaker, and per-ticker
    `DATA_DISCONNECTED` tracking (blocks entries / freezes evaluation on broken
    tickers). One instance is shared by both engines so a combined loss can halt both.
  - `backend/execution_gateway.py` — `BaseExecutionGateway` ABC + implementations:
    `MockExecutionGateway` (simulated latency, fees, order-book-depth slippage
    0.05%–0.2%, partial fills past book depth, in-memory open-order book) and
    `LiveCCXTExecutionGateway` (real exchanges via lazily-imported CCXT, credentials
    from `API_KEY`/`API_SECRET`). Every engine order routes through a gateway; fills
    carry requested vs. actual price and a dollar `slippage_cost`.
  - `backend/db.py` — `Database`: async SQLAlchemy persistence (SQLite by default,
    Postgres via `DATABASE_URL`). Implements `TradePersistence`; owns the `Trade`
    (incl. `requested_price`/`actual_filled_price`/`slippage_cost`), `OpenPosition`,
    and `EquitySnapshot` ORM models and the read-side query helpers the API uses.
  - `backend/notifier.py` — `Notifier` + pluggable `NotificationSink`s (console,
    Discord/Slack-compatible webhook); dispatches BUY/SHORT/CIRCUIT_BREAKER_TRIGGERED
    and DATA_DISCONNECTED/DATA_RECONNECTED events the instant they're produced.
  - `backend/telemetry.py` — JSON-lines structured logging (`logging.json`; every
    record machine-parseable) and `TelemetryTracker`: per-order
    signal→risk-approval→gateway-fill latency (ms) plus cumulative slippage,
    aggregated for `/api/telemetry`.
  - `backend/reconciliation.py` — `reconcile_on_boot`: diffs the gateway's open
    orders against the `open_positions` table by order id after a crash/restart;
    heals rows missing locally, clears rows already closed at the broker.
  - `backend/execution/live_gateway.py` — `LiveExecutionGateway`: the Phase 8
    REST brokerage client (Alpaca-blueprint `POST /v2/orders`, `APCA-*` auth
    headers) with typed error capture, exit-order retries that halt the
    platform on total failure, and RiskGuard/book parity with the mock. Only
    `main.py`'s `_build_gateway` may arm it, behind the PROD_LIVE double lock.
  - `backend/utils/notifier.py` — `SystemNotifier`: the system-health alert
    channel (guard trips, kill switch, engine boot) to `SYSTEM_WEBHOOK_URL`;
    best-effort, never raises, bounded event history surfaced in telemetry.
  - `backend/utils/paths.py` — frozen-aware path authority: `sys._MEIPASS`
    resources vs. per-user app-data for mutable state (DB, logs). Any new
    file the backend reads or writes must resolve its location here.
  - `backend/launcher.py` — packaged-executable entry point: `.env`
    auto-bootstrap (paper-trading defaults), env loading (real env wins),
    single-port uvicorn boot, browser auto-open.
  - `backend/utils/indicators.py` — pure TA math over `OHLCVBar` sequences (pandas):
    `compute_atr` (rolling-14 True Range mean, `min_periods=1` so early-session
    estimates exist) and `nearest_resistance` (lowest peak-pivot ceiling above a price).
  - `backend/strategies/momentum_engine.py` — Opening Range Breakout day-trading engine.
  - `backend/strategies/swing_engine.py` — trendline/pivot swing-trading engine.
  - `backend/main.py` — `create_app()` factory; the composition root that wires one
    `ConfigStore` + `RiskManager` + `Database` + `Notifier` + `ExecutionGateway` +
    `TelemetryTracker` into both engines' background workers (streams wrapped in
    `ResilientStream`), runs boot reconciliation, and exposes the REST/WebSocket API.
    Tests call `create_app(...)` directly for isolated instances rather than
    importing a shared module-level app.
  - `backend/tests/` — all unit and system verification scripts. Tests asserting
    exact prices/PnL must inject a zero-slippage, zero-latency mock gateway (see
    `zero_slip_gateway` in `test_strategies.py`) — the default gateway's randomized
    slippage makes exact assertions flaky.
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
   (including the `TradePersistence` / `ExecutionGateway` / `OrderFlowTelemetry`
   protocols) plus the standalone `backend/config.py` (`ConfigStore`),
   `backend/risk_manager.py` (`RiskManager`), and `backend/execution_gateway.py`
   (default `MockExecutionGateway`) collaborators, all **injected** at
   construction — never imported as global singletons. This is what keeps engines
   unit-testable with fakes/in-memory databases while still reading live config and
   writing through to a real database once `main.py` wires them up. Engines never
   simulate their own fills — every entry/exit routes through the injected gateway
   and books PnL off the actual fill it returns. The API layer composes the engines
   and their collaborators — it does not embed strategy logic.
5. **Bound any per-bar recomputation.** An engine that re-scans its own history on
   every bar (e.g. the swing engine's pivot/trendline search) must cap that history
   (see `SwingEngine.max_bar_history`) rather than let it grow unboundedly — an
   O(n) or worse per-bar cost over an ever-growing buffer becomes a real hang under
   a fast or unpaced bar stream, not just a theoretical concern.
6. **No real credentials in git — ever.** Never commit `.env`, `.env.local`, or any
   file containing real API keys, exchange secrets, webhook URLs, or database
   passwords. The tracked `.env.example` is the only env file allowed in the repo
   and must contain placeholders/empty values only (`.gitignore` enforces this —
   do not weaken those rules). Real credentials live exclusively in untracked
   local files or the deployment environment. Relatedly, paper trading is the
   default posture: `GATEWAY_MODE` stays `mock` unless a human deliberately opts
   into live execution — code must never auto-promote the gateway to live because
   data happens to be live.
