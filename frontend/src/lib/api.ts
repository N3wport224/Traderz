import type {
  BacktestReport,
  BacktestRequest,
  BracketCard,
  EngineChannel,
  EquityPoint,
  MomentumConfig,
  NotifierPingResult,
  RiskStatus,
  StrategyConfig,
  SwingConfig,
  TelemetryStats,
  Trade,
  TradeSignal,
  TraderEvent,
  TraderFeed,
  WatchedTrader,
  WatchlistState,
} from "./types";

/** Where the backend API lives.
 *
 * - NEXT_PUBLIC_API_BASE_URL, when set at build time, always wins (including
 *   an explicit empty string for same-origin serving).
 * - Otherwise, when the page is served by anything other than the Next dev
 *   server (port 3000) — i.e. the unified single-port build where FastAPI
 *   serves the static dashboard itself — use the page's own origin via
 *   relative URLs.
 * - The Next dev server on :3000 falls back to the local backend on :8000.
 */
function resolveApiBase(): string {
  const configured = process.env.NEXT_PUBLIC_API_BASE_URL;
  if (configured !== undefined) return configured;
  if (typeof window !== "undefined" && window.location.port !== "3000") return "";
  return "http://localhost:8000";
}

export const API_BASE_URL: string = resolveApiBase();

export function wsUrlFor(channel: EngineChannel): string {
  if (API_BASE_URL === "") {
    // Same-origin mode: derive the WebSocket endpoint from the page itself.
    const proto = window.location.protocol === "https:" ? "wss" : "ws";
    return `${proto}://${window.location.host}/ws/${channel}`;
  }
  const wsBase = API_BASE_URL.replace(/^http/, "ws");
  return `${wsBase}/ws/${channel}`;
}

async function getJson<T>(path: string): Promise<T> {
  const response = await fetch(`${API_BASE_URL}${path}`);
  if (!response.ok) {
    throw new Error(`Request to ${path} failed with status ${response.status}`);
  }
  return (await response.json()) as T;
}

async function putJson<TBody extends object, TResponse>(path: string, body: TBody): Promise<TResponse> {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!response.ok) {
    const detail = await response.text();
    throw new Error(`Request to ${path} failed with status ${response.status}: ${detail}`);
  }
  return (await response.json()) as TResponse;
}

async function postJson<TResponse>(path: string): Promise<TResponse> {
  const response = await fetch(`${API_BASE_URL}${path}`, { method: "POST" });
  if (!response.ok) {
    throw new Error(`Request to ${path} failed with status ${response.status}`);
  }
  return (await response.json()) as TResponse;
}

export async function fetchSignals(channel: EngineChannel): Promise<TradeSignal[]> {
  return getJson<TradeSignal[]>(`/api/${channel}/signals`);
}

export async function fetchEquityCurve(channel: EngineChannel): Promise<EquityPoint[]> {
  return getJson<EquityPoint[]>(`/api/${channel}/equity`);
}

export async function fetchTrades(channel: EngineChannel): Promise<Trade[]> {
  return getJson<Trade[]>(`/api/${channel}/trades`);
}

export async function fetchConfig(): Promise<StrategyConfig> {
  return getJson<StrategyConfig>("/api/config");
}

export async function updateMomentumConfig(update: Partial<MomentumConfig>): Promise<MomentumConfig> {
  return putJson<Partial<MomentumConfig>, MomentumConfig>("/api/config/momentum", update);
}

export async function updateSwingConfig(update: Partial<SwingConfig>): Promise<SwingConfig> {
  return putJson<Partial<SwingConfig>, SwingConfig>("/api/config/swing", update);
}

export async function fetchRiskStatus(): Promise<RiskStatus> {
  return getJson<RiskStatus>("/api/risk/status");
}

export async function fetchTelemetry(): Promise<TelemetryStats> {
  return getJson<TelemetryStats>("/api/telemetry");
}

export async function fetchBrackets(): Promise<BracketCard[]> {
  return getJson<BracketCard[]>("/api/brackets");
}

export async function runBacktest(request: BacktestRequest): Promise<BacktestReport> {
  const response = await fetch(`${API_BASE_URL}/api/backtest`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(request),
  });
  if (!response.ok) {
    let detail = `status ${response.status}`;
    try {
      const body = (await response.json()) as { detail?: unknown };
      if (body.detail) detail = typeof body.detail === "string" ? body.detail : JSON.stringify(body.detail);
    } catch {
      // keep the status-based message
    }
    throw new Error(`Backtest failed: ${detail}`);
  }
  return (await response.json()) as BacktestReport;
}

export async function engageKillSwitch(): Promise<RiskStatus> {
  return postJson<RiskStatus>("/api/system/kill");
}

export async function resetRiskGuard(): Promise<RiskStatus> {
  return postJson<RiskStatus>("/api/system/guard/reset");
}

export async function fetchWatchlist(): Promise<WatchlistState> {
  return getJson<WatchlistState>("/api/watchlist");
}

export async function updateWatchlist(ticker: string): Promise<WatchlistState> {
  const response = await fetch(`${API_BASE_URL}/api/watchlist`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ ticker }),
  });
  if (!response.ok) {
    if (response.status === 422) {
      throw new Error("Invalid ticker — use a stock symbol (AAPL) or crypto pair (BTC/USDT)");
    }
    throw new Error(`Watchlist update failed with status ${response.status}`);
  }
  return (await response.json()) as WatchlistState;
}

export async function testNotifierWebhook(): Promise<NotifierPingResult> {
  return postJson<NotifierPingResult>("/api/system/notifier/test");
}

// --- Phase 10: copy trading ---------------------------------------------------

/** Shared JSON-body request helper that surfaces the API's `detail` message. */
async function sendJson<TResponse>(method: string, path: string, body?: object): Promise<TResponse> {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    method,
    headers: body !== undefined ? { "Content-Type": "application/json" } : undefined,
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });
  if (!response.ok) {
    let detail = `status ${response.status}`;
    try {
      const parsed = (await response.json()) as { detail?: unknown };
      if (parsed.detail) {
        detail = typeof parsed.detail === "string" ? parsed.detail : JSON.stringify(parsed.detail);
      }
    } catch {
      // keep the status-based message
    }
    throw new Error(detail);
  }
  return (await response.json()) as TResponse;
}

export async function fetchTraders(): Promise<WatchedTrader[]> {
  return getJson<WatchedTrader[]>("/api/traders");
}

export async function createTrader(name: string, assetClass: "stock" | "crypto"): Promise<WatchedTrader> {
  return sendJson<WatchedTrader>("POST", "/api/traders", { name, asset_class: assetClass });
}

export async function deleteTrader(id: number): Promise<void> {
  await sendJson<{ deleted: number }>("DELETE", `/api/traders/${id}`);
}

export async function updateTraderFollow(
  id: number,
  autoFollow: boolean,
  budgetAmount: number,
): Promise<WatchedTrader> {
  return sendJson<WatchedTrader>("PUT", `/api/traders/${id}/follow`, {
    auto_follow: autoFollow,
    budget_amount: budgetAmount,
  });
}

export async function logTraderEvent(
  id: number,
  event: { ticker: string; action: "BUY" | "SELL"; price: number; note?: string },
): Promise<TraderEvent> {
  return sendJson<TraderEvent>("POST", `/api/traders/${id}/events`, event);
}

export async function fetchTraderFeed(limit = 50): Promise<TraderFeed> {
  return getJson<TraderFeed>(`/api/traders/feed?limit=${limit}`);
}

export async function pauseSystem(): Promise<RiskStatus> {
  return postJson<RiskStatus>("/api/system/pause");
}

export async function resumeSystem(): Promise<RiskStatus> {
  return postJson<RiskStatus>("/api/system/resume");
}
