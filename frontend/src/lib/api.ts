import type {
  EngineChannel,
  EquityPoint,
  MomentumConfig,
  RiskStatus,
  StrategyConfig,
  SwingConfig,
  TelemetryStats,
  Trade,
  TradeSignal,
} from "./types";

export const API_BASE_URL: string = process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://localhost:8000";

export function wsUrlFor(channel: EngineChannel): string {
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

export async function pauseSystem(): Promise<RiskStatus> {
  return postJson<RiskStatus>("/api/system/pause");
}

export async function resumeSystem(): Promise<RiskStatus> {
  return postJson<RiskStatus>("/api/system/resume");
}
