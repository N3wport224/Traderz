import type { EngineChannel, EquityPoint, TradeSignal } from "./types";

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

export async function fetchSignals(channel: EngineChannel): Promise<TradeSignal[]> {
  return getJson<TradeSignal[]>(`/api/${channel}/signals`);
}

export async function fetchEquityCurve(channel: EngineChannel): Promise<EquityPoint[]> {
  return getJson<EquityPoint[]>(`/api/${channel}/equity`);
}
