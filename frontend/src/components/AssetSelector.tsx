"use client";

import { useState } from "react";

import type { DataSourceMode } from "@/lib/types";

// Mirrors the backend's WatchlistUpdate validation so obvious typos fail fast
// locally; the API remains the authority.
const TICKER_PATTERN = /^[A-Z0-9.\-]{1,15}(\/[A-Z0-9]{2,10})?$/;

interface AssetSelectorProps {
  activeTicker: string | null;
  dataSourceMode: DataSourceMode | null;
  pending: boolean;
  error: string | null;
  onSubmit: (ticker: string) => Promise<void>;
}

export default function AssetSelector({ activeTicker, dataSourceMode, pending, error, onSubmit }: AssetSelectorProps) {
  const [draft, setDraft] = useState("");
  const [localError, setLocalError] = useState<string | null>(null);

  const handleSubmit = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const ticker = draft.trim().toUpperCase();
    if (!ticker) return;
    if (!TICKER_PATTERN.test(ticker)) {
      setLocalError("Use a stock symbol (AAPL) or crypto pair (BTC/USDT)");
      return;
    }
    setLocalError(null);
    try {
      await onSubmit(ticker);
      setDraft("");
    } catch {
      // submit error is surfaced through the `error` prop
    }
  };

  const message = localError ?? error;

  return (
    <div className="flex flex-wrap items-center gap-3 rounded-md border border-zinc-800 bg-zinc-950 px-4 py-2">
      <span className="text-[11px] font-semibold uppercase tracking-wider text-zinc-400">Asset</span>
      <span className="font-mono text-sm font-semibold text-zinc-100">{activeTicker ?? "…"}</span>
      {dataSourceMode && (
        <span
          className={`rounded-full border px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide ${
            dataSourceMode === "live"
              ? "border-emerald-500/40 bg-emerald-500/10 text-emerald-300"
              : "border-zinc-700 bg-zinc-900 text-zinc-400"
          }`}
          title={
            dataSourceMode === "live"
              ? "Streaming real market data (execution stays simulated — paper trading)"
              : "Simulated random-walk market data"
          }
        >
          {dataSourceMode === "live" ? "Live data" : "Mock data"}
        </span>
      )}
      <form onSubmit={handleSubmit} className="flex flex-1 items-center justify-end gap-2">
        <input
          value={draft}
          onChange={(event) => {
            setDraft(event.target.value);
            setLocalError(null);
          }}
          placeholder="Track a ticker… e.g. AAPL, TSLA, BTC/USDT"
          spellCheck={false}
          autoComplete="off"
          aria-label="Asset ticker"
          className="w-64 rounded-md border border-zinc-700 bg-zinc-900 px-3 py-1.5 font-mono text-xs text-zinc-100 placeholder:text-zinc-600 focus:border-sky-500 focus:outline-none"
        />
        <button
          type="submit"
          disabled={pending || !draft.trim()}
          className="rounded-md border border-sky-500/40 bg-sky-500/10 px-3 py-1.5 text-xs font-semibold text-sky-300 hover:bg-sky-500/20 disabled:cursor-not-allowed disabled:opacity-40"
        >
          {pending ? "Switching…" : "Track"}
        </button>
      </form>
      {message && <span className="w-full text-right text-[11px] font-medium text-rose-400">{message}</span>}
    </div>
  );
}
