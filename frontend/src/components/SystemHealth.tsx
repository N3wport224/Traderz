"use client";

import type { RiskGuardStatus, RiskGuardSync } from "@/lib/types";

function latencyTone(ms: number): string {
  if (ms <= 50) return "text-emerald-400";
  if (ms <= 250) return "text-amber-400";
  return "text-rose-400";
}

function Chip({ children, tone, title }: { children: React.ReactNode; tone: string; title?: string }) {
  return (
    <span
      className={`inline-flex items-center gap-1.5 rounded-md border px-2.5 py-1.5 text-[11px] font-medium ${tone}`}
      title={title}
    >
      {children}
    </span>
  );
}

interface SystemHealthProps {
  guard: RiskGuardStatus | null;
  sync: RiskGuardSync | null;
  dbMode: string | null;
  transport: "rest" | "websocket" | null;
  streamLatencyMs: number | null;
  onKill: () => Promise<void>;
  onReset: () => Promise<void>;
  actionPending: boolean;
}

/** Infrastructure & Connectivity bar: guard health + kill switch, live stream
 * latency, database journal mode, and the persisted-state sync indicator. */
export default function SystemHealth({
  guard,
  sync,
  dbMode,
  transport,
  streamLatencyMs,
  onKill,
  onReset,
  actionPending,
}: SystemHealthProps) {
  const locked = guard?.locked ?? false;
  const synced = sync?.in_sync ?? true;

  return (
    <div className="flex flex-wrap items-center gap-2">
      {/* stream latency (ws event delta when streaming; absent under REST polling) */}
      {transport === "websocket" && streamLatencyMs !== null ? (
        <Chip
          tone={`border-zinc-700 bg-zinc-900 ${latencyTone(streamLatencyMs)}`}
          title="Data-stream latency: exchange event time to local ingest (ping/pong delta)"
        >
          <span className="text-zinc-500">STREAM</span>
          <span className="font-mono font-semibold">{streamLatencyMs.toFixed(0)}ms</span>
        </Chip>
      ) : (
        <Chip tone="border-zinc-700 bg-zinc-900 text-zinc-400" title="Candles arrive by REST polling">
          <span className="text-zinc-500">STREAM</span>
          <span className="font-mono font-semibold">REST</span>
        </Chip>
      )}

      {/* database journal mode */}
      <Chip
        tone={
          dbMode === "wal"
            ? "border-emerald-500/30 bg-emerald-500/10 text-emerald-400"
            : "border-zinc-700 bg-zinc-900 text-zinc-400"
        }
        title="SQLite journal mode — WAL allows concurrent reads during writes"
      >
        <span className="text-zinc-500">DB</span>
        <span className="font-mono font-semibold uppercase">
          {dbMode === "wal" ? "WAL / Active" : (dbMode ?? "…")}
        </span>
      </Chip>

      {/* persisted risk-guard sync state */}
      <Chip
        tone={
          synced
            ? "border-emerald-500/30 bg-emerald-500/10 text-emerald-400"
            : "border-amber-500/40 bg-amber-500/10 text-amber-300"
        }
        title={
          synced
            ? "In-memory risk guard state exactly matches the persisted SystemState row — crash-safe"
            : (sync?.detail ?? "Guard state and database row differ — persistence lagging")
        }
      >
        <span className="text-zinc-500">SYNC</span>
        <span className="font-semibold">{synced ? "State Synced ✓" : "Drift ⚠"}</span>
      </Chip>

      {/* guard health + controls */}
      {locked ? (
        <>
          <span
            className="inline-flex animate-pulse items-center gap-1.5 rounded-md border border-rose-400/60 bg-rose-500/20 px-3 py-1.5 text-xs font-bold uppercase tracking-wide text-rose-300"
            title={guard?.locked_reason ?? undefined}
          >
            <span className="h-2 w-2 rounded-full bg-rose-400" />
            Risk Guard Tripped — Bot Locked
          </span>
          <button
            onClick={() => void onReset()}
            disabled={actionPending}
            className="rounded-md border border-zinc-600 px-3 py-1.5 text-xs font-semibold text-zinc-200 hover:bg-zinc-800 disabled:opacity-40"
            title="Operator reset: release the circuit breaker and clear daily counters"
          >
            Reset Guard
          </button>
        </>
      ) : (
        <>
          <Chip
            tone="border-emerald-500/30 bg-emerald-500/10 text-emerald-400"
            title={
              guard
                ? `Daily PnL ${guard.daily_realized_pnl.toFixed(2)} | entries ${guard.daily_entry_count}/${guard.max_daily_trade_count} | loss limit ${(guard.max_daily_loss_pct * 100).toFixed(1)}%`
                : "Loading guard status…"
            }
          >
            <span className="h-2 w-2 rounded-full bg-emerald-400" />
            <span className="font-semibold">System Healthy</span>
          </Chip>
          <button
            onClick={() => void onKill()}
            disabled={actionPending}
            className="rounded-md border border-rose-500/50 bg-rose-500/10 px-3 py-1.5 text-xs font-bold uppercase text-rose-300 hover:bg-rose-500/25 disabled:opacity-40"
            title="Force the circuit breaker: flatten all open positions and reject every new entry"
          >
            Emergency Kill Switch
          </button>
        </>
      )}
    </div>
  );
}
