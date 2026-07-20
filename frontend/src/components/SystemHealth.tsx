"use client";

import type { RiskGuardStatus } from "@/lib/types";

interface SystemHealthProps {
  guard: RiskGuardStatus | null;
  onKill: () => Promise<void>;
  onReset: () => Promise<void>;
  actionPending: boolean;
}

/** Header health indicator: healthy chip, or a flashing tripped badge with the
 * operator controls. The Emergency Kill Switch is always one click away. */
export default function SystemHealth({ guard, onKill, onReset, actionPending }: SystemHealthProps) {
  const locked = guard?.locked ?? false;

  return (
    <div className="flex items-center gap-2">
      {locked ? (
        <span
          className="inline-flex animate-pulse items-center gap-1.5 rounded-md border border-rose-400/60 bg-rose-500/20 px-3 py-1.5 text-xs font-bold uppercase tracking-wide text-rose-300"
          title={guard?.locked_reason ?? undefined}
        >
          <span className="h-2 w-2 rounded-full bg-rose-400" />
          Risk Guard Tripped — Bot Locked
        </span>
      ) : (
        <span
          className="inline-flex items-center gap-1.5 rounded-md border border-emerald-500/30 bg-emerald-500/10 px-3 py-1.5 text-xs font-medium text-emerald-400"
          title={
            guard
              ? `Daily PnL ${guard.daily_realized_pnl.toFixed(2)} | entries ${guard.daily_entry_count}/${guard.max_daily_trade_count} | loss limit ${(guard.max_daily_loss_pct * 100).toFixed(1)}%`
              : "Loading guard status…"
          }
        >
          <span className="h-2 w-2 rounded-full bg-emerald-400" />
          System Healthy
        </span>
      )}
      {locked ? (
        <button
          onClick={() => void onReset()}
          disabled={actionPending}
          className="rounded-md border border-zinc-600 px-3 py-1.5 text-xs font-semibold text-zinc-200 hover:bg-zinc-800 disabled:opacity-40"
          title="Operator reset: release the circuit breaker and clear daily counters"
        >
          Reset Guard
        </button>
      ) : (
        <button
          onClick={() => void onKill()}
          disabled={actionPending}
          className="rounded-md border border-rose-500/50 bg-rose-500/10 px-3 py-1.5 text-xs font-bold uppercase text-rose-300 hover:bg-rose-500/25 disabled:opacity-40"
          title="Force the circuit breaker: flatten all open positions and reject every new entry"
        >
          Emergency Kill Switch
        </button>
      )}
    </div>
  );
}
