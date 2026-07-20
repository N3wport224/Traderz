import type { TelemetryStats } from "@/lib/types";

function formatLatency(ms: number): string {
  if (ms >= 100) return `${Math.round(ms)}ms`;
  return `${ms.toFixed(ms >= 10 ? 0 : 1)}ms`;
}

function formatDollars(amount: number): string {
  return `$${amount.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

function latencyTone(ms: number): string {
  if (ms <= 25) return "text-emerald-400";
  if (ms <= 100) return "text-amber-400";
  return "text-rose-400";
}

interface MetricProps {
  label: string;
  value: string;
  tone?: string;
  title?: string;
}

function Metric({ label, value, tone = "text-zinc-200", title }: MetricProps) {
  return (
    <div className="flex items-baseline gap-1.5" title={title}>
      <span className="text-[11px] uppercase tracking-wide text-zinc-500">{label}</span>
      <span className={`font-mono text-xs font-semibold ${tone}`}>{value}</span>
    </div>
  );
}

interface TelemetryBarProps {
  telemetry: TelemetryStats | null;
  unreachable: boolean;
}

export default function TelemetryBar({ telemetry, unreachable }: TelemetryBarProps) {
  if (!telemetry) {
    return (
      <div className="flex items-center gap-4 rounded-md border border-zinc-800 bg-zinc-950 px-4 py-2 text-xs text-zinc-500">
        {unreachable ? "Telemetry unavailable — backend unreachable" : "Loading system telemetry…"}
      </div>
    );
  }

  const live = telemetry.gateway.mode === "live";
  // In-memory tracker covers this process's fills; the persisted total also
  // includes trades closed before the last restart. Show the larger picture.
  const slippage = Math.max(telemetry.cumulative_slippage_cost, telemetry.persisted_slippage_cost);
  const healed = telemetry.boot_reconciliation.healed.length;

  return (
    <div className="flex flex-wrap items-center gap-x-6 gap-y-1 rounded-md border border-zinc-800 bg-zinc-950 px-4 py-2">
      <span className="text-[11px] font-semibold uppercase tracking-wider text-zinc-400">System Telemetry</span>
      <Metric
        label="Latency"
        value={formatLatency(telemetry.connection_latency_ms)}
        tone={latencyTone(telemetry.connection_latency_ms)}
        title={`Last gateway fill latency. Avg signal→fill: ${formatLatency(
          telemetry.avg_signal_to_approval_ms + telemetry.avg_approval_to_fill_ms,
        )} over ${telemetry.order_count} orders`}
      />
      <Metric
        label="Slippage"
        value={formatDollars(slippage)}
        tone={slippage > 0 ? "text-amber-400" : "text-zinc-200"}
        title="Cumulative dollars lost to fill-price degradation"
      />
      <Metric
        label="Gateway"
        value={live ? `LIVE EXCHANGE CONNECTED (${telemetry.gateway.name})` : "MOCK"}
        tone={live ? "text-rose-300" : "text-sky-300"}
        title="Order execution backend"
      />
      <Metric label="Orders" value={String(telemetry.order_count)} />
      {healed > 0 && (
        <Metric
          label="Recovered"
          value={String(healed)}
          tone="text-amber-400"
          title="Open positions self-healed by boot reconciliation"
        />
      )}
      {unreachable && <span className="text-[11px] font-medium text-rose-400">stale — backend unreachable</span>}
    </div>
  );
}
