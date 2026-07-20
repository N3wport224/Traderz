import type { SystemStatus } from "@/lib/types";

const STYLES: Record<SystemStatus, string> = {
  RUNNING: "text-emerald-400 border-emerald-500/30 bg-emerald-500/10",
  PAUSED: "text-amber-400 border-amber-500/30 bg-amber-500/10",
  HALTED_BY_DRAWDOWN: "text-rose-400 border-rose-500/30 bg-rose-500/10",
};

const LABELS: Record<SystemStatus, string> = {
  RUNNING: "Running",
  PAUSED: "Paused",
  HALTED_BY_DRAWDOWN: "Halted by drawdown",
};

interface SystemStatusBadgeProps {
  status: SystemStatus | null;
}

export default function SystemStatusBadge({ status }: SystemStatusBadgeProps) {
  if (!status) {
    return (
      <span className="inline-flex items-center gap-1.5 rounded-full border border-zinc-700 bg-zinc-900 px-3 py-1 text-xs font-medium text-zinc-500">
        <span className="h-1.5 w-1.5 rounded-full bg-zinc-600" />
        Connecting&hellip;
      </span>
    );
  }

  return (
    <span className={`inline-flex items-center gap-1.5 rounded-full border px-3 py-1 text-xs font-semibold ${STYLES[status]}`}>
      <span
        className={`h-1.5 w-1.5 rounded-full ${status === "RUNNING" ? "animate-pulse" : ""}`}
        style={{ backgroundColor: "currentColor" }}
      />
      {LABELS[status]}
    </span>
  );
}
