import type { TradeSignal } from "@/lib/types";

const ACTION_STYLES: Record<TradeSignal["action"], string> = {
  buy: "text-emerald-400 border-emerald-500/30 bg-emerald-500/10",
  short: "text-rose-400 border-rose-500/30 bg-rose-500/10",
  sell: "text-rose-400 border-rose-500/30 bg-rose-500/10",
  exit: "text-amber-400 border-amber-500/30 bg-amber-500/10",
  alert: "text-sky-400 border-sky-500/30 bg-sky-500/10",
};

function formatTime(timestamp: string): string {
  const date = new Date(timestamp);
  if (Number.isNaN(date.getTime())) return timestamp;
  return date.toLocaleString(undefined, {
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

interface SignalFeedProps {
  signals: TradeSignal[];
}

export default function SignalFeed({ signals }: SignalFeedProps) {
  if (signals.length === 0) {
    return (
      <div className="flex flex-1 items-center justify-center text-sm text-zinc-500">
        Waiting for signals&hellip;
      </div>
    );
  }

  const ordered = [...signals].reverse();

  return (
    <ul className="flex flex-1 flex-col gap-2 overflow-y-auto pr-1">
      {ordered.map((signal, index) => (
        <li
          key={`${signal.timestamp}-${signal.action}-${index}`}
          className={`rounded-md border px-3 py-2 font-mono text-xs leading-relaxed ${ACTION_STYLES[signal.action]}`}
        >
          <div className="flex items-center justify-between gap-2">
            <span className="font-semibold uppercase tracking-wide">{signal.action}</span>
            <span className="text-zinc-500">{formatTime(signal.timestamp)}</span>
          </div>
          <div className="mt-1 flex items-center justify-between gap-2 text-zinc-300">
            <span>{signal.symbol}</span>
            <span>${signal.price.toFixed(2)}</span>
          </div>
          <div className="mt-1 text-zinc-500">{signal.reason.replaceAll("_", " ")}</div>
        </li>
      ))}
    </ul>
  );
}
