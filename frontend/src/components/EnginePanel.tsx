import type { TradeSignal } from "@/lib/types";
import SignalFeed from "./SignalFeed";

interface EnginePanelProps {
  title: string;
  subtitle: string;
  accent: string;
  connected: boolean;
  signals: TradeSignal[];
}

export default function EnginePanel({ title, subtitle, accent, connected, signals }: EnginePanelProps) {
  return (
    <section className="flex h-[32rem] flex-col rounded-lg border border-zinc-800 bg-zinc-950/60 p-4">
      <header className="mb-3 flex items-start justify-between border-b border-zinc-800 pb-3">
        <div>
          <h2 className="text-sm font-semibold" style={{ color: accent }}>
            {title}
          </h2>
          <p className="mt-0.5 text-xs text-zinc-500">{subtitle}</p>
        </div>
        <div className="flex items-center gap-1.5 text-xs text-zinc-500">
          <span
            className={`h-2 w-2 rounded-full ${connected ? "bg-emerald-400" : "bg-zinc-600"}`}
            aria-hidden
          />
          {connected ? "live" : "reconnecting"}
        </div>
      </header>
      <SignalFeed signals={signals} />
    </section>
  );
}
