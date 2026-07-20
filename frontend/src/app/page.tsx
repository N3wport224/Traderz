"use client";

import EnginePanel from "@/components/EnginePanel";
import EquityChart from "@/components/EquityChart";
import { useEngineFeed } from "@/hooks/useEngineFeed";

export default function Dashboard() {
  const momentum = useEngineFeed("momentum");
  const swing = useEngineFeed("swing");

  return (
    <div className="min-h-screen bg-black text-zinc-100">
      <main className="mx-auto flex max-w-6xl flex-col gap-6 px-6 py-8">
        <header>
          <h1 className="text-lg font-semibold tracking-tight text-zinc-100">Traderz</h1>
          <p className="text-sm text-zinc-500">Multi-engine algorithmic trading dashboard</p>
        </header>

        <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
          <EnginePanel
            title="Day Trading — Momentum Engine"
            subtitle="1-minute Opening Range Breakout, 20-minute time-stop"
            accent="#34d399"
            connected={momentum.connected}
            signals={momentum.signals}
          />
          <EnginePanel
            title="Swing Trading — Trendline Engine"
            subtitle="4-hour pivots, trendline retests, engulfing confirmation"
            accent="#38bdf8"
            connected={swing.connected}
            signals={swing.signals}
          />
        </div>

        <EquityChart momentumEquity={momentum.equityCurve} swingEquity={swing.equityCurve} />
      </main>
    </div>
  );
}
