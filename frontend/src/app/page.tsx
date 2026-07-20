"use client";

import { useState } from "react";

import EnginePanel from "@/components/EnginePanel";
import EquityChart from "@/components/EquityChart";
import RiskControlsModal from "@/components/RiskControlsModal";
import SystemStatusBadge from "@/components/SystemStatusBadge";
import TradesTable from "@/components/TradesTable";
import { useEngineFeed } from "@/hooks/useEngineFeed";
import { useRiskStatus } from "@/hooks/useRiskStatus";

export default function Dashboard() {
  const momentum = useEngineFeed("momentum");
  const swing = useEngineFeed("swing");
  const risk = useRiskStatus();
  const [riskModalOpen, setRiskModalOpen] = useState(false);

  return (
    <div className="min-h-screen bg-black text-zinc-100">
      <main className="mx-auto flex max-w-6xl flex-col gap-6 px-6 py-8">
        <header className="flex items-start justify-between gap-4">
          <div>
            <h1 className="text-lg font-semibold tracking-tight text-zinc-100">Traderz</h1>
            <p className="text-sm text-zinc-500">Multi-engine algorithmic trading dashboard</p>
          </div>
          <div className="flex items-center gap-3">
            <SystemStatusBadge status={risk.status?.system_status ?? null} />
            <button
              onClick={() => setRiskModalOpen(true)}
              className="rounded-md border border-zinc-700 px-3 py-1.5 text-xs font-medium text-zinc-300 hover:bg-zinc-800"
            >
              Risk &amp; Controls
            </button>
          </div>
        </header>

        <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
          <EnginePanel
            title="Day Trading — Momentum Engine"
            subtitle="1-minute Opening Range Breakout, live-configurable time-stop"
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

        <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
          <TradesTable title="Momentum Engine — Trade History" trades={momentum.trades} />
          <TradesTable title="Swing Engine — Trade History" trades={swing.trades} />
        </div>
      </main>

      <RiskControlsModal open={riskModalOpen} onClose={() => setRiskModalOpen(false)} risk={risk} />
    </div>
  );
}
