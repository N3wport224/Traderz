"use client";

import { useState } from "react";

import ActiveTargetSignals from "@/components/ActiveTargetSignals";
import AssetSelector from "@/components/AssetSelector";
import DataDisconnectedBanner from "@/components/DataDisconnectedBanner";
import EnginePanel from "@/components/EnginePanel";
import EquityChart from "@/components/EquityChart";
import RiskControlsModal from "@/components/RiskControlsModal";
import SystemStatusBadge from "@/components/SystemStatusBadge";
import TelemetryBar from "@/components/TelemetryBar";
import TradesTable from "@/components/TradesTable";
import { useBrackets } from "@/hooks/useBrackets";
import { useEngineFeed } from "@/hooks/useEngineFeed";
import { useRiskStatus } from "@/hooks/useRiskStatus";
import { useTelemetry } from "@/hooks/useTelemetry";
import { useWatchlist } from "@/hooks/useWatchlist";

/** Everything scoped to one tracked asset. Keyed by ticker from the parent so a
 * watchlist switch unmounts it wholesale — feeds, charts, and tables all reset
 * and replot from the fresh symbol's data instead of mixing old series in. */
function AssetDashboard({ ticker }: { ticker: string }) {
  const momentum = useEngineFeed("momentum");
  const swing = useEngineFeed("swing");
  const brackets = useBrackets();

  return (
    <>
      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <EnginePanel
          title={`Day Trading — Momentum Engine · ${ticker}`}
          subtitle="1-minute Opening Range Breakout, live-configurable time-stop"
          accent="#34d399"
          connected={momentum.connected}
          signals={momentum.signals}
        />
        <EnginePanel
          title={`Swing Trading — Trendline Engine · ${ticker}`}
          subtitle="4-hour pivots, trendline retests, engulfing confirmation"
          accent="#38bdf8"
          connected={swing.connected}
          signals={swing.signals}
        />
      </div>

      <EquityChart momentumEquity={momentum.equityCurve} swingEquity={swing.equityCurve} />

      <ActiveTargetSignals brackets={brackets} />

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <TradesTable title={`Momentum Engine — Trade History · ${ticker}`} trades={momentum.trades} />
        <TradesTable title={`Swing Engine — Trade History · ${ticker}`} trades={swing.trades} />
      </div>
    </>
  );
}

export default function Dashboard() {
  const risk = useRiskStatus();
  const { telemetry, unreachable } = useTelemetry();
  const watchlist = useWatchlist();
  const [riskModalOpen, setRiskModalOpen] = useState(false);

  const dataDisconnected = telemetry?.data_disconnected ?? risk.status?.data_disconnected ?? false;
  const activeTicker = watchlist.ticker ?? telemetry?.ticker ?? null;

  return (
    <div className="min-h-screen bg-black text-zinc-100">
      <main className="mx-auto flex max-w-6xl flex-col gap-6 px-6 py-8">
        <TelemetryBar telemetry={telemetry} unreachable={unreachable} />

        <AssetSelector
          activeTicker={activeTicker}
          dataSourceMode={watchlist.dataSourceMode ?? telemetry?.data_source_mode ?? null}
          pending={watchlist.pending}
          error={watchlist.error}
          onSubmit={watchlist.submit}
        />

        {dataDisconnected && (
          <DataDisconnectedBanner
            tickers={telemetry?.disconnected_tickers ?? risk.status?.disconnected_tickers ?? []}
            streams={telemetry?.streams ?? {}}
          />
        )}

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

        {activeTicker && <AssetDashboard key={activeTicker} ticker={activeTicker} />}
      </main>

      <RiskControlsModal open={riskModalOpen} onClose={() => setRiskModalOpen(false)} risk={risk} />
    </div>
  );
}
