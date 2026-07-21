"use client";

import { useEffect, useState } from "react";

import ActiveTargetSignals from "@/components/ActiveTargetSignals";
import AssetSelector from "@/components/AssetSelector";
import BacktestPanel from "@/components/BacktestPanel";
import DataDisconnectedBanner from "@/components/DataDisconnectedBanner";
import EnginePanel from "@/components/EnginePanel";
import EquityChart from "@/components/EquityChart";
import RiskControlsModal from "@/components/RiskControlsModal";
import SystemHealth from "@/components/SystemHealth";
import SystemStatusBadge from "@/components/SystemStatusBadge";
import TelemetryBar from "@/components/TelemetryBar";
import TradesTable from "@/components/TradesTable";
import TraderWatchPanel from "@/components/TraderWatchPanel";
import { useBrackets } from "@/hooks/useBrackets";
import { useEngineFeed } from "@/hooks/useEngineFeed";
import { useRiskStatus } from "@/hooks/useRiskStatus";
import { useTelemetry } from "@/hooks/useTelemetry";
import { useTraderWatch } from "@/hooks/useTraderWatch";
import { useWatchlist } from "@/hooks/useWatchlist";

/** The full-page workspaces. Each mode swaps the ENTIRE page for the panels
 * that matter to that style of trading — no mixed clutter. */
type DashboardView = "day" | "swing" | "crypto" | "copy" | "analytics";

const TABS: { key: DashboardView; label: string; hint: string }[] = [
  { key: "day", label: "Day Trading", hint: "1-minute breakouts · intraday momentum · stocks" },
  { key: "swing", label: "Long-Term", hint: "4-hour trendlines · multi-day swings · stocks" },
  { key: "crypto", label: "Crypto", hint: "both engines on a crypto pair · 24/7 market" },
  { key: "copy", label: "Copy Trading", hint: "track people's trades · notify · auto-follow" },
  { key: "analytics", label: "Analytics & Backtesting", hint: "replay strategies over history" },
];

const DEFAULT_STOCK = "MOCK";
const DEFAULT_CRYPTO = "BTC/USDT";

const isCryptoTicker = (ticker: string): boolean => ticker.includes("/");

/** Remembered per-mode tickers so tab switches land on the asset you last
 * used there (persisted across visits in localStorage). */
function rememberedTicker(key: string, fallback: string): string {
  if (typeof window === "undefined") return fallback;
  return window.localStorage.getItem(key) ?? fallback;
}

/** Everything scoped to one tracked asset. Keyed by ticker+mode from the
 * parent so a watchlist or tab switch unmounts it wholesale — feeds, charts,
 * and tables all reset and replot for the fresh context. `engines` controls
 * which engine's panels render: day = momentum only, long-term = swing only,
 * crypto = both. */
function AssetDashboard({
  ticker,
  engines,
}: {
  ticker: string;
  engines: ("momentum" | "swing")[];
}) {
  const momentum = useEngineFeed("momentum");
  const swing = useEngineFeed("swing");
  const brackets = useBrackets();

  const showMomentum = engines.includes("momentum");
  const showSwing = engines.includes("swing");
  const visibleBrackets = brackets.filter((b) =>
    (b.engine_type === "momentum" && showMomentum) || (b.engine_type === "swing" && showSwing),
  );

  return (
    <>
      <div className={`grid grid-cols-1 gap-4 ${showMomentum && showSwing ? "lg:grid-cols-2" : ""}`}>
        {showMomentum && (
          <EnginePanel
            title={`Day Trading — Momentum Engine · ${ticker}`}
            subtitle="1-minute Opening Range Breakout, live-configurable time-stop"
            accent="#34d399"
            connected={momentum.connected}
            signals={momentum.signals}
          />
        )}
        {showSwing && (
          <EnginePanel
            title={`Swing Trading — Trendline Engine · ${ticker}`}
            subtitle="4-hour pivots, trendline retests, engulfing confirmation"
            accent="#38bdf8"
            connected={swing.connected}
            signals={swing.signals}
          />
        )}
      </div>

      <EquityChart
        momentumEquity={momentum.equityCurve}
        swingEquity={swing.equityCurve}
        show={showMomentum && showSwing ? "both" : showMomentum ? "momentum" : "swing"}
      />

      <ActiveTargetSignals brackets={visibleBrackets} />

      <div className={`grid grid-cols-1 gap-4 ${showMomentum && showSwing ? "lg:grid-cols-2" : ""}`}>
        {showMomentum && (
          <TradesTable title={`Momentum Engine — Trade History · ${ticker}`} trades={momentum.trades} />
        )}
        {showSwing && (
          <TradesTable title={`Swing Engine — Trade History · ${ticker}`} trades={swing.trades} />
        )}
      </div>
    </>
  );
}

export default function Dashboard() {
  const risk = useRiskStatus();
  const { telemetry, unreachable } = useTelemetry();
  const watchlist = useWatchlist();
  const traderWatch = useTraderWatch();
  const [riskModalOpen, setRiskModalOpen] = useState(false);
  const [view, setView] = useState<DashboardView>("day");
  const [lastStock, setLastStock] = useState(() => rememberedTicker("traderz.lastStock", DEFAULT_STOCK));
  const [lastCrypto, setLastCrypto] = useState(() =>
    rememberedTicker("traderz.lastCrypto", DEFAULT_CRYPTO),
  );

  const dataDisconnected = telemetry?.data_disconnected ?? risk.status?.data_disconnected ?? false;
  const activeTicker = watchlist.ticker ?? telemetry?.ticker ?? null;
  const guard = telemetry?.risk_guard ?? risk.status?.risk_guard ?? null;

  // Remember the last asset used per world (stock vs crypto).
  useEffect(() => {
    if (!activeTicker) return;
    if (isCryptoTicker(activeTicker)) {
      setLastCrypto(activeTicker);
      window.localStorage.setItem("traderz.lastCrypto", activeTicker);
    } else {
      setLastStock(activeTicker);
      window.localStorage.setItem("traderz.lastStock", activeTicker);
    }
  }, [activeTicker]);

  // Mode tabs swap the tracked asset with them: entering Crypto resubscribes
  // both engines to your last crypto pair; entering a stock mode switches
  // back to your last stock. The whole page follows the tab.
  useEffect(() => {
    if (!activeTicker || watchlist.pending) return;
    if (view === "crypto" && !isCryptoTicker(activeTicker)) {
      void watchlist.submit(lastCrypto);
    } else if ((view === "day" || view === "swing") && isCryptoTicker(activeTicker)) {
      void watchlist.submit(lastStock);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [view, activeTicker, watchlist.pending]);

  const activeTab = TABS.find((t) => t.key === view);
  const showMarketChrome = view === "day" || view === "swing" || view === "crypto";

  return (
    <div className="min-h-screen bg-black text-zinc-100">
      <main className="mx-auto flex max-w-6xl flex-col gap-6 px-6 py-8">
        <TelemetryBar telemetry={telemetry} unreachable={unreachable} />

        {showMarketChrome && (
          <AssetSelector
            activeTicker={activeTicker}
            dataSourceMode={watchlist.dataSourceMode ?? telemetry?.data_source_mode ?? null}
            pending={watchlist.pending}
            error={watchlist.error}
            onSubmit={watchlist.submit}
          />
        )}

        {dataDisconnected && (
          <DataDisconnectedBanner
            tickers={telemetry?.disconnected_tickers ?? risk.status?.disconnected_tickers ?? []}
            streams={telemetry?.streams ?? {}}
          />
        )}

        <header className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <h1 className="text-lg font-semibold tracking-tight text-zinc-100">Traderz</h1>
            <p className="text-sm text-zinc-500">Multi-engine algorithmic trading dashboard</p>
          </div>
          <div className="flex flex-wrap items-center gap-3">
            <SystemHealth
              guard={guard}
              sync={telemetry?.risk_guard_sync ?? risk.status?.risk_guard_sync ?? null}
              dbMode={telemetry?.database?.journal_mode ?? null}
              transport={telemetry?.transport ?? null}
              streamLatencyMs={telemetry?.stream_latency_ms ?? null}
              gateway={telemetry?.gateway ?? null}
              notifier={telemetry?.notifier ?? null}
              onKill={risk.kill}
              onReset={risk.resetGuard}
              actionPending={risk.actionPending}
            />
            <SystemStatusBadge status={risk.status?.system_status ?? null} />
            <button
              onClick={() => setRiskModalOpen(true)}
              className="rounded-md border border-zinc-700 px-3 py-1.5 text-xs font-medium text-zinc-300 hover:bg-zinc-800"
            >
              Risk &amp; Controls
            </button>
          </div>
        </header>

        {/* full-page mode tabs */}
        <nav className="border-b border-zinc-800">
          <div className="flex flex-wrap gap-1">
            {TABS.map((tab) => (
              <button
                key={tab.key}
                onClick={() => setView(tab.key)}
                className={`-mb-px border-b-2 px-4 py-2 text-sm font-medium transition-colors ${
                  view === tab.key
                    ? "border-sky-400 text-sky-300"
                    : "border-transparent text-zinc-500 hover:text-zinc-300"
                }`}
              >
                {tab.label}
              </button>
            ))}
          </div>
        </nav>
        {activeTab && <p className="-mt-4 text-xs text-zinc-600">{activeTab.hint}</p>}

        {view === "day" && activeTicker && (
          <AssetDashboard key={`day-${activeTicker}`} ticker={activeTicker} engines={["momentum"]} />
        )}
        {view === "swing" && activeTicker && (
          <AssetDashboard key={`swing-${activeTicker}`} ticker={activeTicker} engines={["swing"]} />
        )}
        {view === "crypto" && activeTicker && (
          <AssetDashboard
            key={`crypto-${activeTicker}`}
            ticker={activeTicker}
            engines={["momentum", "swing"]}
          />
        )}
        {view === "copy" && <TraderWatchPanel watch={traderWatch} />}
        {view === "analytics" && <BacktestPanel />}
      </main>

      <RiskControlsModal open={riskModalOpen} onClose={() => setRiskModalOpen(false)} risk={risk} />
    </div>
  );
}
