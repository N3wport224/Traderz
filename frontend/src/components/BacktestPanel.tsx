"use client";

import { useState } from "react";

import { runBacktest } from "@/lib/api";
import type { BacktestReport } from "@/lib/types";

const TICKER_PATTERN = /^[A-Z0-9.\-]{1,15}(\/[A-Z0-9]{2,10})?$/;

function money(value: number): string {
  const sign = value < 0 ? "-" : "";
  return `${sign}$${Math.abs(value).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

function formatProfitFactor(value: number | "inf" | null): string {
  if (value === null) return "—";
  if (value === "inf") return "∞";
  return value.toFixed(2);
}

interface MetricTileProps {
  label: string;
  value: string;
  tone?: string;
  hint?: string;
}

function MetricTile({ label, value, tone = "text-zinc-100", hint }: MetricTileProps) {
  return (
    <div className="flex flex-col gap-1 rounded-lg border border-zinc-800 bg-zinc-950/60 p-4" title={hint}>
      <span className="text-[11px] uppercase tracking-wider text-zinc-500">{label}</span>
      <span className={`font-mono text-xl font-semibold ${tone}`}>{value}</span>
    </div>
  );
}

export default function BacktestPanel() {
  const [symbol, setSymbol] = useState("AAPL");
  const [strategy, setStrategy] = useState<"momentum" | "swing">("momentum");
  const [startDate, setStartDate] = useState("2026-07-13");
  const [endDate, setEndDate] = useState("2026-07-20");
  const [capital, setCapital] = useState("100000");
  const [running, setRunning] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [report, setReport] = useState<BacktestReport | null>(null);

  const submit = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const ticker = symbol.trim().toUpperCase();
    if (!TICKER_PATTERN.test(ticker)) {
      setError("Use a stock symbol (AAPL) or crypto pair (BTC/USDT)");
      return;
    }
    const initialCapital = Number(capital);
    if (!Number.isFinite(initialCapital) || initialCapital <= 0) {
      setError("Initial capital must be a positive number");
      return;
    }
    setRunning(true);
    setError(null);
    try {
      const result = await runBacktest({
        symbol: ticker,
        strategy,
        start_date: startDate,
        end_date: endDate,
        initial_capital: initialCapital,
      });
      setReport(result);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Backtest failed");
    } finally {
      setRunning(false);
    }
  };

  const inputClass =
    "rounded-md border border-zinc-700 bg-zinc-900 px-3 py-1.5 font-mono text-xs text-zinc-100 focus:border-sky-500 focus:outline-none";

  return (
    <div className="flex flex-col gap-4">
      <form
        onSubmit={submit}
        className="flex flex-wrap items-end gap-3 rounded-lg border border-zinc-800 bg-zinc-950/60 p-4"
      >
        <label className="flex flex-col gap-1 text-[11px] uppercase tracking-wide text-zinc-500">
          Asset
          <input
            value={symbol}
            onChange={(e) => setSymbol(e.target.value)}
            className={`${inputClass} w-32`}
            spellCheck={false}
            aria-label="Backtest asset"
          />
        </label>
        <label className="flex flex-col gap-1 text-[11px] uppercase tracking-wide text-zinc-500">
          Strategy
          <select
            value={strategy}
            onChange={(e) => setStrategy(e.target.value as "momentum" | "swing")}
            className={`${inputClass} w-44`}
            aria-label="Backtest strategy"
          >
            <option value="momentum">Momentum (1m ORB)</option>
            <option value="swing">Swing (4h trendline)</option>
          </select>
        </label>
        <label className="flex flex-col gap-1 text-[11px] uppercase tracking-wide text-zinc-500">
          Start
          <input type="date" value={startDate} onChange={(e) => setStartDate(e.target.value)} className={inputClass} />
        </label>
        <label className="flex flex-col gap-1 text-[11px] uppercase tracking-wide text-zinc-500">
          End
          <input type="date" value={endDate} onChange={(e) => setEndDate(e.target.value)} className={inputClass} />
        </label>
        <label className="flex flex-col gap-1 text-[11px] uppercase tracking-wide text-zinc-500">
          Initial Capital
          <input value={capital} onChange={(e) => setCapital(e.target.value)} className={`${inputClass} w-28`} />
        </label>
        <button
          type="submit"
          disabled={running}
          className="rounded-md border border-sky-500/40 bg-sky-500/10 px-4 py-1.5 text-xs font-semibold text-sky-300 hover:bg-sky-500/20 disabled:opacity-40"
        >
          {running ? "Replaying history…" : "Run Backtest Simulation"}
        </button>
        {error && <span className="w-full text-[11px] font-medium text-rose-400">{error}</span>}
      </form>

      {report && (
        <>
          <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
            <MetricTile
              label="Profit Factor"
              value={formatProfitFactor(report.profit_factor)}
              tone={
                report.profit_factor === "inf" || (typeof report.profit_factor === "number" && report.profit_factor >= 1)
                  ? "text-emerald-400"
                  : "text-rose-400"
              }
              hint="Gross gains / gross losses"
            />
            <MetricTile
              label="Win Rate"
              value={report.win_rate_pct === null ? "—" : `${report.win_rate_pct.toFixed(1)}%`}
              hint={`${report.trade_count} closed trades`}
            />
            <MetricTile
              label="Max Drawdown"
              value={`${report.max_drawdown_pct.toFixed(2)}%`}
              tone={report.max_drawdown_pct > 5 ? "text-rose-400" : "text-zinc-100"}
              hint="Max peak-to-trough drawdown of account equity"
            />
            <MetricTile
              label="Total Net PnL"
              value={money(report.net_pnl)}
              tone={report.net_pnl >= 0 ? "text-emerald-400" : "text-rose-400"}
              hint={`${report.total_return_pct.toFixed(2)}% return on ${money(report.initial_capital)}`}
            />
          </div>

          <div className="flex flex-wrap items-center gap-x-6 gap-y-1 rounded-lg border border-zinc-800 bg-zinc-950/60 px-4 py-3 font-mono text-xs text-zinc-400">
            <span>
              {report.strategy} · {report.symbol} · {report.bars_replayed.toLocaleString()} bars replayed
            </span>
            <span>
              trades: <span className="text-zinc-200">{report.trade_count}</span>
            </span>
            <span>
              TP hits: <span className="text-emerald-400">{report.bracket_outcomes.HIT_TP ?? 0}</span>
            </span>
            <span>
              SL hits: <span className="text-rose-400">{report.bracket_outcomes.HIT_SL ?? 0}</span>
            </span>
            <span>
              time exits: <span className="text-amber-400">{report.bracket_outcomes.TIME_EXITED ?? 0}</span>
            </span>
            {report.risk_guard?.locked && (
              <span className="font-semibold text-rose-400">
                RISK GUARD TRIPPED DURING REPLAY — {report.risk_guard.locked_reason}
              </span>
            )}
          </div>
        </>
      )}
      {!report && (
        <div className="rounded-lg border border-zinc-800 bg-zinc-950/60 p-8 text-center text-xs text-zinc-600">
          Configure a window and run a simulation — history replays through the exact live engines, gateway
          slippage, bracket monitor, and risk guard.
        </div>
      )}
    </div>
  );
}
