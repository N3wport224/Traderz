"use client";

import { useState } from "react";

import { useConfig } from "@/hooks/useConfig";
import type { UseRiskStatus } from "@/hooks/useRiskStatus";
import type { MomentumConfig, SwingConfig } from "@/lib/types";
import SystemStatusBadge from "./SystemStatusBadge";

interface RiskControlsModalProps {
  open: boolean;
  onClose: () => void;
  risk: UseRiskStatus;
}

export default function RiskControlsModal({ open, onClose, risk }: RiskControlsModalProps) {
  const { config, error, saving, saveMomentum, saveSwing } = useConfig();

  if (!open) return null;

  const status = risk.status;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 p-4" onClick={onClose}>
      <div
        className="max-h-[90vh] w-full max-w-lg overflow-y-auto rounded-lg border border-zinc-800 bg-zinc-950 p-6"
        onClick={(event) => event.stopPropagation()}
      >
        <div className="mb-5 flex items-center justify-between">
          <h2 className="text-base font-semibold text-zinc-100">Risk &amp; Controls</h2>
          <button onClick={onClose} className="text-lg leading-none text-zinc-500 hover:text-zinc-300" aria-label="Close">
            &times;
          </button>
        </div>

        <section className="mb-6">
          <div className="mb-3 flex items-center justify-between">
            <SystemStatusBadge status={status?.system_status ?? null} />
            <div className="flex gap-2">
              <button
                onClick={() => void risk.pause()}
                disabled={risk.actionPending || !status || status.paused}
                className="rounded border border-zinc-700 px-3 py-1 text-xs text-zinc-300 hover:bg-zinc-800 disabled:opacity-40"
              >
                Pause
              </button>
              <button
                onClick={() => void risk.resume()}
                disabled={risk.actionPending || !status || (!status.paused && !status.halted)}
                className="rounded border border-zinc-700 px-3 py-1 text-xs text-zinc-300 hover:bg-zinc-800 disabled:opacity-40"
              >
                Resume
              </button>
            </div>
          </div>

          {status && (
            <dl className="grid grid-cols-2 gap-x-4 gap-y-2 rounded-md border border-zinc-800 bg-zinc-900/50 p-3 font-mono text-xs">
              <Metric label="Daily P&L" value={`${status.daily_pnl >= 0 ? "+" : ""}${status.daily_pnl.toFixed(2)}`} />
              <Metric
                label="Drawdown"
                value={`${(status.daily_drawdown_pct * 100).toFixed(2)}% / ${(status.max_daily_drawdown_pct * 100).toFixed(2)}%`}
              />
              <Metric label="Total capital" value={`$${status.total_capital.toLocaleString()}`} />
              <Metric label="Fee rate" value={`${(status.fee_rate * 100).toFixed(3)}%`} />
              <Metric label="Momentum alloc" value={`${((status.allocation_pct.momentum ?? 0) * 100).toFixed(1)}%`} />
              <Metric label="Swing alloc" value={`${((status.allocation_pct.swing ?? 0) * 100).toFixed(1)}%`} />
              {status.current_date && <Metric label="Trading day" value={status.current_date} />}
              {status.halted_reason && (
                <div className="col-span-2 mt-1 rounded border border-rose-500/30 bg-rose-500/10 px-2 py-1 text-rose-300">
                  {status.halted_reason}
                </div>
              )}
            </dl>
          )}
        </section>

        {error && <p className="mb-3 text-xs text-rose-400">{error}</p>}

        {config ? (
          <div className="flex flex-col gap-5">
            <MomentumConfigForm initial={config.momentum} saving={saving} onSave={saveMomentum} />
            <SwingConfigForm initial={config.swing} saving={saving} onSave={saveSwing} />
          </div>
        ) : (
          <p className="text-xs text-zinc-500">Loading configuration&hellip;</p>
        )}
      </div>
    </div>
  );
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex flex-col">
      <span className="text-zinc-500">{label}</span>
      <span className="text-zinc-200">{value}</span>
    </div>
  );
}

function MomentumConfigForm({
  initial,
  saving,
  onSave,
}: {
  initial: MomentumConfig;
  saving: boolean;
  onSave: (update: Partial<MomentumConfig>) => Promise<boolean>;
}) {
  const [openingRange, setOpeningRange] = useState(initial.opening_range_minutes);
  const [timeStop, setTimeStop] = useState(initial.time_stop_minutes);
  const [saved, setSaved] = useState(false);

  return (
    <form
      className="flex flex-col gap-3 rounded-md border border-zinc-800 p-3"
      onSubmit={(event) => {
        event.preventDefault();
        setSaved(false);
        void onSave({ opening_range_minutes: openingRange, time_stop_minutes: timeStop }).then(setSaved);
      }}
    >
      <h3 className="text-xs font-semibold uppercase tracking-wide text-emerald-400">Momentum Engine</h3>
      <label className="flex items-center justify-between gap-3 text-xs text-zinc-300">
        Opening range (minutes)
        <input
          type="number"
          min={1}
          max={120}
          value={openingRange}
          onChange={(event) => setOpeningRange(Number(event.target.value))}
          className="w-20 rounded border border-zinc-700 bg-zinc-900 px-2 py-1 text-right text-zinc-100"
        />
      </label>
      <label className="flex items-center justify-between gap-3 text-xs text-zinc-300">
        Time-stop (minutes)
        <input
          type="number"
          min={1}
          max={480}
          value={timeStop}
          onChange={(event) => setTimeStop(Number(event.target.value))}
          className="w-20 rounded border border-zinc-700 bg-zinc-900 px-2 py-1 text-right text-zinc-100"
        />
      </label>
      <button
        type="submit"
        disabled={saving}
        className="self-start rounded border border-emerald-500/30 bg-emerald-500/10 px-3 py-1 text-xs font-medium text-emerald-400 hover:bg-emerald-500/20 disabled:opacity-40"
      >
        {saved ? "Saved" : "Save"}
      </button>
    </form>
  );
}

function SwingConfigForm({
  initial,
  saving,
  onSave,
}: {
  initial: SwingConfig;
  saving: boolean;
  onSave: (update: Partial<SwingConfig>) => Promise<boolean>;
}) {
  const [minTouches, setMinTouches] = useState(initial.min_touches);
  const [tolerancePct, setTolerancePct] = useState(initial.touch_tolerance_pct * 100);
  const [saved, setSaved] = useState(false);

  return (
    <form
      className="flex flex-col gap-3 rounded-md border border-zinc-800 p-3"
      onSubmit={(event) => {
        event.preventDefault();
        setSaved(false);
        void onSave({ min_touches: minTouches, touch_tolerance_pct: tolerancePct / 100 }).then(setSaved);
      }}
    >
      <h3 className="text-xs font-semibold uppercase tracking-wide text-sky-400">Swing Engine</h3>
      <label className="flex items-center justify-between gap-3 text-xs text-zinc-300">
        Minimum trendline touches
        <input
          type="number"
          min={2}
          max={10}
          value={minTouches}
          onChange={(event) => setMinTouches(Number(event.target.value))}
          className="w-20 rounded border border-zinc-700 bg-zinc-900 px-2 py-1 text-right text-zinc-100"
        />
      </label>
      <label className="flex items-center justify-between gap-3 text-xs text-zinc-300">
        Bounce proximity (%)
        <input
          type="number"
          min={0.01}
          max={10}
          step={0.01}
          value={tolerancePct}
          onChange={(event) => setTolerancePct(Number(event.target.value))}
          className="w-20 rounded border border-zinc-700 bg-zinc-900 px-2 py-1 text-right text-zinc-100"
        />
      </label>
      <button
        type="submit"
        disabled={saving}
        className="self-start rounded border border-sky-500/30 bg-sky-500/10 px-3 py-1 text-xs font-medium text-sky-400 hover:bg-sky-500/20 disabled:opacity-40"
      >
        {saved ? "Saved" : "Save"}
      </button>
    </form>
  );
}
