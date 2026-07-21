"use client";

import { useState } from "react";

import { testNotifierWebhook } from "@/lib/api";
import type { GatewayInfo, NotifierPingResult, NotifierStatus } from "@/lib/types";

interface ExecutionSettingsModalProps {
  open: boolean;
  onClose: () => void;
  gateway: GatewayInfo | null;
  notifier: NotifierStatus | null;
}

function Row({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="flex items-center justify-between gap-4 py-1.5">
      <dt className="text-xs text-zinc-500">{label}</dt>
      <dd className="text-right font-mono text-xs text-zinc-200">{children}</dd>
    </div>
  );
}

const MODE_BADGES: Record<GatewayInfo["mode"], { label: string; tone: string }> = {
  mock: {
    label: "PAPER TRADING",
    tone: "border-emerald-500/40 bg-emerald-500/10 text-emerald-300",
  },
  live: {
    label: "LIVE — CCXT EXCHANGE",
    tone: "border-amber-500/40 bg-amber-500/10 text-amber-300",
  },
  prod_live: {
    label: "REAL MONEY — PROD LIVE",
    tone: "border-rose-500/50 bg-rose-500/15 text-rose-300",
  },
};

/** Settings cog target: active execution-provider metadata plus the system
 * alert webhook's health, with a connectivity test button that fires a real
 * ping through POST /api/system/notifier/test. */
export default function ExecutionSettingsModal({ open, onClose, gateway, notifier }: ExecutionSettingsModalProps) {
  const [pinging, setPinging] = useState(false);
  const [pingResult, setPingResult] = useState<NotifierPingResult | null>(null);
  const [pingError, setPingError] = useState<string | null>(null);

  if (!open) return null;

  const mode = gateway ? MODE_BADGES[gateway.mode] : null;

  const runPing = async () => {
    setPinging(true);
    setPingError(null);
    setPingResult(null);
    try {
      setPingResult(await testNotifierWebhook());
    } catch (error) {
      setPingError(error instanceof Error ? error.message : "test ping failed");
    } finally {
      setPinging(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 p-4" onClick={onClose}>
      <div
        className="max-h-[90vh] w-full max-w-lg overflow-y-auto rounded-lg border border-zinc-800 bg-zinc-950 p-6"
        onClick={(event) => event.stopPropagation()}
      >
        <div className="mb-5 flex items-center justify-between">
          <h2 className="text-base font-semibold text-zinc-100">Execution Settings</h2>
          <button onClick={onClose} className="text-lg leading-none text-zinc-500 hover:text-zinc-300" aria-label="Close">
            &times;
          </button>
        </div>

        {/* --- active execution provider --------------------------------- */}
        <section className="mb-6">
          <h3 className="mb-2 text-xs font-semibold uppercase tracking-wide text-zinc-500">
            Execution Provider
          </h3>
          <div className="rounded-md border border-zinc-800 bg-zinc-900/50 p-3">
            {gateway ? (
              <>
                <div className="mb-2 flex items-center justify-between gap-3">
                  <span className="font-mono text-sm font-semibold text-zinc-100">{gateway.name}</span>
                  {mode && (
                    <span
                      className={`rounded border px-2 py-0.5 text-[10px] font-bold uppercase tracking-wide ${mode.tone}`}
                    >
                      {mode.label}
                    </span>
                  )}
                </div>
                <dl className="divide-y divide-zinc-800/60">
                  <Row label="Provider">{gateway.provider}</Row>
                  {gateway.metadata && (
                    <>
                      <Row label="Broker endpoint">{gateway.metadata.broker_url}</Row>
                      <Row label="Exit-order retries">{gateway.metadata.exit_retry_attempts}</Row>
                      <Row label="Halt-on-exit-failure armed">
                        {gateway.metadata.risk_manager_attached ? "yes" : "no"}
                      </Row>
                    </>
                  )}
                </dl>
                {gateway.mode === "mock" && (
                  <p className="mt-2 text-[11px] leading-relaxed text-zinc-500">
                    Orders fill through the simulated gateway (slippage, fees, partial fills) — no real
                    capital is at risk.
                  </p>
                )}
                {gateway.mode === "prod_live" && (
                  <p className="mt-2 text-[11px] leading-relaxed text-rose-300/90">
                    Real-money execution is armed (GATEWAY_MODE=PROD_LIVE + I_AM_RISKING_REAL_MONEY=TRUE).
                    Exit-order failures lock the platform via the risk manager.
                  </p>
                )}
              </>
            ) : (
              <p className="text-xs text-zinc-500">Waiting for telemetry…</p>
            )}
          </div>
        </section>

        {/* --- system alert webhook --------------------------------------- */}
        <section>
          <h3 className="mb-2 text-xs font-semibold uppercase tracking-wide text-zinc-500">
            System Alert Webhook
          </h3>
          <div className="rounded-md border border-zinc-800 bg-zinc-900/50 p-3">
            <div className="flex items-center justify-between gap-3">
              <span
                className="inline-flex items-center gap-2 text-xs font-medium"
                title="Guard trips, kill-switch presses, and boot events dispatch here"
              >
                <span
                  className={`h-2 w-2 rounded-full ${notifier?.configured ? "bg-emerald-400" : "bg-zinc-600"}`}
                />
                <span className={notifier?.configured ? "text-emerald-300" : "text-zinc-400"}>
                  {notifier ? (notifier.configured ? "Webhook configured" : "Log-only mode") : "Loading…"}
                </span>
              </span>
              <button
                onClick={() => void runPing()}
                disabled={pinging || !notifier}
                className="rounded-md border border-sky-500/40 bg-sky-500/10 px-3 py-1.5 text-xs font-semibold text-sky-300 hover:bg-sky-500/20 disabled:opacity-40"
                title="Send a test ping through the configured webhook"
              >
                {pinging ? "Pinging…" : "Test Connectivity"}
              </button>
            </div>

            {notifier && (
              <dl className="mt-2 divide-y divide-zinc-800/60">
                <Row label="Delivered">{notifier.delivered_count}</Row>
                <Row label="Failed">{notifier.failed_count}</Row>
              </dl>
            )}

            {pingResult && (
              <p
                className={`mt-3 rounded border px-2.5 py-2 text-[11px] font-medium ${
                  pingResult.delivered
                    ? "border-emerald-500/40 bg-emerald-500/10 text-emerald-300"
                    : pingResult.configured
                      ? "border-rose-500/40 bg-rose-500/10 text-rose-300"
                      : "border-amber-500/40 bg-amber-500/10 text-amber-300"
                }`}
              >
                {pingResult.delivered ? "✓ " : "⚠ "}
                {pingResult.detail}
              </p>
            )}
            {pingError && (
              <p className="mt-3 rounded border border-rose-500/40 bg-rose-500/10 px-2.5 py-2 text-[11px] font-medium text-rose-300">
                ⚠ {pingError}
              </p>
            )}
          </div>
        </section>
      </div>
    </div>
  );
}
