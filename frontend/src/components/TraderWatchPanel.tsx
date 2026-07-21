"use client";

import { useState } from "react";

import type { UseTraderWatch } from "@/hooks/useTraderWatch";
import type { TraderEvent, WatchedTrader } from "@/lib/types";

function timeLabel(timestamp: string | null): string {
  if (!timestamp) return "";
  const date = new Date(timestamp);
  return Number.isNaN(date.getTime()) ? "" : date.toLocaleTimeString();
}

/** One row in the live event feed: who did what, and what Traderz did about it. */
function EventRow({ event }: { event: TraderEvent }) {
  const actionTone =
    event.action === "BUY"
      ? "border-emerald-500/40 bg-emerald-500/10 text-emerald-300"
      : "border-rose-500/40 bg-rose-500/10 text-rose-300";
  const statusChip = event.followed
    ? { label: "COPIED ✓", tone: "border-emerald-500/40 bg-emerald-500/10 text-emerald-300" }
    : event.follow_detail.startsWith("blocked")
      ? { label: "BLOCKED", tone: "border-amber-500/40 bg-amber-500/10 text-amber-300" }
      : { label: "NOTIFIED", tone: "border-zinc-700 bg-zinc-900 text-zinc-400" };

  return (
    <li className="flex flex-wrap items-center gap-2 border-b border-zinc-800/60 py-2 text-xs">
      <span className="font-mono text-zinc-500">{timeLabel(event.timestamp)}</span>
      <span className="font-semibold text-zinc-200">{event.trader_name}</span>
      <span className={`rounded border px-1.5 py-0.5 font-bold ${actionTone}`}>{event.action}</span>
      <span className="font-mono font-semibold text-zinc-100">{event.ticker}</span>
      <span className="font-mono text-zinc-300">@ {event.price.toLocaleString()}</span>
      <span className={`rounded border px-1.5 py-0.5 text-[10px] font-bold ${statusChip.tone}`}>
        {statusChip.label}
      </span>
      <span className="basis-full text-[11px] text-zinc-500">{event.follow_detail}</span>
    </li>
  );
}

/** One watched trader: the auto-follow toggle, budget, and quick log form. */
function TraderCard({
  trader,
  watch,
}: {
  trader: WatchedTrader;
  watch: UseTraderWatch;
}) {
  const [budget, setBudget] = useState(trader.budget_amount > 0 ? String(trader.budget_amount) : "");
  const [ticker, setTicker] = useState("");
  const [action, setAction] = useState<"BUY" | "SELL">("BUY");
  const [price, setPrice] = useState("");

  const budgetValue = Number.parseFloat(budget);
  const budgetValid = Number.isFinite(budgetValue) && budgetValue > 0;

  const toggleFollow = () => {
    // Turning ON requires a budget; turning OFF keeps it remembered.
    void watch.setFollow(trader.id, !trader.auto_follow, budgetValid ? budgetValue : trader.budget_amount);
  };

  const submitEvent = () => {
    const parsedPrice = Number.parseFloat(price);
    if (!ticker.trim() || !Number.isFinite(parsedPrice) || parsedPrice <= 0) return;
    void watch
      .logEvent(trader.id, { ticker: ticker.trim().toUpperCase(), action, price: parsedPrice })
      .then(() => {
        setTicker("");
        setPrice("");
      });
  };

  return (
    <div className="rounded-lg border border-zinc-800 bg-zinc-950 p-4">
      <div className="mb-3 flex items-center justify-between gap-2">
        <div className="flex items-center gap-2">
          <span className="text-sm font-semibold text-zinc-100">{trader.name}</span>
          <span className="rounded border border-zinc-700 bg-zinc-900 px-1.5 py-0.5 text-[10px] font-medium uppercase text-zinc-400">
            {trader.asset_class}
          </span>
        </div>
        <button
          onClick={() => void watch.removeTrader(trader.id)}
          className="text-xs text-zinc-600 hover:text-rose-400"
          title="Stop watching this trader (removes their event history)"
        >
          Remove
        </button>
      </div>

      {/* auto-follow toggle + budget */}
      <div className="mb-3 flex flex-wrap items-center gap-2 rounded-md border border-zinc-800 bg-zinc-900/50 p-2.5">
        <button
          onClick={toggleFollow}
          disabled={watch.pending || (!trader.auto_follow && !budgetValid)}
          className={`relative h-5 w-9 rounded-full transition-colors disabled:opacity-40 ${
            trader.auto_follow ? "bg-emerald-500/80" : "bg-zinc-700"
          }`}
          title={
            trader.auto_follow
              ? "Auto-follow is ON — their trades are copied instantly with your budget (paper money)"
              : "Enter a budget, then switch ON to copy their trades automatically"
          }
          aria-label="Toggle auto-follow"
        >
          <span
            className={`absolute left-0.5 top-0.5 h-4 w-4 rounded-full bg-white transition-transform ${
              trader.auto_follow ? "translate-x-4" : ""
            }`}
          />
        </button>
        <span className={`text-xs font-semibold ${trader.auto_follow ? "text-emerald-300" : "text-zinc-400"}`}>
          {trader.auto_follow ? "Auto-follow ON" : "Notify only"}
        </span>
        <div className="ml-auto flex items-center gap-1.5">
          <span className="text-xs text-zinc-500">Budget $</span>
          <input
            value={budget}
            onChange={(e) => setBudget(e.target.value)}
            onBlur={() => {
              if (budgetValid && budgetValue !== trader.budget_amount) {
                void watch.setFollow(trader.id, trader.auto_follow, budgetValue);
              }
            }}
            placeholder="500"
            inputMode="decimal"
            className="w-24 rounded border border-zinc-700 bg-black px-2 py-1 text-right font-mono text-xs text-zinc-100 focus:border-sky-500 focus:outline-none"
            title="Dollars committed per copied trade"
          />
        </div>
      </div>

      {/* quick manual log: "I saw them trade" */}
      <div className="flex flex-wrap items-center gap-1.5">
        <input
          value={ticker}
          onChange={(e) => setTicker(e.target.value)}
          placeholder="AAPL or BTC/USDT"
          className="w-32 flex-1 rounded border border-zinc-700 bg-black px-2 py-1.5 font-mono text-xs text-zinc-100 focus:border-sky-500 focus:outline-none"
        />
        <select
          value={action}
          onChange={(e) => setAction(e.target.value as "BUY" | "SELL")}
          className="rounded border border-zinc-700 bg-black px-1.5 py-1.5 text-xs text-zinc-100 focus:border-sky-500 focus:outline-none"
        >
          <option value="BUY">Bought</option>
          <option value="SELL">Sold</option>
        </select>
        <input
          value={price}
          onChange={(e) => setPrice(e.target.value)}
          placeholder="price"
          inputMode="decimal"
          className="w-20 rounded border border-zinc-700 bg-black px-2 py-1.5 text-right font-mono text-xs text-zinc-100 focus:border-sky-500 focus:outline-none"
        />
        <button
          onClick={submitEvent}
          disabled={watch.pending}
          className="rounded border border-sky-500/40 bg-sky-500/10 px-3 py-1.5 text-xs font-semibold text-sky-300 hover:bg-sky-500/20 disabled:opacity-40"
          title="Log this trade — you'll be notified, and if auto-follow is on it's copied instantly"
        >
          Log trade
        </button>
      </div>
    </div>
  );
}

/** The Copy Trading tab: roster of watched traders + the live event feed. */
export default function TraderWatchPanel({ watch }: { watch: UseTraderWatch }) {
  const [name, setName] = useState("");
  const [assetClass, setAssetClass] = useState<"stock" | "crypto">("stock");

  const submitTrader = () => {
    if (!name.trim()) return;
    void watch.addTrader(name.trim(), assetClass).then(() => setName(""));
  };

  const positions = watch.feed?.open_copied_positions ?? [];

  return (
    <div className="flex flex-col gap-4">
      <section className="rounded-lg border border-zinc-800 bg-zinc-950 p-4">
        <h2 className="text-sm font-semibold text-zinc-100">Copy Trading — Trader Watch</h2>
        <p className="mt-1 text-xs leading-relaxed text-zinc-500">
          Add a trader you want to track, then log their buys/sells the moment you see them (each
          trader card has a quick form; automated feeds can push to the same API as a webhook).
          You&apos;ll be notified instantly. Flip <span className="text-emerald-400">Auto-follow</span>{" "}
          with a budget to mirror their trades the moment they&apos;re logged — copies run through the{" "}
          <span className="text-zinc-300">paper-trading engine</span> with your risk guard, so no real
          money moves.
        </p>

        {/* add-trader form */}
        <div className="mt-3 flex flex-wrap items-center gap-2">
          <input
            value={name}
            onChange={(e) => setName(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && submitTrader()}
            placeholder="Trader name (e.g. Dave, @cryptowhale)"
            className="w-64 rounded border border-zinc-700 bg-black px-2.5 py-1.5 text-xs text-zinc-100 focus:border-sky-500 focus:outline-none"
          />
          <select
            value={assetClass}
            onChange={(e) => setAssetClass(e.target.value as "stock" | "crypto")}
            className="rounded border border-zinc-700 bg-black px-2 py-1.5 text-xs text-zinc-100 focus:border-sky-500 focus:outline-none"
          >
            <option value="stock">Stocks</option>
            <option value="crypto">Crypto</option>
          </select>
          <button
            onClick={submitTrader}
            disabled={watch.pending || !name.trim()}
            className="rounded border border-emerald-500/40 bg-emerald-500/10 px-3 py-1.5 text-xs font-semibold text-emerald-300 hover:bg-emerald-500/20 disabled:opacity-40"
          >
            Watch trader
          </button>
        </div>
        {watch.error && (
          <p className="mt-2 rounded border border-rose-500/40 bg-rose-500/10 px-2.5 py-1.5 text-xs text-rose-300">
            {watch.error}
          </p>
        )}
      </section>

      {/* trader cards */}
      {watch.traders.length > 0 && (
        <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
          {watch.traders.map((trader) => (
            <TraderCard key={trader.id} trader={trader} watch={watch} />
          ))}
        </div>
      )}

      {/* open copied positions */}
      {positions.length > 0 && (
        <section className="rounded-lg border border-zinc-800 bg-zinc-950 p-4">
          <h3 className="mb-2 text-xs font-semibold uppercase tracking-wide text-zinc-500">
            Open copied positions
          </h3>
          <div className="flex flex-wrap gap-2">
            {positions.map((p) => (
              <span
                key={`${p.trader_id}-${p.ticker}`}
                className="rounded border border-sky-500/40 bg-sky-500/10 px-2 py-1 font-mono text-xs text-sky-300"
              >
                {p.ticker} · ${p.size.toLocaleString()} @ {p.entry.toLocaleString()}
              </span>
            ))}
          </div>
        </section>
      )}

      {/* live feed */}
      <section className="rounded-lg border border-zinc-800 bg-zinc-950 p-4">
        <h3 className="mb-1 text-xs font-semibold uppercase tracking-wide text-zinc-500">
          Trade event feed
        </h3>
        {watch.feed === null || watch.feed.events.length === 0 ? (
          <p className="py-4 text-center text-xs text-zinc-600">
            No events yet — log a watched trader&apos;s buy or sell above and it appears here
            instantly.
          </p>
        ) : (
          <ul>
            {watch.feed.events.map((event) => (
              <EventRow key={event.id} event={event} />
            ))}
          </ul>
        )}
      </section>
    </div>
  );
}
