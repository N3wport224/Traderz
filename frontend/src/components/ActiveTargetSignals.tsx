import type { BracketCard } from "@/lib/types";

function money(value: number): string {
  return `$${value.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

function formatRR(ratio: number | null): string {
  if (ratio === null || !Number.isFinite(ratio)) return "—";
  return `1:${ratio.toFixed(1)} RR`;
}

/** Where the live price sits between SL (0%) and TP (100%), for the meter. */
function bracketProgress(card: BracketCard): number {
  const span =
    card.side === "long"
      ? card.take_profit_price - card.stop_loss_price
      : card.stop_loss_price - card.take_profit_price;
  if (span <= 0) return 50;
  const travelled =
    card.side === "long"
      ? card.current_price - card.stop_loss_price
      : card.stop_loss_price - card.current_price;
  return Math.min(100, Math.max(0, (travelled / span) * 100));
}

function BracketCardView({ card }: { card: BracketCard }) {
  const profitable = card.unrealized_pct >= 0;
  const sideTone =
    card.side === "long"
      ? "border-emerald-500/40 bg-emerald-500/10 text-emerald-300"
      : "border-rose-500/40 bg-rose-500/10 text-rose-300";

  return (
    <div className="flex flex-col gap-2 rounded-lg border border-zinc-800 bg-zinc-950/60 p-3">
      <div className="flex items-center gap-2">
        <span className="font-mono text-sm font-semibold text-zinc-100">{card.ticker}</span>
        <span className={`rounded-full border px-2 py-0.5 text-[10px] font-semibold uppercase ${sideTone}`}>
          {card.side}
        </span>
        <span className="text-[10px] uppercase tracking-wide text-zinc-500">{card.engine_type}</span>
        <span
          className="ml-auto rounded-md border border-sky-500/30 bg-sky-500/10 px-2 py-0.5 font-mono text-[11px] font-semibold text-sky-300"
          title="Risk-to-reward ratio of the bracket (reward distance vs. risk distance from entry)"
        >
          {formatRR(card.risk_reward_ratio)}
        </span>
      </div>

      <div className="flex items-baseline justify-between font-mono text-xs">
        <span className="text-zinc-500">
          Entry <span className="text-zinc-200">{money(card.entry_price)}</span>
        </span>
        <span className="text-zinc-500">
          Live{" "}
          <span className={profitable ? "text-emerald-400" : "text-rose-400"}>
            {money(card.current_price)} ({card.unrealized_pct >= 0 ? "+" : ""}
            {card.unrealized_pct.toFixed(2)}%)
          </span>
        </span>
      </div>

      {/* SL <- live price -> TP meter */}
      <div className="relative h-1.5 rounded-full bg-gradient-to-r from-rose-500/50 via-zinc-700 to-emerald-500/50">
        <span
          className="absolute top-1/2 h-3 w-1 -translate-y-1/2 rounded-sm bg-zinc-100"
          style={{ left: `calc(${bracketProgress(card)}% - 2px)` }}
        />
      </div>

      <div className="flex items-baseline justify-between font-mono text-xs">
        <span className="text-rose-400" title="Stop loss — risk remaining from the live price">
          SL {money(card.stop_loss_price)}{" "}
          <span className="text-rose-400/70">({card.sl_distance_pct.toFixed(2)}% risk)</span>
        </span>
        <span className="text-emerald-400" title="Take profit — distance remaining from the live price">
          TP {money(card.take_profit_price)}{" "}
          <span className="text-emerald-400/70">({card.tp_distance_pct.toFixed(2)}% away)</span>
        </span>
      </div>
    </div>
  );
}

export default function ActiveTargetSignals({ brackets }: { brackets: BracketCard[] }) {
  return (
    <div className="rounded-lg border border-zinc-800 bg-zinc-950/60 p-4">
      <h3 className="mb-3 text-sm font-medium text-zinc-300">
        Active Target Signals
        <span className="ml-2 text-xs font-normal text-zinc-500">
          live bracket orders — entry, stop loss, take profit
        </span>
      </h3>
      {brackets.length === 0 ? (
        <div className="py-6 text-center text-xs text-zinc-600">
          No open positions — brackets appear here the moment an entry fills
        </div>
      ) : (
        <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
          {brackets.map((card) => (
            <BracketCardView key={card.order_id} card={card} />
          ))}
        </div>
      )}
    </div>
  );
}
