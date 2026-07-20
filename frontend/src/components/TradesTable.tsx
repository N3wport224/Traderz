import type { Trade } from "@/lib/types";

function formatTime(timestamp: string): string {
  const date = new Date(timestamp);
  if (Number.isNaN(date.getTime())) return timestamp;
  return date.toLocaleString(undefined, { month: "short", day: "2-digit", hour: "2-digit", minute: "2-digit" });
}

interface TradesTableProps {
  title: string;
  trades: Trade[];
}

export default function TradesTable({ title, trades }: TradesTableProps) {
  const ordered = [...trades].reverse().slice(0, 20);

  return (
    <div className="flex flex-1 flex-col rounded-lg border border-zinc-800 bg-zinc-950/60 p-4">
      <h3 className="mb-3 text-sm font-medium text-zinc-300">{title}</h3>
      {ordered.length === 0 ? (
        <div className="flex flex-1 items-center justify-center py-8 text-xs text-zinc-600">No trades recorded yet</div>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full min-w-[420px] border-collapse text-left font-mono text-xs">
            <thead>
              <tr className="border-b border-zinc-800 text-zinc-500">
                <th className="py-1.5 pr-3 font-normal">Exit</th>
                <th className="py-1.5 pr-3 font-normal">Entry &rarr; Exit</th>
                <th className="py-1.5 pr-3 font-normal">Size</th>
                <th className="py-1.5 pr-3 font-normal">Fees</th>
                <th className="py-1.5 pr-3 font-normal" title="Dollars lost to fill-price degradation (entry + exit)">
                  Slip
                </th>
                <th className="py-1.5 font-normal text-right">Net P&amp;L</th>
              </tr>
            </thead>
            <tbody>
              {ordered.map((trade) => (
                <tr key={trade.id} className="border-b border-zinc-900 text-zinc-300">
                  <td className="py-1.5 pr-3 text-zinc-500">{formatTime(trade.exit_timestamp)}</td>
                  <td className="py-1.5 pr-3">
                    ${trade.entry_price.toFixed(2)} &rarr; ${trade.exit_price.toFixed(2)}
                  </td>
                  <td className="py-1.5 pr-3">${trade.position_size.toFixed(0)}</td>
                  <td className="py-1.5 pr-3 text-zinc-500">${trade.fees.toFixed(2)}</td>
                  <td
                    className="py-1.5 pr-3 text-amber-400/80"
                    title={`Requested $${trade.requested_price.toFixed(2)}, filled $${trade.actual_filled_price.toFixed(2)}`}
                  >
                    ${trade.slippage_cost.toFixed(2)}
                  </td>
                  <td className={`py-1.5 text-right ${trade.net_profit >= 0 ? "text-emerald-400" : "text-rose-400"}`}>
                    {trade.net_profit >= 0 ? "+" : ""}
                    {trade.net_profit.toFixed(2)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
