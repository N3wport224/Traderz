"use client";

import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import type { EquityPoint } from "@/lib/types";

interface EquityPanelProps {
  title: string;
  color: string;
  data: EquityPoint[];
}

function formatTick(timestamp: string): string {
  const date = new Date(timestamp);
  if (Number.isNaN(date.getTime())) return "";
  return date.toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit" });
}

function EquityPanel({ title, color, data }: EquityPanelProps) {
  const latest = data.length > 0 ? data[data.length - 1].equity : 0;

  return (
    <div className="flex flex-1 flex-col rounded-lg border border-zinc-800 bg-zinc-950/60 p-4">
      <div className="mb-2 flex items-center justify-between">
        <h3 className="text-sm font-medium text-zinc-300">{title}</h3>
        <span className={`font-mono text-sm ${latest >= 0 ? "text-emerald-400" : "text-rose-400"}`}>
          {latest >= 0 ? "+" : ""}
          {latest.toFixed(2)}
        </span>
      </div>
      <div className="h-40 w-full">
        {data.length === 0 ? (
          <div className="flex h-full items-center justify-center text-xs text-zinc-600">
            No closed trades yet
          </div>
        ) : (
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={data} margin={{ top: 4, right: 8, bottom: 0, left: -20 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#27272a" />
              <XAxis dataKey="timestamp" tickFormatter={formatTick} stroke="#52525b" fontSize={11} />
              <YAxis stroke="#52525b" fontSize={11} />
              <Tooltip
                contentStyle={{ background: "#18181b", border: "1px solid #3f3f46", fontSize: 12 }}
                labelFormatter={(label) => formatTick(String(label))}
                formatter={(value) => [Number(value).toFixed(2), "equity"]}
              />
              <Line type="monotone" dataKey="equity" stroke={color} strokeWidth={2} dot={false} />
            </LineChart>
          </ResponsiveContainer>
        )}
      </div>
    </div>
  );
}

interface EquityChartProps {
  momentumEquity: EquityPoint[];
  swingEquity: EquityPoint[];
}

export default function EquityChart({ momentumEquity, swingEquity }: EquityChartProps) {
  return (
    <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
      <EquityPanel title="Momentum Engine — Equity" color="#34d399" data={momentumEquity} />
      <EquityPanel title="Swing Engine — Equity" color="#38bdf8" data={swingEquity} />
    </div>
  );
}
