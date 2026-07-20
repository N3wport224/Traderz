interface DataDisconnectedBannerProps {
  tickers: string[];
  streams: Record<string, { state: string; disconnect_count: number }>;
}

export default function DataDisconnectedBanner({ tickers, streams }: DataDisconnectedBannerProps) {
  const reconnecting = Object.entries(streams)
    .filter(([, s]) => s.state !== "connected")
    .map(([name]) => name);

  return (
    <div
      role="alert"
      className="flex flex-wrap items-center gap-3 rounded-md border border-rose-500/50 bg-rose-950/60 px-4 py-3"
    >
      <span className="relative flex h-2.5 w-2.5">
        <span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-rose-400 opacity-75" />
        <span className="relative inline-flex h-2.5 w-2.5 rounded-full bg-rose-500" />
      </span>
      <span className="text-sm font-semibold uppercase tracking-wide text-rose-300">Data disconnected</span>
      <span className="text-sm text-rose-200/90">
        Live market data lost for {tickers.length > 0 ? tickers.join(", ") : "one or more tickers"}. Engines are
        frozen on affected tickers until stream integrity is re-verified
        {reconnecting.length > 0 ? ` — reconnecting: ${reconnecting.join(", ")}` : ""}.
      </span>
    </div>
  );
}
