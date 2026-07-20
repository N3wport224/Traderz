"use client";

import { useEffect, useRef, useState } from "react";

import { fetchEquityCurve, fetchSignals, fetchTrades, wsUrlFor } from "@/lib/api";
import type { EngineChannel, EquityPoint, Trade, TradeSignal } from "@/lib/types";

const MAX_SIGNALS = 200;
const POLL_MS = 4000;
const WS_RECONNECT_MS = 3000;

export interface EngineFeed {
  signals: TradeSignal[];
  equityCurve: EquityPoint[];
  trades: Trade[];
  connected: boolean;
}

export function useEngineFeed(channel: EngineChannel): EngineFeed {
  const [signals, setSignals] = useState<TradeSignal[]>([]);
  const [equityCurve, setEquityCurve] = useState<EquityPoint[]>([]);
  const [trades, setTrades] = useState<Trade[]>([]);
  const [connected, setConnected] = useState(false);
  const mountedRef = useRef(true);

  useEffect(() => {
    mountedRef.current = true;

    const refreshPersisted = () => {
      fetchEquityCurve(channel)
        .then((points) => mountedRef.current && setEquityCurve(points))
        .catch(() => undefined);
      fetchTrades(channel)
        .then((rows) => mountedRef.current && setTrades(rows))
        .catch(() => undefined);
    };

    fetchSignals(channel)
      .then((initial) => mountedRef.current && setSignals(initial.slice(-MAX_SIGNALS)))
      .catch(() => undefined);
    refreshPersisted();

    const pollInterval = setInterval(refreshPersisted, POLL_MS);

    let socket: WebSocket | null = null;
    let reconnectTimer: ReturnType<typeof setTimeout> | null = null;

    const connect = () => {
      if (!mountedRef.current) return;
      socket = new WebSocket(wsUrlFor(channel));

      socket.onopen = () => mountedRef.current && setConnected(true);
      socket.onclose = () => {
        if (!mountedRef.current) return;
        setConnected(false);
        reconnectTimer = setTimeout(connect, WS_RECONNECT_MS);
      };
      socket.onerror = () => socket?.close();
      socket.onmessage = (event: MessageEvent<string>) => {
        try {
          const signal = JSON.parse(event.data) as TradeSignal;
          if (!mountedRef.current) return;
          setSignals((prev) => [...prev, signal].slice(-MAX_SIGNALS));
          // A closed trade or a fresh circuit-breaker trip just landed in the DB —
          // refresh the persisted views a beat sooner than the regular poll.
          if (signal.action === "exit" || signal.action === "circuit_breaker_triggered") {
            refreshPersisted();
          }
        } catch {
          // ignore malformed frames
        }
      };
    };
    connect();

    return () => {
      mountedRef.current = false;
      clearInterval(pollInterval);
      if (reconnectTimer) clearTimeout(reconnectTimer);
      socket?.close();
    };
  }, [channel]);

  return { signals, equityCurve, trades, connected };
}
