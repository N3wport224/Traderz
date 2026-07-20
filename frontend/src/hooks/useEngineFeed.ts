"use client";

import { useEffect, useRef, useState } from "react";

import { fetchEquityCurve, fetchSignals, wsUrlFor } from "@/lib/api";
import type { EngineChannel, EquityPoint, TradeSignal } from "@/lib/types";

const MAX_SIGNALS = 200;
const EQUITY_POLL_MS = 4000;
const WS_RECONNECT_MS = 3000;

export interface EngineFeed {
  signals: TradeSignal[];
  equityCurve: EquityPoint[];
  connected: boolean;
}

export function useEngineFeed(channel: EngineChannel): EngineFeed {
  const [signals, setSignals] = useState<TradeSignal[]>([]);
  const [equityCurve, setEquityCurve] = useState<EquityPoint[]>([]);
  const [connected, setConnected] = useState(false);
  const mountedRef = useRef(true);

  useEffect(() => {
    mountedRef.current = true;

    fetchSignals(channel)
      .then((initial) => mountedRef.current && setSignals(initial.slice(-MAX_SIGNALS)))
      .catch(() => undefined);
    fetchEquityCurve(channel)
      .then((initial) => mountedRef.current && setEquityCurve(initial))
      .catch(() => undefined);

    const equityInterval = setInterval(() => {
      fetchEquityCurve(channel)
        .then((points) => mountedRef.current && setEquityCurve(points))
        .catch(() => undefined);
    }, EQUITY_POLL_MS);

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
        } catch {
          // ignore malformed frames
        }
      };
    };
    connect();

    return () => {
      mountedRef.current = false;
      clearInterval(equityInterval);
      if (reconnectTimer) clearTimeout(reconnectTimer);
      socket?.close();
    };
  }, [channel]);

  return { signals, equityCurve, connected };
}
