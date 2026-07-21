"use client";

import { useCallback, useEffect, useRef, useState } from "react";

import {
  createTrader,
  deleteTrader,
  fetchTraderFeed,
  fetchTraders,
  logTraderEvent,
  updateTraderFollow,
} from "@/lib/api";
import type { TraderFeed, WatchedTrader } from "@/lib/types";

const FEED_POLL_MS = 4000;

export interface UseTraderWatch {
  traders: WatchedTrader[];
  feed: TraderFeed | null;
  error: string | null;
  pending: boolean;
  addTrader: (name: string, assetClass: "stock" | "crypto") => Promise<void>;
  removeTrader: (id: number) => Promise<void>;
  setFollow: (id: number, autoFollow: boolean, budgetAmount: number) => Promise<void>;
  logEvent: (
    id: number,
    event: { ticker: string; action: "BUY" | "SELL"; price: number; note?: string },
  ) => Promise<void>;
  clearError: () => void;
}

/** State + actions for the Copy Trading tab: the trader roster (refreshed
 * after every mutation) and the event feed (polled so webhook-sourced events
 * and auto-follow results appear without a manual refresh). */
export function useTraderWatch(): UseTraderWatch {
  const [traders, setTraders] = useState<WatchedTrader[]>([]);
  const [feed, setFeed] = useState<TraderFeed | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [pending, setPending] = useState(false);
  const mountedRef = useRef(true);

  const refreshTraders = useCallback(async () => {
    try {
      const list = await fetchTraders();
      if (mountedRef.current) setTraders(list);
    } catch {
      /* transient — the next poll retries */
    }
  }, []);

  const refreshFeed = useCallback(async () => {
    try {
      const next = await fetchTraderFeed();
      if (mountedRef.current) setFeed(next);
    } catch {
      /* transient — the next poll retries */
    }
  }, []);

  useEffect(() => {
    mountedRef.current = true;
    void refreshTraders();
    void refreshFeed();
    const timer = setInterval(() => void refreshFeed(), FEED_POLL_MS);
    return () => {
      mountedRef.current = false;
      clearInterval(timer);
    };
  }, [refreshTraders, refreshFeed]);

  const run = useCallback(
    async (action: () => Promise<unknown>) => {
      setPending(true);
      setError(null);
      try {
        await action();
        await refreshTraders();
        await refreshFeed();
      } catch (err) {
        if (mountedRef.current) setError(err instanceof Error ? err.message : "request failed");
      } finally {
        if (mountedRef.current) setPending(false);
      }
    },
    [refreshTraders, refreshFeed],
  );

  return {
    traders,
    feed,
    error,
    pending,
    addTrader: (name, assetClass) => run(() => createTrader(name, assetClass)),
    removeTrader: (id) => run(() => deleteTrader(id)),
    setFollow: (id, autoFollow, budgetAmount) => run(() => updateTraderFollow(id, autoFollow, budgetAmount)),
    logEvent: (id, event) => run(() => logTraderEvent(id, event)),
    clearError: () => setError(null),
  };
}
