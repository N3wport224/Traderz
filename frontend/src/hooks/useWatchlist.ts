"use client";

import { useCallback, useEffect, useRef, useState } from "react";

import { fetchWatchlist, updateWatchlist } from "@/lib/api";
import type { DataSourceMode } from "@/lib/types";

export interface UseWatchlist {
  /** Active ticker, or null before the first fetch resolves. */
  ticker: string | null;
  dataSourceMode: DataSourceMode | null;
  submit: (ticker: string) => Promise<void>;
  pending: boolean;
  error: string | null;
}

export function useWatchlist(): UseWatchlist {
  const [ticker, setTicker] = useState<string | null>(null);
  const [dataSourceMode, setDataSourceMode] = useState<DataSourceMode | null>(null);
  const [pending, setPending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const mountedRef = useRef(true);

  useEffect(() => {
    mountedRef.current = true;
    fetchWatchlist()
      .then((state) => {
        if (mountedRef.current) {
          setTicker(state.ticker);
          setDataSourceMode(state.data_source_mode);
        }
      })
      .catch(() => undefined);
    return () => {
      mountedRef.current = false;
    };
  }, []);

  const submit = useCallback(async (nextTicker: string) => {
    setPending(true);
    setError(null);
    try {
      const state = await updateWatchlist(nextTicker);
      if (mountedRef.current) {
        setTicker(state.ticker);
        setDataSourceMode(state.data_source_mode);
      }
    } catch (err) {
      if (mountedRef.current) {
        setError(err instanceof Error ? err.message : "Watchlist update failed");
      }
      throw err;
    } finally {
      if (mountedRef.current) setPending(false);
    }
  }, []);

  return { ticker, dataSourceMode, submit, pending, error };
}
