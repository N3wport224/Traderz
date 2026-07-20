"use client";

import { useCallback, useEffect, useRef, useState } from "react";

import { fetchTelemetry } from "@/lib/api";
import type { TelemetryStats } from "@/lib/types";

const POLL_MS = 2000;

export interface UseTelemetry {
  telemetry: TelemetryStats | null;
  /** True when the last poll failed — the backend itself is unreachable. */
  unreachable: boolean;
}

export function useTelemetry(): UseTelemetry {
  const [telemetry, setTelemetry] = useState<TelemetryStats | null>(null);
  const [unreachable, setUnreachable] = useState(false);
  const mountedRef = useRef(true);

  const refresh = useCallback(() => {
    fetchTelemetry()
      .then((next) => {
        if (mountedRef.current) {
          setTelemetry(next);
          setUnreachable(false);
        }
      })
      .catch(() => {
        if (mountedRef.current) setUnreachable(true);
      });
  }, []);

  useEffect(() => {
    mountedRef.current = true;
    refresh();
    const interval = setInterval(refresh, POLL_MS);
    return () => {
      mountedRef.current = false;
      clearInterval(interval);
    };
  }, [refresh]);

  return { telemetry, unreachable };
}
