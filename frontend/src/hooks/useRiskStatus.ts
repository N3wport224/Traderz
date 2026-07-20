"use client";

import { useCallback, useEffect, useRef, useState } from "react";

import { engageKillSwitch, fetchRiskStatus, pauseSystem, resetRiskGuard, resumeSystem } from "@/lib/api";
import type { RiskStatus } from "@/lib/types";

const POLL_MS = 3000;

export interface UseRiskStatus {
  status: RiskStatus | null;
  refresh: () => void;
  pause: () => Promise<void>;
  resume: () => Promise<void>;
  kill: () => Promise<void>;
  resetGuard: () => Promise<void>;
  actionPending: boolean;
}

export function useRiskStatus(): UseRiskStatus {
  const [status, setStatus] = useState<RiskStatus | null>(null);
  const [actionPending, setActionPending] = useState(false);
  const mountedRef = useRef(true);

  const refresh = useCallback(() => {
    fetchRiskStatus()
      .then((next) => mountedRef.current && setStatus(next))
      .catch(() => undefined);
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

  const pause = useCallback(async () => {
    setActionPending(true);
    try {
      const next = await pauseSystem();
      if (mountedRef.current) setStatus(next);
    } finally {
      if (mountedRef.current) setActionPending(false);
    }
  }, []);

  const resume = useCallback(async () => {
    setActionPending(true);
    try {
      const next = await resumeSystem();
      if (mountedRef.current) setStatus(next);
    } finally {
      if (mountedRef.current) setActionPending(false);
    }
  }, []);

  const kill = useCallback(async () => {
    setActionPending(true);
    try {
      const next = await engageKillSwitch();
      if (mountedRef.current) setStatus(next);
    } finally {
      if (mountedRef.current) setActionPending(false);
    }
  }, []);

  const resetGuard = useCallback(async () => {
    setActionPending(true);
    try {
      const next = await resetRiskGuard();
      if (mountedRef.current) setStatus(next);
    } finally {
      if (mountedRef.current) setActionPending(false);
    }
  }, []);

  return { status, refresh, pause, resume, kill, resetGuard, actionPending };
}
