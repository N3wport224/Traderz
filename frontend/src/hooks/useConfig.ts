"use client";

import { useCallback, useEffect, useRef, useState } from "react";

import { fetchConfig, updateMomentumConfig, updateSwingConfig } from "@/lib/api";
import type { MomentumConfig, StrategyConfig, SwingConfig } from "@/lib/types";

export interface UseConfig {
  config: StrategyConfig | null;
  error: string | null;
  saving: boolean;
  saveMomentum: (update: Partial<MomentumConfig>) => Promise<boolean>;
  saveSwing: (update: Partial<SwingConfig>) => Promise<boolean>;
}

export function useConfig(): UseConfig {
  const [config, setConfig] = useState<StrategyConfig | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const mountedRef = useRef(true);

  useEffect(() => {
    mountedRef.current = true;
    fetchConfig()
      .then((next) => mountedRef.current && setConfig(next))
      .catch(() => undefined);
    return () => {
      mountedRef.current = false;
    };
  }, []);

  const saveMomentum = useCallback(async (update: Partial<MomentumConfig>): Promise<boolean> => {
    setSaving(true);
    setError(null);
    try {
      const momentum = await updateMomentumConfig(update);
      if (mountedRef.current) setConfig((prev) => (prev ? { ...prev, momentum } : prev));
      return true;
    } catch (err) {
      if (mountedRef.current) setError(err instanceof Error ? err.message : "Failed to update momentum config");
      return false;
    } finally {
      if (mountedRef.current) setSaving(false);
    }
  }, []);

  const saveSwing = useCallback(async (update: Partial<SwingConfig>): Promise<boolean> => {
    setSaving(true);
    setError(null);
    try {
      const swing = await updateSwingConfig(update);
      if (mountedRef.current) setConfig((prev) => (prev ? { ...prev, swing } : prev));
      return true;
    } catch (err) {
      if (mountedRef.current) setError(err instanceof Error ? err.message : "Failed to update swing config");
      return false;
    } finally {
      if (mountedRef.current) setSaving(false);
    }
  }, []);

  return { config, error, saving, saveMomentum, saveSwing };
}
