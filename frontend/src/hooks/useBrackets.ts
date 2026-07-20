"use client";

import { useEffect, useRef, useState } from "react";

import { fetchBrackets } from "@/lib/api";
import type { BracketCard } from "@/lib/types";

const POLL_MS = 2000;

export function useBrackets(): BracketCard[] {
  const [brackets, setBrackets] = useState<BracketCard[]>([]);
  const mountedRef = useRef(true);

  useEffect(() => {
    mountedRef.current = true;
    const refresh = () => {
      fetchBrackets()
        .then((cards) => mountedRef.current && setBrackets(cards))
        .catch(() => undefined);
    };
    refresh();
    const interval = setInterval(refresh, POLL_MS);
    return () => {
      mountedRef.current = false;
      clearInterval(interval);
    };
  }, []);

  return brackets;
}
