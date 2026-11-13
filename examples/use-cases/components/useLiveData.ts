"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import type { ApiError } from "../lib/types";

/** Stop polling after this long without a pointer, key or scroll, so a forgotten tab lets FluxDB sleep. */
const IDLE_PAUSE_MS = 10 * 60_000;
const WAKE_RETRY_MS = 3_000;
const WAKE_GIVE_UP_MS = 3 * 60_000;
/** A first answer slower than this is almost always FluxDB starting from zero. */
const SLOW_MS = 2_500;

export type Phase = "loading" | "waking" | "ready" | "error";

export interface Live<T> {
  data: T | null;
  error: ApiError | null;
  phase: Phase;
  refreshing: boolean;
  wakingSince: number | null;
  paused: boolean;
  updatedAt: number | null;
  /** Fetch now; `fresh` skips the CDN's short cache, e.g. right after a write. */
  refresh: (fresh?: boolean) => void;
  resume: () => void;
}

/**
 * Fetch a dashboard endpoint and keep it current. The previous answer stays
 * on screen while the next loads, a sleeping FluxDB is retried until it is
 * up, and polling stops while the tab is hidden or idle.
 */
export function useLiveData<T>(url: string, refreshMs: number): Live<T> {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<ApiError | null>(null);
  const [phase, setPhase] = useState<Phase>("loading");
  const [refreshing, setRefreshing] = useState(false);
  const [wakingSince, setWakingSince] = useState<number | null>(null);
  const [paused, setPaused] = useState(false);
  const [updatedAt, setUpdatedAt] = useState<number | null>(null);
  const [tick, setTick] = useState(0);
  const fresh = useRef(false);
  const hasData = useRef(false);
  const pausedRef = useRef(false);
  const lastActivity = useRef(0);
  const wakeStart = useRef<number | null>(null);

  useEffect(() => {
    lastActivity.current = Date.now();
    const mark = () => {
      lastActivity.current = Date.now();
    };
    const events = ["pointermove", "pointerdown", "keydown", "scroll", "touchstart"] as const;
    for (const name of events) window.addEventListener(name, mark, { passive: true });
    const onVisible = () => {
      if (document.visibilityState === "visible" && !pausedRef.current) setTick((t) => t + 1);
    };
    document.addEventListener("visibilitychange", onVisible);
    return () => {
      for (const name of events) window.removeEventListener(name, mark);
      document.removeEventListener("visibilitychange", onVisible);
    };
  }, []);

  useEffect(() => {
    let cancelled = false;
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;
    let slowTimer: ReturnType<typeof setTimeout> | undefined;

    const later = (ms: number) => {
      timer = setTimeout(() => {
        if (cancelled || document.visibilityState !== "visible") return;
        if (Date.now() - lastActivity.current > IDLE_PAUSE_MS) {
          pausedRef.current = true;
          setPaused(true);
          return;
        }
        setTick((t) => t + 1);
      }, ms);
    };

    const waking = () => {
      const since = wakeStart.current ?? Date.now();
      wakeStart.current = since;
      setWakingSince(since);
      if (Date.now() - since > WAKE_GIVE_UP_MS) {
        setError({ error: "waking", message: "FluxDB did not answer for three minutes. Try again in a moment." });
        setPhase(hasData.current ? "ready" : "error");
        return;
      }
      if (!hasData.current) setPhase("waking");
      timer = setTimeout(() => !cancelled && setTick((t) => t + 1), WAKE_RETRY_MS);
    };

    async function run() {
      setRefreshing(true);
      if (!hasData.current) {
        slowTimer = setTimeout(() => {
          if (cancelled) return;
          wakeStart.current ??= Date.now() - SLOW_MS;
          setWakingSince(wakeStart.current);
          setPhase((p) => (p === "loading" ? "waking" : p));
        }, SLOW_MS);
      }
      const target = fresh.current ? `${url}${url.includes("?") ? "&" : "?"}fresh=${Date.now()}` : url;
      fresh.current = false;
      try {
        const response = await fetch(target, { signal: controller.signal, cache: "no-store" });
        const body = (await response.json().catch(() => null)) as T | ApiError | null;
        if (cancelled) return;
        const apiError = !response.ok
          ? ((body as ApiError | null) ?? { error: "failed", message: `The server answered HTTP ${response.status}` })
          : null;
        if (!apiError) {
          hasData.current = true;
          wakeStart.current = null;
          setData(body as T);
          setError(null);
          setPhase("ready");
          setWakingSince(null);
          setUpdatedAt(Date.now());
          later(refreshMs);
        } else if (apiError.error === "waking" || response.status === 504) {
          waking();
        } else {
          setError(apiError);
          setPhase(hasData.current ? "ready" : "error");
          later(Math.max(refreshMs, 30_000));
        }
      } catch {
        if (cancelled || controller.signal.aborted) return;
        // The browser lost the connection; treat it like a slow start and retry.
        waking();
      } finally {
        clearTimeout(slowTimer);
        if (!cancelled) setRefreshing(false);
      }
    }

    run();
    return () => {
      cancelled = true;
      controller.abort();
      clearTimeout(timer);
      clearTimeout(slowTimer);
    };
  }, [url, tick, refreshMs]);

  const refresh = useCallback((skipCache = false) => {
    fresh.current = skipCache;
    setTick((t) => t + 1);
  }, []);

  const resume = useCallback(() => {
    lastActivity.current = Date.now();
    pausedRef.current = false;
    setPaused(false);
    setTick((t) => t + 1);
  }, []);

  return { data, error, phase, refreshing, wakingSince, paused, updatedAt, refresh, resume };
}
