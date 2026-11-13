"use client";

import { useEffect, useState } from "react";
import { milliseconds } from "../lib/format";
import type { ApiError } from "../lib/types";

interface Status {
  project: string;
  buckets: { useCase: string; name: string; present: boolean }[];
  totalMs: number;
}

/**
 * Asks the backend which FluxDB project it is connected to. Loading the home
 * page therefore wakes a sleeping FluxDB before the visitor opens a dashboard.
 */
export function StatusPill() {
  const [status, setStatus] = useState<Status | null>(null);
  const [error, setError] = useState<ApiError | null>(null);
  const [waking, setWaking] = useState(false);

  useEffect(() => {
    let cancelled = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const started = Date.now();
    const slow = setTimeout(() => !cancelled && setWaking(true), 2_000);
    async function check() {
      try {
        const response = await fetch("/api/status", { cache: "no-store" });
        const body = await response.json().catch(() => null);
        if (cancelled) return;
        if (response.ok) {
          setStatus(body as Status);
          setWaking(false);
          clearTimeout(slow);
        } else if ((body as ApiError | null)?.error === "waking" || response.status === 504) {
          setWaking(true);
          if (Date.now() - started < 180_000) timer = setTimeout(check, 3_000);
        } else {
          setError(body as ApiError);
          clearTimeout(slow);
        }
      } catch {
        if (!cancelled && Date.now() - started < 180_000) timer = setTimeout(check, 3_000);
      }
    }
    check();
    return () => {
      cancelled = true;
      clearTimeout(timer);
      clearTimeout(slow);
    };
  }, []);

  if (status) {
    const missing = status.buckets.filter((b) => !b.present);
    return (
      <div className="status-pill" role="status">
        <span className={`status-dot ${missing.length ? "bad" : "ok"}`} />
        {missing.length
          ? `Connected to project “${status.project}”, but bucket ${missing.map((b) => b.name).join(", ")} is missing`
          : `Connected to FluxDB · project “${status.project}” · ${status.buckets.length} buckets · answered in ${milliseconds(status.totalMs)}`}
      </div>
    );
  }
  if (error) {
    return (
      <div className="status-pill" role="status">
        <span className="status-dot bad" />
        {error.message}
      </div>
    );
  }
  return (
    <div className="status-pill" role="status">
      <span className="status-dot wait" />
      {waking ? "Waking FluxDB: it sleeps when nobody is using it, back in under a minute…" : "Connecting to FluxDB…"}
    </div>
  );
}
