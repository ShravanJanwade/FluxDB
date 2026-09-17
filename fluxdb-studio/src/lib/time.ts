/** Time-range selection shared by the explorer, the query workspace and dashboards. */

export type RangeKey =
  | "15m"
  | "1h"
  | "3h"
  | "6h"
  | "12h"
  | "24h"
  | "7d"
  | "30d";

export const RANGES: { key: RangeKey; label: string; seconds: number }[] = [
  { key: "15m", label: "Last 15 minutes", seconds: 15 * 60 },
  { key: "1h", label: "Last hour", seconds: 60 * 60 },
  { key: "3h", label: "Last 3 hours", seconds: 3 * 60 * 60 },
  { key: "6h", label: "Last 6 hours", seconds: 6 * 60 * 60 },
  { key: "12h", label: "Last 12 hours", seconds: 12 * 60 * 60 },
  { key: "24h", label: "Last 24 hours", seconds: 24 * 60 * 60 },
  { key: "7d", label: "Last 7 days", seconds: 7 * 24 * 60 * 60 },
  { key: "30d", label: "Last 30 days", seconds: 30 * 24 * 60 * 60 },
];

export const DEFAULT_RANGE: RangeKey = "6h";

export type Window = { from: string; to: string; fromMs: number; toMs: number };

/**
 * Resolve a range key to absolute nanosecond bounds. Bounds are absolute rather
 * than relative because the SQL subset has no `now()`: the caller always sends
 * the window it wants, which also means a chart's axis and its data can never
 * disagree.
 */
export function resolveRange(key: RangeKey, nowMs = Date.now()): Window {
  const seconds =
    RANGES.find((range) => range.key === key)?.seconds ?? 6 * 3600;
  const toMs = nowMs;
  const fromMs = nowMs - seconds * 1000;
  return {
    from: msToNanos(fromMs),
    to: msToNanos(toMs),
    fromMs,
    toMs,
  };
}

export function rangeLabel(key: RangeKey): string {
  return RANGES.find((range) => range.key === key)?.label ?? "Last 6 hours";
}

/** Milliseconds to a decimal-nanosecond string, without losing precision to
 *  floating point at the scale of nanosecond epochs. */
export function msToNanos(milliseconds: number): string {
  return (BigInt(Math.round(milliseconds)) * 1_000_000n).toString();
}

export function nanosToMs(nanos: string): number {
  try {
    return Number(BigInt(nanos) / 1_000_000n);
  } catch {
    return Number.NaN;
  }
}

/** Nanosecond string for the current instant, for point timestamps. */
export function nowNanos(): string {
  return msToNanos(Date.now());
}

/** Retention presets offered when creating or editing a bucket. */
export const RETENTIONS: { label: string; seconds: number }[] = [
  { label: "Keep forever", seconds: 0 },
  { label: "1 day", seconds: 86_400 },
  { label: "7 days", seconds: 7 * 86_400 },
  { label: "30 days", seconds: 30 * 86_400 },
  { label: "90 days", seconds: 90 * 86_400 },
  { label: "1 year", seconds: 365 * 86_400 },
];
