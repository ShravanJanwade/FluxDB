/** Display helpers. Every number the console shows goes through one of these. */

import type { FieldValue } from "./types";

const compact = new Intl.NumberFormat("en", {
  notation: "compact",
  maximumFractionDigits: 1,
});
const plain = new Intl.NumberFormat("en");
const precise = new Intl.NumberFormat("en", { maximumFractionDigits: 2 });

export function count(value: number): string {
  if (!Number.isFinite(value)) return "—";
  return value >= 10_000 ? compact.format(value) : plain.format(value);
}

export function decimal(value: number): string {
  if (!Number.isFinite(value)) return "—";
  const magnitude = Math.abs(value);
  if (magnitude !== 0 && magnitude < 0.01) return value.toExponential(1);
  if (magnitude >= 100_000) return compact.format(value);
  return precise.format(value);
}

export function bytes(value: number): string {
  if (!Number.isFinite(value) || value < 0) return "—";
  if (value === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const exponent = Math.min(
    units.length - 1,
    Math.floor(Math.log(value) / Math.log(1024)),
  );
  const scaled = value / 1024 ** exponent;
  return `${scaled >= 100 || exponent === 0 ? Math.round(scaled) : precise.format(scaled)} ${units[exponent]}`;
}

export function milliseconds(value: number): string {
  if (!Number.isFinite(value)) return "—";
  if (value < 1) return `${(value * 1000).toFixed(0)} µs`;
  if (value < 1000) return `${precise.format(value)} ms`;
  return `${precise.format(value / 1000)} s`;
}

/** A retention period as a person would say it. */
export function duration(seconds: number): string {
  if (seconds <= 0) return "Forever";
  const units: [number, string][] = [
    [86_400 * 365, "year"],
    [86_400 * 30, "month"],
    [86_400 * 7, "week"],
    [86_400, "day"],
    [3_600, "hour"],
    [60, "minute"],
    [1, "second"],
  ];
  for (const [size, name] of units) {
    if (seconds >= size) {
      const amount = Math.round(seconds / size);
      return `${amount} ${name}${amount === 1 ? "" : "s"}`;
    }
  }
  return `${seconds} seconds`;
}

export function uptime(seconds: number): string {
  if (!Number.isFinite(seconds) || seconds < 0) return "—";
  const days = Math.floor(seconds / 86_400);
  const hours = Math.floor((seconds % 86_400) / 3_600);
  const minutes = Math.floor((seconds % 3_600) / 60);
  if (days > 0) return `${days}d ${hours}h`;
  if (hours > 0) return `${hours}h ${minutes}m`;
  return `${minutes}m ${Math.floor(seconds % 60)}s`;
}

const dateTime = new Intl.DateTimeFormat(undefined, {
  dateStyle: "medium",
  timeStyle: "short",
});
const timeOnly = new Intl.DateTimeFormat(undefined, {
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
});
const dayMonth = new Intl.DateTimeFormat(undefined, {
  month: "short",
  day: "numeric",
});

export function timestamp(milliseconds: number): string {
  if (!Number.isFinite(milliseconds) || milliseconds <= 0) return "—";
  return dateTime.format(new Date(milliseconds));
}

/** Decimal-nanosecond string to a local date and time. */
export function nanosToLocal(nanos: string): string {
  const value = Number(BigInt(nanos) / 1_000_000n);
  return Number.isFinite(value) ? dateTime.format(new Date(value)) : "—";
}

/** Axis label for a bucket boundary, at a granularity that suits the span. */
export function axisLabel(milliseconds: number, spanMs: number): string {
  const date = new Date(milliseconds);
  if (spanMs > 3 * 86_400_000) return dayMonth.format(date);
  if (spanMs > 86_400_000)
    return `${dayMonth.format(date)} ${timeOnly.format(date).slice(0, 5)}`;
  return timeOnly.format(date).slice(0, 5);
}

export function relative(milliseconds: number): string {
  if (!Number.isFinite(milliseconds) || milliseconds <= 0) return "never";
  const delta = Date.now() - milliseconds;
  if (delta < 0) {
    const ahead = Math.abs(delta);
    if (ahead < 60_000) return "in under a minute";
    if (ahead < 3_600_000) return `in ${Math.round(ahead / 60_000)} min`;
    if (ahead < 86_400_000) return `in ${Math.round(ahead / 3_600_000)} h`;
    return `in ${Math.round(ahead / 86_400_000)} days`;
  }
  if (delta < 10_000) return "just now";
  if (delta < 60_000) return `${Math.floor(delta / 1000)}s ago`;
  if (delta < 3_600_000) return `${Math.floor(delta / 60_000)}m ago`;
  if (delta < 86_400_000) return `${Math.floor(delta / 3_600_000)}h ago`;
  if (delta < 30 * 86_400_000) return `${Math.floor(delta / 86_400_000)}d ago`;
  return timestamp(milliseconds);
}

/** A stored field value, rendered for a table cell. */
export function fieldText(value: FieldValue | null | undefined): string {
  if (value === null || value === undefined) return "—";
  if (typeof value === "object" && "integer" in value) return value.integer;
  if (typeof value === "boolean") return value ? "true" : "false";
  if (typeof value === "number") return decimal(value);
  return value;
}

/** Numeric view of a field value, for charting. Strings and booleans have none. */
export function fieldNumber(
  value: FieldValue | null | undefined,
): number | null {
  if (value === null || value === undefined) return null;
  if (typeof value === "number") return value;
  if (typeof value === "object" && "integer" in value) {
    const parsed = Number(value.integer);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

/** A query-result cell, which may be a stringified integer or timestamp. */
export function cellText(value: unknown, column: string): string {
  if (value === null || value === undefined) return "—";
  if (column === "time" && typeof value === "string" && /^\d+$/.test(value)) {
    return nanosToLocal(value);
  }
  if (typeof value === "number") return decimal(value);
  if (typeof value === "boolean") return value ? "true" : "false";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

export function cellNumber(value: unknown): number | null {
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  if (typeof value === "string" && value !== "") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

/** Percentage of a quota, clamped so a bar never overflows its track. */
export function percentOf(used: number, limit: number): number {
  if (!Number.isFinite(used) || !Number.isFinite(limit) || limit <= 0) return 0;
  return Math.min(100, Math.max(0, (used / limit) * 100));
}

export function plural(amount: number, singular: string, pluralForm?: string) {
  return `${count(amount)} ${amount === 1 ? singular : (pluralForm ?? `${singular}s`)}`;
}

/** Initials for an avatar, from a display name or email address. */
export function initials(nameOrEmail: string): string {
  const source = nameOrEmail.includes("@")
    ? nameOrEmail.split("@")[0]
    : nameOrEmail;
  const words = source.split(/[\s._-]+/).filter(Boolean);
  if (words.length === 0) return "?";
  if (words.length === 1) return words[0].slice(0, 2).toUpperCase();
  return (words[0][0] + words[words.length - 1][0]).toUpperCase();
}
