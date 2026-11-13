/** Number and time formatting shared by the dashboards. */

const formatters = new Map<string, Intl.NumberFormat>();

function numberFormat(options: Intl.NumberFormatOptions): Intl.NumberFormat {
  const key = JSON.stringify(options);
  let formatter = formatters.get(key);
  if (!formatter) {
    formatter = new Intl.NumberFormat("en-US", options);
    formatters.set(key, formatter);
  }
  return formatter;
}

export function fixed(value: number, digits = 1): string {
  return numberFormat({ minimumFractionDigits: digits, maximumFractionDigits: digits }).format(value);
}

export function whole(value: number): string {
  return numberFormat({ maximumFractionDigits: 0 }).format(value);
}

/** 1,284 / 12.9K / 4.2M */
export function compact(value: number): string {
  if (Math.abs(value) < 10_000) return whole(value);
  return numberFormat({ notation: "compact", maximumFractionDigits: 1 }).format(value);
}

export function usd(value: number, digits = 2): string {
  return numberFormat({ style: "currency", currency: "USD", minimumFractionDigits: digits, maximumFractionDigits: digits }).format(value);
}

/** Prices keep the precision their size calls for. */
export function price(value: number): string {
  return usd(value, Math.abs(value) >= 1 ? 2 : 4);
}

export function signedPercent(value: number, digits = 2): string {
  const sign = value > 0 ? "+" : value < 0 ? "−" : "";
  return `${sign}${fixed(Math.abs(value), digits)}%`;
}

const dateFormats = new Map<string, Intl.DateTimeFormat>();

export function formatTime(t: number, options: Intl.DateTimeFormatOptions, timeZone?: string): string {
  const key = `${timeZone ?? ""}|${JSON.stringify(options)}`;
  let formatter = dateFormats.get(key);
  if (!formatter) {
    formatter = new Intl.DateTimeFormat("en-US", { ...options, timeZone });
    dateFormats.set(key, formatter);
  }
  return formatter.format(t);
}

/** "3:05 PM", or "Nov 12, 3:05 PM" when the range spans days. */
export function clock(t: number, timeZone?: string, withDay = false): string {
  return formatTime(
    t,
    withDay ? { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" } : { hour: "numeric", minute: "2-digit" },
    timeZone,
  );
}

export function ago(ms: number): string {
  const seconds = Math.max(0, Math.round(ms / 1000));
  if (seconds < 10) return "just now";
  if (seconds < 60) return `${seconds} s ago`;
  const minutes = Math.round(seconds / 60);
  if (minutes < 60) return `${minutes} min ago`;
  return `${Math.round(minutes / 60)} h ago`;
}

export function milliseconds(ms: number): string {
  if (ms < 1) return `${fixed(ms, 2)} ms`;
  if (ms < 10) return `${fixed(ms, 1)} ms`;
  if (ms < 1000) return `${whole(ms)} ms`;
  return `${fixed(ms / 1000, 1)} s`;
}
