import type { Interval } from "./fluxdb";

export const MINUTE = 60_000;
export const HOUR = 60 * MINUTE;
export const DAY = 24 * HOUR;

export type WindowKey = "1h" | "6h" | "24h" | "7d";

export interface WindowSpec {
  key: WindowKey;
  label: string;
  ms: number;
  /** The `$interval` charts use for this window. */
  interval: Interval;
}

export const WINDOWS: Record<WindowKey, WindowSpec> = {
  "1h": { key: "1h", label: "1H", ms: HOUR, interval: "1m" },
  "6h": { key: "6h", label: "6H", ms: 6 * HOUR, interval: "5m" },
  "24h": { key: "24h", label: "24H", ms: DAY, interval: "15m" },
  "7d": { key: "7d", label: "7D", ms: 7 * DAY, interval: "1h" },
};

const INTERVAL_MS: Record<Interval, number> = {
  "10s": 10_000,
  "30s": 30_000,
  "1m": MINUTE,
  "2m": 2 * MINUTE,
  "5m": 5 * MINUTE,
  "10m": 10 * MINUTE,
  "15m": 15 * MINUTE,
  "30m": 30 * MINUTE,
  "1h": HOUR,
  "3h": 3 * HOUR,
  "6h": 6 * HOUR,
  "12h": 12 * HOUR,
  "1d": DAY,
};

export function intervalMs(interval: Interval): number {
  return INTERVAL_MS[interval];
}

/** The window named in a query string, if this page offers it. */
export function pickWindow(value: string | null, allowed: WindowKey[], fallback: WindowKey): WindowSpec {
  const key = allowed.find((candidate) => candidate === value) ?? fallback;
  return WINDOWS[key];
}

export function alignDown(t: number, step: number): number {
  return Math.floor(t / step) * step;
}

export interface ZonedTime {
  /** Local hour with fraction, 0 to 24. */
  hour: number;
  /** 0 = Sunday. */
  weekday: number;
  /** 1 to 366. */
  dayOfYear: number;
  /** Milliseconds to add to UTC to get local wall-clock time. */
  offset: number;
}

const offsets = new Map<string, number>();
const formatters = new Map<string, Intl.DateTimeFormat>();

/** UTC offset of a time zone at an instant. Offsets change on the hour, so one lookup serves the hour. */
function zoneOffset(t: number, timeZone: string): number {
  const hourKey = `${timeZone}:${Math.floor(t / HOUR)}`;
  const cached = offsets.get(hourKey);
  if (cached !== undefined) return cached;
  let formatter = formatters.get(timeZone);
  if (!formatter) {
    formatter = new Intl.DateTimeFormat("en-US", {
      timeZone,
      hourCycle: "h23",
      year: "numeric",
      month: "numeric",
      day: "numeric",
      hour: "numeric",
      minute: "numeric",
    });
    formatters.set(timeZone, formatter);
  }
  const instant = alignDown(t, HOUR);
  const parts = Object.fromEntries(formatter.formatToParts(instant).map((part) => [part.type, part.value]));
  const wall = Date.UTC(+parts.year, +parts.month - 1, +parts.day, +parts.hour % 24, +parts.minute);
  const offset = wall - instant;
  if (offsets.size > 5_000) offsets.clear();
  offsets.set(hourKey, offset);
  return offset;
}

export function zoned(t: number, timeZone: string): ZonedTime {
  const offset = zoneOffset(t, timeZone);
  const local = new Date(t + offset);
  const startOfYear = Date.UTC(local.getUTCFullYear(), 0, 1);
  return {
    hour: local.getUTCHours() + local.getUTCMinutes() / 60 + local.getUTCSeconds() / 3600,
    weekday: local.getUTCDay(),
    dayOfYear: Math.floor((local.getTime() - startOfYear) / DAY) + 1,
    offset,
  };
}

/** The instant local midnight began, for the day containing `t`. */
export function startOfLocalDay(t: number, timeZone: string): number {
  const offset = zoneOffset(t, timeZone);
  const wallMidnight = alignDown(t + offset, DAY);
  // On a daylight-saving day the offset at midnight differs from the offset now.
  const offsetAtMidnight = zoneOffset(wallMidnight - offset, timeZone);
  return wallMidnight - offsetAtMidnight;
}
