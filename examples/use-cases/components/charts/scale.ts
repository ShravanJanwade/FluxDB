import { formatTime } from "../../lib/format";
import { DAY, HOUR, MINUTE, zoned } from "../../lib/time";

export type Scale = (value: number) => number;

export function linear(domain: [number, number], range: [number, number]): Scale {
  const [d0, d1] = domain;
  const [r0, r1] = range;
  const k = d1 === d0 ? 0 : (r1 - r0) / (d1 - d0);
  return (value) => r0 + (value - d0) * k;
}

/** Round-number ticks that cover [min, max]; the domain widens to the outer ticks. */
export function niceTicks(min: number, max: number, count = 4): { ticks: number[]; domain: [number, number] } {
  if (!Number.isFinite(min) || !Number.isFinite(max)) return { ticks: [0, 1], domain: [0, 1] };
  if (min === max) {
    const pad = Math.abs(min) * 0.1 || 1;
    min -= pad;
    max += pad;
  }
  const raw = (max - min) / count;
  const magnitude = 10 ** Math.floor(Math.log10(raw));
  const normalized = raw / magnitude;
  const step = (normalized <= 1 ? 1 : normalized <= 2 ? 2 : normalized <= 2.5 ? 2.5 : normalized <= 5 ? 5 : 10) * magnitude;
  const low = Math.floor(min / step + 1e-9) * step;
  const high = Math.ceil(max / step - 1e-9) * step;
  const ticks: number[] = [];
  for (let i = 0; low + i * step <= high + step / 1e6; i++) ticks.push(Number((low + i * step).toPrecision(12)));
  return { ticks, domain: [low, high] };
}

/** Value extent of several arrays, ignoring nulls. */
export function extent(values: Iterable<number | null | undefined>): [number, number] | null {
  let min = Infinity;
  let max = -Infinity;
  for (const value of values) {
    if (value === null || value === undefined || !Number.isFinite(value)) continue;
    if (value < min) min = value;
    if (value > max) max = value;
  }
  return min === Infinity ? null : [min, max];
}

const TIME_STEPS = [5 * MINUTE, 10 * MINUTE, 15 * MINUTE, 30 * MINUTE, HOUR, 2 * HOUR, 3 * HOUR, 6 * HOUR, 12 * HOUR, DAY];

/** Ticks on round local times (every 2 h, at midnight, …) in the chart's time zone. */
export function timeTicks(from: number, to: number, width: number, timeZone?: string): { t: number; label: string }[] {
  const wanted = Math.max(2, Math.floor(width / 84));
  const step = TIME_STEPS.find((candidate) => (to - from) / candidate <= wanted) ?? DAY;
  const zone = timeZone ?? Intl.DateTimeFormat().resolvedOptions().timeZone;
  const offset = zoned(from, zone).offset;
  const ticks: { t: number; label: string }[] = [];
  for (let wall = Math.ceil((from + offset) / step) * step; wall - offset <= to; wall += step) {
    const t = wall - offset;
    const local = zoned(t, zone);
    const midnight = local.hour < 1e-6;
    const label =
      step >= DAY || midnight
        ? formatTime(t, { month: "short", day: "numeric" }, zone)
        : formatTime(t, step >= HOUR ? { hour: "numeric" } : { hour: "numeric", minute: "2-digit" }, zone);
    ticks.push({ t, label });
  }
  return ticks;
}
