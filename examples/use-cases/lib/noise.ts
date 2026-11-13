/**
 * Deterministic randomness. Every simulated reading is a pure function of
 * (series, time), so regenerating a minute always yields the same values and
 * FluxDB's upsert leaves exactly one copy, however many visitors race to
 * write it.
 */

/** FNV-1a hash of a string, as an unsigned 32-bit integer. */
export function hashString(text: string): number {
  let hash = 0x811c9dc5;
  for (let i = 0; i < text.length; i++) {
    hash ^= text.charCodeAt(i);
    hash = Math.imul(hash, 0x01000193);
  }
  return hash >>> 0;
}

/** A uniform value in [0, 1) fixed by (seed, n). `n` may be any integer up to 2^53. */
export function random01(seed: number, n: number): number {
  const low = n % 4294967296 | 0;
  const high = Math.floor(n / 4294967296) | 0;
  let h = (seed ^ Math.imul(low, 0x9e3779b1) ^ Math.imul(high, 0x85ebca77)) >>> 0;
  h = Math.imul(h ^ (h >>> 16), 0x85ebca6b);
  h = Math.imul(h ^ (h >>> 13), 0xc2b2ae35);
  h ^= h >>> 16;
  return (h >>> 0) / 4294967296;
}

/** Roughly normal, mean 0 and standard deviation 1, fixed by (seed, n). */
export function gaussian(seed: number, n: number): number {
  return (random01(seed, n) + random01(seed ^ 0x5bd1e995, n) + random01(seed ^ 0x1b873593, n) - 1.5) * 2;
}

/** Smooth noise in [-1, 1]: random values every `period` ms, eased in between. */
export function smoothNoise(seed: number, t: number, period: number): number {
  const x = t / period;
  const i = Math.floor(x);
  const f = x - i;
  const a = random01(seed, i) * 2 - 1;
  const b = random01(seed, i + 1) * 2 - 1;
  return a + (b - a) * f * f * (3 - 2 * f);
}

/** Smooth noise with finer detail layered on top; roughly in [-1, 1]. */
export function drift(seed: number, t: number, period: number, octaves = 3): number {
  let sum = 0;
  let weight = 1;
  let total = 0;
  let p = period;
  for (let octave = 0; octave < octaves; octave++) {
    sum += weight * smoothNoise(seed + octave * 7919, t, p);
    total += weight;
    weight *= 0.5;
    p /= 2.7;
  }
  return sum / total;
}

export function clamp(value: number, low: number, high: number): number {
  return Math.min(high, Math.max(low, value));
}

/** 0 below `edge0`, 1 above `edge1`, eased in between. */
export function smoothstep(edge0: number, edge1: number, x: number): number {
  const t = clamp((x - edge0) / (edge1 - edge0), 0, 1);
  return t * t * (3 - 2 * t);
}

/** A bell curve: 1 at `center`, falling off over `width`. */
export function bump(x: number, center: number, width: number): number {
  const d = (x - center) / width;
  return Math.exp(-d * d);
}

export function round(value: number, digits = 0): number {
  const factor = 10 ** digits;
  return Math.round(value * factor) / factor;
}
