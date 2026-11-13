/**
 * Hospital ward vitals: eight fictional patients on a step-down unit, each on
 * a bedside monitor that charts a set of vital signs every minute. Most of the
 * time they are stable; a couple of times a day one of them deteriorates over
 * a few hours (rising pulse, breathing and temperature, falling blood pressure
 * and oxygen), gets treated and recovers. The ward board scores each patient
 * with NEWS2, the early-warning score UK hospitals use to decide who needs
 * review.
 *
 * Synthetic data: these are not real people and none of this is medical advice.
 */
import type { Point } from "./lineProtocol";
import { clamp, drift, gaussian, hashString, random01, round, smoothstep } from "./noise";
import { DAY, HOUR, MINUTE, zoned } from "./time";

export const VITALS_STEP = MINUTE;
export const WARD_TIME_ZONE = "America/New_York";

export interface Baseline {
  hr: number;
  spo2: number;
  rr: number;
  sbp: number;
  temp: number;
}

export interface Bed {
  id: string;
  patient: string;
  reason: string;
  /** On supplemental oxygen at baseline. */
  oxygen: boolean;
  /** NEWS2 SpO2 scale 2 is for patients with a prescribed 88-92% target (hypercapnic respiratory failure). */
  spo2Scale: 1 | 2;
  base: Baseline;
  /** Beat-to-beat irregularity, as in atrial fibrillation. */
  hrSpread?: number;
}

export const BEDS: Bed[] = [
  { id: "B1", patient: "M, 72", reason: "Pneumonia, IV antibiotics", oxygen: true, spo2Scale: 1, base: { hr: 86, spo2: 96, rr: 17, sbp: 128, temp: 37.4 } },
  { id: "B2", patient: "F, 58", reason: "Day 2 after hip replacement", oxygen: false, spo2Scale: 1, base: { hr: 76, spo2: 97, rr: 15, sbp: 134, temp: 36.9 } },
  { id: "B3", patient: "M, 81", reason: "COPD exacerbation", oxygen: true, spo2Scale: 2, base: { hr: 86, spo2: 90, rr: 18, sbp: 142, temp: 36.8 } },
  { id: "B4", patient: "F, 44", reason: "Cellulitis", oxygen: false, spo2Scale: 1, base: { hr: 80, spo2: 98, rr: 15, sbp: 120, temp: 37.3 } },
  { id: "B5", patient: "M, 66", reason: "Heart failure, diuresis", oxygen: false, spo2Scale: 1, base: { hr: 84, spo2: 96, rr: 17, sbp: 116, temp: 36.6 } },
  { id: "B6", patient: "F, 77", reason: "Atrial fibrillation", oxygen: false, spo2Scale: 1, base: { hr: 94, spo2: 96, rr: 16, sbp: 124, temp: 36.7 }, hrSpread: 2.6 },
  { id: "B7", patient: "M, 35", reason: "Recovering from DKA", oxygen: false, spo2Scale: 1, base: { hr: 84, spo2: 99, rr: 16, sbp: 124, temp: 36.9 } },
  { id: "B8", patient: "F, 63", reason: "Day 1 after bowel resection", oxygen: false, spo2Scale: 1, base: { hr: 82, spo2: 97, rr: 16, sbp: 118, temp: 37.2 } },
];

export function findBed(id: string | null): Bed | undefined {
  return BEDS.find((bed) => bed.id === id);
}

export interface Vitals {
  hr: number;
  spo2: number;
  rr: number;
  sbp: number;
  temp_c: number;
  on_o2: boolean;
  /** ACVPU: Alert, new Confusion, responds to Voice, to Pain, Unresponsive. */
  acvpu: "A" | "C" | "V" | "P" | "U";
}

const EPISODE_SEED = hashString("ward-deterioration");

/**
 * How unwell a patient is at time t, 0 to about 1: a few hours of decline,
 * a plateau while the team responds, then recovery. About two patients a day.
 */
export function deterioration(bed: Bed, t: number): number {
  const seed = EPISODE_SEED ^ hashString(bed.id);
  let level = 0;
  const today = Math.floor(t / DAY);
  for (const day of [today - 1, today]) {
    if (random01(seed, day) > 0.26) continue;
    const onset = day * DAY + random01(seed + 1, day) * DAY;
    const severity = 0.6 + 0.5 * random01(seed + 2, day);
    const hours = (t - onset) / HOUR;
    const rise = smoothstep(0, 2.6, hours);
    const recover = 1 - smoothstep(3.6, 6, hours);
    level = Math.max(level, severity * rise * recover);
  }
  return level;
}

export function vitalsAt(bed: Bed, t: number): Vitals {
  const seed = hashString(`bed:${bed.id}`);
  const minute = Math.floor(t / MINUTE);
  const hour = zoned(t, WARD_TIME_ZONE).hour;
  const night = smoothstep(0, 2, hour) * (1 - smoothstep(5, 7, hour));
  const sick = deterioration(bed, t);
  const slow = (salt: number) => drift(seed + salt, t, 3 * HOUR);
  const noise = (salt: number) => gaussian(seed + salt, minute);

  const hr = bed.base.hr - 6 * night + 34 * sick + 5 * slow(1) + (1.6 + (bed.hrSpread ?? 0) * 2) * noise(2);
  const rr = bed.base.rr - 1 * night + 9 * sick + 1.2 * slow(3) + 0.9 * noise(4);
  const sbp = bed.base.sbp - 7 * night - 30 * sick + 7 * slow(5) + 3.5 * noise(6);
  const temp = bed.base.temp - 0.25 * night + 1.8 * sick + 0.15 * slow(7) + 0.04 * noise(8);
  const onOxygen = bed.oxygen || sick > 0.45;
  const oxygenBoost = onOxygen && !bed.oxygen ? 3 : 0;
  const spo2 = bed.base.spo2 - 7 * sick + oxygenBoost + 0.8 * slow(9) + 0.7 * noise(10);
  return {
    hr: Math.round(clamp(hr, 35, 180)),
    spo2: Math.round(clamp(spo2, 75, 100)),
    rr: Math.round(clamp(rr, 6, 40)),
    sbp: Math.round(clamp(sbp, 70, 220)),
    temp_c: round(clamp(temp, 34.5, 41), 1),
    on_o2: onOxygen,
    acvpu: sick > 0.9 ? "C" : "A",
  };
}

export function vitalsPoint(bed: Bed, vitals: Vitals, t: number, source: "monitor" | "manual"): Point {
  return {
    measurement: "vitals",
    tags: { bed: bed.id, source },
    fields: {
      hr: BigInt(vitals.hr),
      spo2: BigInt(vitals.spo2),
      rr: BigInt(vitals.rr),
      sbp: BigInt(vitals.sbp),
      temp_c: vitals.temp_c,
      on_o2: vitals.on_o2,
      acvpu: vitals.acvpu,
    },
    time: t,
  };
}

/** Every monitor reading in [from, to] for the whole ward. */
export function wardPoints(from: number, to: number): Point[] {
  const points: Point[] = [];
  for (let t = from; t <= to; t += VITALS_STEP) {
    for (const bed of BEDS) points.push(vitalsPoint(bed, vitalsAt(bed, t), t, "monitor"));
  }
  return points;
}

// ---- NEWS2 ------------------------------------------------------------------
// Royal College of Physicians, National Early Warning Score 2 (2017).

export type Risk = "low" | "low-medium" | "medium" | "high";

export interface News2 {
  total: number;
  risk: Risk;
  /** Points per parameter, for the breakdown. */
  parts: { rr: number; spo2: number; air: number; sbp: number; hr: number; acvpu: number; temp: number };
  response: string;
}

function rrScore(rr: number): number {
  if (rr <= 8) return 3;
  if (rr <= 11) return 1;
  if (rr <= 20) return 0;
  if (rr <= 24) return 2;
  return 3;
}

function spo2Score(spo2: number, scale: 1 | 2, onOxygen: boolean): number {
  if (scale === 1) {
    if (spo2 <= 91) return 3;
    if (spo2 <= 93) return 2;
    if (spo2 <= 95) return 1;
    return 0;
  }
  if (spo2 <= 83) return 3;
  if (spo2 <= 85) return 2;
  if (spo2 <= 87) return 1;
  if (spo2 <= 92 || !onOxygen) return 0;
  if (spo2 <= 94) return 1;
  if (spo2 <= 96) return 2;
  return 3;
}

function sbpScore(sbp: number): number {
  if (sbp <= 90) return 3;
  if (sbp <= 100) return 2;
  if (sbp <= 110) return 1;
  if (sbp <= 219) return 0;
  return 3;
}

function hrScore(hr: number): number {
  if (hr <= 40) return 3;
  if (hr <= 50) return 1;
  if (hr <= 90) return 0;
  if (hr <= 110) return 1;
  if (hr <= 130) return 2;
  return 3;
}

function tempScore(temp: number): number {
  if (temp <= 35.0) return 3;
  if (temp <= 36.0) return 1;
  if (temp <= 38.0) return 0;
  if (temp <= 39.0) return 1;
  return 2;
}

export function news2(vitals: Vitals, scale: 1 | 2): News2 {
  const parts = {
    rr: rrScore(vitals.rr),
    spo2: spo2Score(vitals.spo2, scale, vitals.on_o2),
    air: vitals.on_o2 ? 2 : 0,
    sbp: sbpScore(vitals.sbp),
    hr: hrScore(vitals.hr),
    acvpu: vitals.acvpu === "A" ? 0 : 3,
    temp: tempScore(vitals.temp_c),
  };
  const total = Object.values(parts).reduce((sum, value) => sum + value, 0);
  const redFlag = Object.values(parts).some((value) => value === 3);
  const risk: Risk = total >= 7 ? "high" : total >= 5 ? "medium" : redFlag ? "low-medium" : "low";
  const response = {
    high: "Emergency response: continuous monitoring, critical-care assessment",
    medium: "Urgent response: clinician review, observations at least hourly",
    "low-medium": "Urgent ward review: one parameter scores 3",
    low: total === 0 ? "Routine: observations every 12 hours" : "Ward-based response: observations every 4-6 hours",
  }[risk];
  return { total, risk, parts, response };
}

/** Plausible ranges for a manually entered observation. */
export const OBSERVATION_LIMITS = {
  hr: [25, 220],
  spo2: [60, 100],
  rr: [4, 60],
  sbp: [50, 260],
  temp_c: [32, 43],
} as const;
