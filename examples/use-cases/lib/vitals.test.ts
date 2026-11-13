import { describe, expect, it } from "vitest";
import { BEDS, news2, vitalsAt, wardPoints, type Vitals } from "./vitals";

const calm: Vitals = { hr: 72, spo2: 98, rr: 14, sbp: 124, temp_c: 36.8, on_o2: false, acvpu: "A" };

describe("news2", () => {
  it("scores a well patient 0", () => {
    const score = news2(calm, 1);
    expect(score.total).toBe(0);
    expect(score.risk).toBe("low");
  });

  it("adds every parameter", () => {
    // RR 25 → 3, SpO2 91 → 3, oxygen → 2, SBP 95 → 2, HR 125 → 2, alert → 0, 38.5 °C → 1
    const score = news2({ hr: 125, spo2: 91, rr: 25, sbp: 95, temp_c: 38.5, on_o2: true, acvpu: "A" }, 1);
    expect(score.parts).toEqual({ rr: 3, spo2: 3, air: 2, sbp: 2, hr: 2, acvpu: 0, temp: 1 });
    expect(score.total).toBe(13);
    expect(score.risk).toBe("high");
  });

  it("flags a single parameter scoring 3 as low-medium", () => {
    const score = news2({ ...calm, acvpu: "C" }, 1);
    expect(score.total).toBe(3);
    expect(score.risk).toBe("low-medium");
  });

  it("uses medium for 5 or 6", () => {
    expect(news2({ ...calm, hr: 115, rr: 22, temp_c: 38.3 }, 1)).toMatchObject({ total: 5, risk: "medium" });
  });

  it("scores SpO2 on scale 2 for a prescribed 88-92% target", () => {
    expect(news2({ ...calm, spo2: 89, on_o2: true }, 2).parts.spo2).toBe(0);
    expect(news2({ ...calm, spo2: 89, on_o2: true }, 1).parts.spo2).toBe(3);
    expect(news2({ ...calm, spo2: 97, on_o2: true }, 2).parts.spo2).toBe(3);
    expect(news2({ ...calm, spo2: 97, on_o2: false }, 2).parts.spo2).toBe(0);
    expect(news2({ ...calm, spo2: 85 }, 2).parts.spo2).toBe(2);
  });
});

describe("simulated monitors", () => {
  it("are a pure function of time, so re-writing a minute changes nothing", () => {
    const t = Date.UTC(2026, 10, 12, 15, 30);
    for (const bed of BEDS) expect(vitalsAt(bed, t)).toEqual(vitalsAt(bed, t));
    expect(wardPoints(t, t + 5 * 60_000)).toEqual(wardPoints(t, t + 5 * 60_000));
  });

  it("stay inside physiological limits over a day", () => {
    const start = Date.UTC(2026, 10, 12);
    for (let t = start; t < start + 86_400_000; t += 7 * 60_000) {
      for (const bed of BEDS) {
        const v = vitalsAt(bed, t);
        expect(v.hr).toBeGreaterThanOrEqual(35);
        expect(v.hr).toBeLessThanOrEqual(180);
        expect(v.spo2).toBeLessThanOrEqual(100);
        expect(v.temp_c).toBeGreaterThan(34);
      }
    }
  });

  it("write one point per bed per minute", () => {
    const t = Date.UTC(2026, 10, 12, 8);
    expect(wardPoints(t, t + 59 * 60_000)).toHaveLength(60 * BEDS.length);
  });
});
