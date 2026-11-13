import { describe, expect, it, vi } from "vitest";
import { BUILDINGS, ENERGY_STEP, energyPoints } from "./energy";
import type { FluxDB, QueryResult, WriteResult } from "./fluxdb";
import type { Point } from "./lineProtocol";
import { catchUp, pendingRange } from "./sync";
import { DAY, HOUR, MINUTE, startOfLocalDay, zoned } from "./time";

describe("pendingRange", () => {
  const now = Date.UTC(2026, 10, 12, 12, 7, 30);

  it("backfills the maximum after a quiet spell", () => {
    expect(pendingRange(undefined, now, 5 * MINUTE, DAY)).toEqual({
      from: Date.UTC(2026, 10, 11, 12, 5),
      to: Date.UTC(2026, 10, 12, 12, 5),
    });
  });

  it("continues one step after the watermark", () => {
    expect(pendingRange(Date.UTC(2026, 10, 12, 11, 55), now, 5 * MINUTE, DAY)).toEqual({
      from: Date.UTC(2026, 10, 12, 12, 0),
      to: Date.UTC(2026, 10, 12, 12, 5),
    });
  });

  it("is null when the stream is current", () => {
    expect(pendingRange(Date.UTC(2026, 10, 12, 12, 5), now, 5 * MINUTE, DAY)).toBeNull();
  });

  it("never reaches further back than the cap", () => {
    expect(pendingRange(Date.UTC(2026, 9, 1), now, MINUTE, HOUR)?.from).toBe(Date.UTC(2026, 10, 12, 11, 7));
  });
});

describe("catchUp", () => {
  it("writes the readings and then the watermark in one request", async () => {
    const written: Point[][] = [];
    const db = {
      write: vi.fn(async (_bucket: string, points: Point[]): Promise<WriteResult> => {
        written.push(points);
        return { bucket: "energy", points: points.length, requests: 1, totalMs: 3, sample: [] };
      }),
      query: vi.fn(async (): Promise<QueryResult> => {
        throw new Error("not used");
      }),
    } as unknown as FluxDB;
    const now = Date.UTC(2026, 10, 12, 12, 0);
    const [report] = await catchUp(
      db,
      "energy",
      [
        {
          stream: "campus-meters",
          stepMs: ENERGY_STEP,
          maxBackfillMs: HOUR,
          produce: async (from, to) => ({ points: energyPoints(from, to), watermark: to, source: "test" }),
        },
      ],
      new Map([["campus-meters", now - 15 * MINUTE]]),
      now,
    );
    // 10, 5 and 0 minutes ago: three readings of (4 buildings + campus).
    expect(report.written).toBe(3 * (BUILDINGS.length + 1));
    expect(written).toHaveLength(1);
    const last = written[0].at(-1)!;
    expect(last).toMatchObject({ measurement: "sync_state", tags: { stream: "campus-meters" }, fields: { until_ms: now } });
  });
});

describe("time zones", () => {
  it("finds local midnight across a daylight-saving change", () => {
    // 2026-11-01 is the day US clocks fall back; midnight was still EDT (UTC-4).
    const t = Date.UTC(2026, 10, 1, 18);
    expect(startOfLocalDay(t, "America/New_York")).toBe(Date.UTC(2026, 10, 1, 4));
    expect(zoned(t, "America/New_York").hour).toBe(13);
  });
});
