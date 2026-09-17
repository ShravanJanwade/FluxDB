/**
 * The console never asks the user to describe the shape of a query result — it
 * classifies the columns and picks a chart. These tests pin that
 * classification, because getting it wrong means a panel silently plots the
 * wrong thing rather than failing.
 */

import { describe, expect, it } from "vitest";
import {
  bucketTelemetry,
  MAX_SERIES,
  statValue,
  toSeries,
} from "../src/lib/charts";
import type { QueryResult } from "../src/lib/types";

/** Nanosecond string for a whole-minute offset from a fixed epoch. */
function at(minute: number): string {
  return String(1_700_000_000_000_000_000n + BigInt(minute) * 60_000_000_000n);
}

describe("toSeries", () => {
  it("splits a time-bucketed result into one series per tag value", () => {
    const result: QueryResult = {
      columns: ["time", "host", "cpu"],
      rows: [
        [at(0), "api-01", 40],
        [at(0), "api-02", 10],
        [at(1), "api-01", 44],
        [at(1), "api-02", 12],
      ],
      execution_time_ms: 1,
    };
    const shaped = toSeries(result);
    expect(shaped.hasTime).toBe(true);
    expect(shaped.series.map((series) => series.name)).toEqual([
      "api-01",
      "api-02",
    ]);
    // Ordered by peak value, so the legend matches what stands out.
    expect(shaped.series[0].points).toHaveLength(2);
    expect(shaped.series[0].points[0][1]).toBe(40);
    expect(shaped.spanMs).toBe(60_000);
    expect(shaped.measures).toEqual(["cpu"]);
    expect(shaped.labels).toEqual(["host"]);
  });

  it("names series by measure when several are selected", () => {
    const result: QueryResult = {
      columns: ["time", "service", "p50", "p99"],
      rows: [
        [at(0), "checkout", 10, 80],
        [at(1), "checkout", 12, 95],
      ],
      execution_time_ms: 1,
    };
    const shaped = toSeries(result);
    expect(shaped.series.map((series) => series.name).sort()).toEqual([
      "checkout · p50",
      "checkout · p99",
    ]);
  });

  it("treats a result with no time column as categories", () => {
    const result: QueryResult = {
      columns: ["service", "errors"],
      rows: [
        ["payments", 412],
        ["checkout", 18],
        ["catalog", 2],
      ],
      execution_time_ms: 1,
    };
    const shaped = toSeries(result);
    expect(shaped.hasTime).toBe(false);
    expect(shaped.series).toHaveLength(0);
    expect(shaped.categories).toEqual(["payments", "checkout", "catalog"]);
    expect(shaped.bars).toEqual([{ name: "errors", values: [412, 18, 2] }]);
  });

  it("reads integer and timestamp columns that arrive as decimal strings", () => {
    // The API returns 64-bit integers as strings so JSON cannot round them;
    // a column of those must still count as numeric.
    const result: QueryResult = {
      columns: ["time", "requests"],
      rows: [
        [at(0), "9007199254740993"],
        [at(1), "12"],
      ],
      execution_time_ms: 1,
    };
    const shaped = toSeries(result);
    expect(shaped.series).toHaveLength(1);
    expect(shaped.series[0].points[1][1]).toBe(12);
  });

  it("classifies a column with any non-numeric value as a label", () => {
    const result: QueryResult = {
      columns: ["time", "series", "state"],
      rows: [
        [at(0), "cpu,host=api-01", "ok"],
        [at(1), "cpu,host=api-01", "degraded"],
      ],
      execution_time_ms: 1,
    };
    const shaped = toSeries(result);
    expect(shaped.measures).toEqual([]);
    expect(shaped.series).toHaveLength(0);
  });

  it("ignores rows whose timestamp cannot be read", () => {
    const result: QueryResult = {
      columns: ["time", "cpu"],
      rows: [
        ["not-a-timestamp", 5],
        [at(0), 7],
      ],
      execution_time_ms: 1,
    };
    expect(toSeries(result).series[0].points).toEqual([
      [Number(1_700_000_000_000n), 7],
    ]);
  });

  it("returns an empty shape for nothing, rather than throwing", () => {
    expect(toSeries(null).series).toEqual([]);
    expect(
      toSeries({ columns: [], rows: [], execution_time_ms: 0 }).categories,
    ).toEqual([]);
    expect(
      toSeries({ columns: ["a"], rows: [], execution_time_ms: 0 }).bars,
    ).toEqual([]);
  });
});

describe("statValue", () => {
  it("takes the newest point of the leading series", () => {
    const result: QueryResult = {
      columns: ["time", "cpu"],
      rows: [
        [at(0), 40],
        [at(1), 62],
      ],
      execution_time_ms: 1,
    };
    expect(statValue(result)).toBe(62);
  });

  it("falls back to a categorical result's last value", () => {
    expect(
      statValue({
        columns: ["service", "errors"],
        rows: [["payments", 9]],
        execution_time_ms: 1,
      }),
    ).toBe(9);
  });

  it("is null when there is nothing numeric to show", () => {
    expect(statValue(null)).toBeNull();
    expect(
      statValue({ columns: ["time"], rows: [[at(0)]], execution_time_ms: 1 }),
    ).toBeNull();
  });
});

describe("bucketTelemetry", () => {
  it("buckets samples and computes percentiles per bucket", () => {
    const now = Date.now();
    const samples = [
      // Two recent samples, one of them a failure.
      { time: now - 1_000, duration_ms: 10, status: 200 },
      { time: now - 2_000, duration_ms: 90, status: 500 },
      // One outside the window, which must be dropped.
      { time: now - 7_200_000, duration_ms: 1_000, status: 200 },
    ];
    const buckets = bucketTelemetry(samples, 15, 10);
    expect(buckets).toHaveLength(10);
    const total = buckets.reduce((sum, bucket) => sum + bucket.count, 0);
    expect(total).toBe(2);
    const last = buckets[buckets.length - 1];
    expect(last.max).toBe(90);
    expect(last.p50).toBeGreaterThanOrEqual(10);
    expect(last.ok).toBe(1);
    expect(last.failed).toBe(1);
  });

  it("returns zeroed buckets when there are no samples", () => {
    const buckets = bucketTelemetry([], 60, 4);
    expect(buckets).toHaveLength(4);
    expect(
      buckets.every((bucket) => bucket.count === 0 && bucket.p95 === 0),
    ).toBe(true);
  });
});

describe("categorical slots", () => {
  /** A result with `count` hosts, each holding one point at `minute`. */
  function fleet(hosts: string[], minute = 0): QueryResult {
    return {
      columns: ["time", "host", "cpu"],
      rows: hosts.map((host, index) => [at(minute), host, 10 + index]),
      execution_time_ms: 1,
    };
  }

  it("keeps a series on the same colour when the ordering changes", () => {
    // Colour assigned by rank would repaint every survivor as soon as the
    // range changed and the busiest series changed with it.
    const quiet = toSeries({
      columns: ["time", "host", "cpu"],
      rows: [
        [at(0), "api-01", 10],
        [at(0), "api-02", 90],
      ],
      execution_time_ms: 1,
    });
    const busy = toSeries({
      columns: ["time", "host", "cpu"],
      rows: [
        [at(0), "api-01", 90],
        [at(0), "api-02", 10],
      ],
      execution_time_ms: 1,
    });
    const slotOf = (shaped: ReturnType<typeof toSeries>, name: string) =>
      shaped.series.find((series) => series.name === name)?.slot;

    // The legend order follows the peak, so the leading series differs...
    expect(quiet.series[0].name).toBe("api-02");
    expect(busy.series[0].name).toBe("api-01");
    // ...while each host keeps the colour it had.
    expect(slotOf(quiet, "api-01")).toBe(slotOf(busy, "api-01"));
    expect(slotOf(quiet, "api-02")).toBe(slotOf(busy, "api-02"));
  });

  it("never gives two visible series the same slot", () => {
    const hosts = Array.from({ length: MAX_SERIES }, (_, i) => `host-${i}`);
    const shaped = toSeries(fleet(hosts));
    const slots = shaped.series.map((series) => series.slot);
    expect(new Set(slots).size).toBe(MAX_SERIES);
    expect(slots.every((slot) => slot >= 0 && slot < MAX_SERIES)).toBe(true);
  });

  it("drops series past the palette rather than recycling a colour", () => {
    // A ninth hue is indistinguishable from one already in use, so the extra
    // series are reported as hidden instead of being drawn ambiguously.
    const hosts = Array.from(
      { length: 20 },
      (_, i) => `host-${String(i).padStart(2, "0")}`,
    );
    const shaped = toSeries(fleet(hosts));
    expect(shaped.series).toHaveLength(MAX_SERIES);
    expect(shaped.hidden).toBe(12);
    expect(new Set(shaped.series.map((series) => series.slot)).size).toBe(
      MAX_SERIES,
    );
    // The ones kept are the busiest, which is what an operator is looking for.
    expect(shaped.series[0].name).toBe("host-19");
  });

  it("truncates a long categorical result and says how many were dropped", () => {
    const rows = Array.from({ length: 40 }, (_, index) => [
      `service-${index}`,
      index,
    ]);
    const shaped = toSeries({
      columns: ["service", "errors"],
      rows,
      execution_time_ms: 1,
    });
    expect(shaped.categories).toHaveLength(24);
    expect(shaped.hidden).toBe(16);
    expect(shaped.bars[0].values).toHaveLength(24);
  });

  it("reports nothing hidden when everything fits", () => {
    const shaped = toSeries(fleet(["api-01", "api-02", "api-03"]));
    expect(shaped.hidden).toBe(0);
    expect(shaped.series).toHaveLength(3);
  });
});
