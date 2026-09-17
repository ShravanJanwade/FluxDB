/** Formatting, time handling and the query macros. */

import { describe, expect, it } from "vitest";
import { autoInterval, INTERVALS, substituteMacros } from "../src/lib/api";
import {
  bytes,
  cellNumber,
  cellText,
  count,
  decimal,
  duration,
  fieldNumber,
  fieldText,
  initials,
  percentOf,
  uptime,
} from "../src/lib/format";
import { msToNanos, nanosToMs, resolveRange } from "../src/lib/time";

describe("query macros", () => {
  it("expands every macro to absolute values", () => {
    expect(
      substituteMacros(
        "SELECT MEAN(usage) FROM cpu WHERE $timeFilter GROUP BY time($interval)",
        "100",
        "200",
        "5m",
      ),
    ).toBe(
      "SELECT MEAN(usage) FROM cpu WHERE time >= 100 AND time <= 200 GROUP BY time('5m')",
    );
  });

  it("expands a macro used more than once", () => {
    // `replaceAll`, not `replace`: a panel may filter twice in one query.
    expect(substituteMacros("$from..$to and $from", "1", "9", "1m")).toBe(
      "1..9 and 1",
    );
  });

  it("leaves a query with no macros untouched", () => {
    const sql = "SELECT * FROM cpu WHERE time > 5 LIMIT 10";
    expect(substituteMacros(sql, "1", "2", "1m")).toBe(sql);
  });

  it("chooses a bucket width that keeps a chart readable", () => {
    for (const minutes of [5, 30, 60, 360, 1_440, 10_080, 43_200]) {
      const span = minutes * 60 * 1e9;
      const interval = autoInterval(span);
      expect(INTERVALS).toContain(interval as (typeof INTERVALS)[number]);
    }
    // A short window gets the finest bucket; a month gets six-hour buckets,
    // which is 120 points — still a readable line.
    expect(autoInterval(5 * 60 * 1e9)).toBe("10s");
    expect(autoInterval(30 * 86_400 * 1e9)).toBe("6h");
    // Every choice lands in the 60–400 point band the picker aims for.
    for (const minutes of [5, 60, 1_440, 43_200]) {
      const span = minutes * 60 * 1e9;
      const interval = autoInterval(span);
      const seconds = Number.parseInt(interval, 10);
      const unit = interval.slice(-1);
      const widthNs =
        seconds *
        (unit === "s"
          ? 1e9
          : unit === "m"
            ? 60e9
            : unit === "h"
              ? 3600e9
              : 86400e9);
      expect(span / widthNs).toBeLessThanOrEqual(400);
      expect(span / widthNs).toBeGreaterThanOrEqual(1);
    }
  });
});

describe("time", () => {
  it("converts milliseconds to nanoseconds without losing precision", () => {
    // 1.7e18 nanoseconds exceeds Number.MAX_SAFE_INTEGER, so this has to go
    // through BigInt or the timestamp is silently wrong.
    expect(msToNanos(1_700_000_000_123)).toBe("1700000000123000000");
    expect(Number(msToNanos(1_700_000_000_123))).toBeGreaterThan(
      Number.MAX_SAFE_INTEGER,
    );
    expect(nanosToMs("1700000000123000000")).toBe(1_700_000_000_123);
    expect(Number.isNaN(nanosToMs("not-a-number"))).toBe(true);
  });

  it("resolves a range to absolute bounds around the supplied instant", () => {
    const now = 1_700_000_000_000;
    const window = resolveRange("1h", now);
    expect(window.toMs).toBe(now);
    expect(window.fromMs).toBe(now - 3_600_000);
    expect(window.from).toBe(msToNanos(now - 3_600_000));
    expect(window.to).toBe(msToNanos(now));
  });
});

describe("formatting", () => {
  it("abbreviates large counts and keeps small ones exact", () => {
    expect(count(842)).toBe("842");
    expect(count(9_999)).toBe("9,999");
    expect(count(12_500)).toBe("12.5K");
    expect(count(3_400_000)).toBe("3.4M");
    expect(count(Number.NaN)).toBe("—");
  });

  it("scales bytes and never reports a negative size", () => {
    expect(bytes(0)).toBe("0 B");
    expect(bytes(900)).toBe("900 B");
    expect(bytes(2_048)).toBe("2 KB");
    expect(bytes(5 * 1024 * 1024)).toBe("5 MB");
    expect(bytes(-1)).toBe("—");
  });

  it("describes retention the way a person would say it", () => {
    expect(duration(0)).toBe("Forever");
    expect(duration(86_400)).toBe("1 day");
    expect(duration(7 * 86_400)).toBe("1 week");
    expect(duration(30 * 86_400)).toBe("1 month");
    expect(duration(90)).toBe("2 minutes");
  });

  it("formats uptime at a useful granularity", () => {
    expect(uptime(45)).toBe("0m 45s");
    expect(uptime(3_700)).toBe("1h 1m");
    expect(uptime(90_000)).toBe("1d 1h");
    expect(uptime(-1)).toBe("—");
  });

  it("keeps very small and very large numbers legible", () => {
    expect(decimal(0.0001)).toBe("1.0e-4");
    expect(decimal(42.849)).toBe("42.85");
    expect(decimal(1_250_000)).toBe("1.3M");
  });

  it("renders every field value shape the API can return", () => {
    expect(fieldText({ integer: "9223372036854775807" })).toBe(
      "9223372036854775807",
    );
    expect(fieldText(true)).toBe("true");
    expect(fieldText("6.8.0")).toBe("6.8.0");
    expect(fieldText(42.8)).toBe("42.8");
    expect(fieldText(null)).toBe("—");
    // Only numeric shapes are chartable.
    expect(fieldNumber({ integer: "8" })).toBe(8);
    expect(fieldNumber("text")).toBeNull();
    expect(fieldNumber(true)).toBeNull();
  });

  it("renders a time column as a local timestamp, not a raw integer", () => {
    const text = cellText("1700000000000000000", "time");
    expect(text).not.toBe("1700000000000000000");
    expect(text).toMatch(/\d/);
    expect(cellText("9007199254740993", "requests")).toBe("9007199254740993");
    expect(cellText(null, "anything")).toBe("—");
    expect(cellNumber("12.5")).toBe(12.5);
    expect(cellNumber("")).toBeNull();
  });

  it("clamps a quota percentage to the track", () => {
    expect(percentOf(50, 100)).toBe(50);
    expect(percentOf(500, 100)).toBe(100);
    expect(percentOf(5, 0)).toBe(0);
  });

  it("derives initials from a name or an email address", () => {
    expect(initials("Ada Lovelace")).toBe("AL");
    expect(initials("ada.lovelace@example.com")).toBe("AL");
    expect(initials("ada")).toBe("AD");
    expect(initials("")).toBe("?");
  });
});
