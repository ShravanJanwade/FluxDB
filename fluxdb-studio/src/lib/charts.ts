/**
 * Turning query results into charts.
 *
 * The SQL subset returns three recognisable shapes and the console has to plot
 * all of them without being told which is which:
 *
 *  * `GROUP BY time(...)` with optional tags — a time series per tag value;
 *  * `GROUP BY <tag>` with no time bucket — one category per tag value;
 *  * `SELECT *` — raw points, with `time` and `series` columns.
 *
 * `toSeries` classifies the columns once and every panel builds on that, so a
 * chart never has to be configured with a schema the result already describes.
 */

import type { EChartsOption } from "echarts";
import { axisLabel, cellNumber, decimal } from "./format";
import { nanosToMs } from "./time";
import type { PanelKind, QueryResult } from "./types";

export type Series = { name: string; points: [number, number][] };

export type Shaped = {
  /** Series over time. Empty when the result is categorical. */
  series: Series[];
  /** Category labels, when the result has no time column. */
  categories: string[];
  /** One value per category per measure, when the result is categorical. */
  bars: { name: string; values: (number | null)[] }[];
  hasTime: boolean;
  spanMs: number;
  /** Columns that held numbers. */
  measures: string[];
  /** Columns that held labels (tags). */
  labels: string[];
};

const EMPTY: Shaped = {
  series: [],
  categories: [],
  bars: [],
  hasTime: false,
  spanMs: 0,
  measures: [],
  labels: [],
};

/** Reads the theme's series colours so charts restyle with the rest of the UI. */
export function palette(): string[] {
  if (typeof window === "undefined") return ["#5b5bf0"];
  const styles = getComputedStyle(document.documentElement);
  const colours = Array.from({ length: 8 }, (_, index) =>
    styles.getPropertyValue(`--series-${index + 1}`).trim(),
  ).filter(Boolean);
  return colours.length > 0 ? colours : ["#5b5bf0"];
}

type ChartInk = {
  text: string;
  muted: string;
  grid: string;
  surface: string;
  border: string;
};

function ink(): ChartInk {
  if (typeof window === "undefined") {
    return {
      text: "#0f172a",
      muted: "#6b7789",
      grid: "#e3e8f1",
      surface: "#ffffff",
      border: "#ccd4e2",
    };
  }
  const styles = getComputedStyle(document.documentElement);
  const read = (name: string, fallback: string) =>
    styles.getPropertyValue(name).trim() || fallback;
  return {
    text: read("--text", "#0f172a"),
    muted: read("--text-muted", "#6b7789"),
    grid: read("--border", "#e3e8f1"),
    surface: read("--surface-raised", "#ffffff"),
    border: read("--border-strong", "#ccd4e2"),
  };
}

export function toSeries(result: QueryResult | null): Shaped {
  if (!result || result.columns.length === 0 || result.rows.length === 0) {
    return EMPTY;
  }
  const timeIndex = result.columns.findIndex((column) => column === "time");
  const hasTime = timeIndex >= 0;

  // A column is a measure when every non-null cell in it reads as a number.
  const measureIndexes: number[] = [];
  const labelIndexes: number[] = [];
  result.columns.forEach((_, index) => {
    if (index === timeIndex) return;
    let sawValue = false;
    let allNumeric = true;
    for (const row of result.rows) {
      const cell = row[index];
      if (cell === null || cell === undefined || cell === "") continue;
      sawValue = true;
      if (cellNumber(cell) === null) {
        allNumeric = false;
        break;
      }
    }
    if (sawValue && allNumeric) measureIndexes.push(index);
    else labelIndexes.push(index);
  });

  const labelOf = (row: (string | number | boolean | null)[]) =>
    labelIndexes
      .map((index) => String(row[index] ?? ""))
      .filter(Boolean)
      .join(" · ");

  if (hasTime) {
    const grouped = new Map<string, [number, number][]>();
    let min = Number.POSITIVE_INFINITY;
    let max = Number.NEGATIVE_INFINITY;
    for (const row of result.rows) {
      const raw = row[timeIndex];
      const at =
        typeof raw === "string" ? nanosToMs(raw) : Number(raw ?? Number.NaN);
      if (!Number.isFinite(at)) continue;
      min = Math.min(min, at);
      max = Math.max(max, at);
      const label = labelOf(row);
      for (const index of measureIndexes) {
        const value = cellNumber(row[index]);
        if (value === null) continue;
        // A single measure with a tag label reads better as the tag alone.
        const name =
          measureIndexes.length === 1
            ? label || result.columns[index]
            : label
              ? `${label} · ${result.columns[index]}`
              : result.columns[index];
        const points = grouped.get(name) ?? [];
        points.push([at, value]);
        grouped.set(name, points);
      }
    }
    const series = [...grouped.entries()]
      .map(([name, points]) => ({
        name,
        points: points.sort((a, b) => a[0] - b[0]),
      }))
      // Busiest series first, so the legend's order matches what the eye picks
      // out of the chart.
      .sort(
        (a, b) =>
          Math.max(...b.points.map((point) => point[1])) -
          Math.max(...a.points.map((point) => point[1])),
      );
    return {
      ...EMPTY,
      series,
      hasTime: true,
      spanMs: Number.isFinite(max - min) ? max - min : 0,
      measures: measureIndexes.map((index) => result.columns[index]),
      labels: labelIndexes.map((index) => result.columns[index]),
    };
  }

  const categories = result.rows.map(
    (row, position) => labelOf(row) || `Row ${position + 1}`,
  );
  const bars = measureIndexes.map((index) => ({
    name: result.columns[index],
    values: result.rows.map((row) => cellNumber(row[index])),
  }));
  return {
    ...EMPTY,
    categories,
    bars,
    measures: measureIndexes.map((index) => result.columns[index]),
    labels: labelIndexes.map((index) => result.columns[index]),
  };
}

/** The single number a stat panel shows: the last value of the busiest series,
 *  or the sole cell of a one-row result. */
export function statValue(result: QueryResult | null): number | null {
  const shaped = toSeries(result);
  if (shaped.series.length > 0) {
    const points = shaped.series[0].points;
    return points.length > 0 ? points[points.length - 1][1] : null;
  }
  if (shaped.bars.length > 0) {
    const values = shaped.bars[0].values.filter(
      (value): value is number => value !== null,
    );
    return values.length > 0 ? values[values.length - 1] : null;
  }
  return null;
}

const baseTooltip = (theme: ChartInk) => ({
  trigger: "axis" as const,
  backgroundColor: theme.surface,
  borderColor: theme.border,
  borderWidth: 1,
  padding: [8, 12] as [number, number],
  textStyle: { color: theme.text, fontSize: 12 },
  axisPointer: { type: "line" as const, lineStyle: { color: theme.border } },
  confine: true,
});

/** Chart option for a shaped result. `kind` picks between lines, filled areas
 *  and bars; everything else is derived. */
export function chartOption(
  shaped: Shaped,
  kind: Exclude<PanelKind, "stat" | "table">,
  unit: string,
  options: { legend?: boolean; compact?: boolean } = {},
): EChartsOption {
  const theme = ink();
  const colours = palette();
  const showLegend =
    options.legend !== false &&
    (shaped.series.length > 1 || shaped.bars.length > 1);
  const grid = {
    left: options.compact ? 8 : 4,
    right: options.compact ? 8 : 12,
    top: showLegend ? 34 : 14,
    bottom: 4,
    containLabel: true,
  };
  const valueAxis = {
    type: "value" as const,
    axisLabel: {
      color: theme.muted,
      fontSize: 11,
      formatter: (value: number) =>
        `${decimal(value)}${unit ? ` ${unit}` : ""}`,
    },
    splitLine: { lineStyle: { color: theme.grid, type: "dashed" as const } },
    axisLine: { show: false },
    axisTick: { show: false },
  };
  const legend = showLegend
    ? {
        type: "scroll" as const,
        top: 0,
        left: 0,
        icon: "roundRect",
        itemWidth: 9,
        itemHeight: 9,
        itemGap: 14,
        textStyle: { color: theme.muted, fontSize: 11 },
      }
    : { show: false };

  if (shaped.hasTime) {
    return {
      color: colours,
      grid,
      legend,
      tooltip: {
        ...baseTooltip(theme),
        valueFormatter: (value) =>
          `${decimal(Number(value))}${unit ? ` ${unit}` : ""}`,
      },
      xAxis: {
        type: "time",
        axisLabel: {
          color: theme.muted,
          fontSize: 11,
          hideOverlap: true,
          formatter: (value: number) => axisLabel(value, shaped.spanMs),
        },
        axisLine: { lineStyle: { color: theme.grid } },
        axisTick: { show: false },
        splitLine: { show: false },
      },
      yAxis: valueAxis,
      animationDuration: 260,
      series: shaped.series.map((series, index) => ({
        name: series.name,
        type: kind === "bar" ? "bar" : "line",
        data: series.points,
        showSymbol: false,
        smooth: kind !== "bar" ? 0.18 : undefined,
        lineStyle: { width: 1.9 },
        emphasis: { focus: "series" as const },
        areaStyle:
          kind === "area"
            ? {
                opacity: 0.16,
                color: colours[index % colours.length],
              }
            : undefined,
      })),
    };
  }

  return {
    color: colours,
    grid: { ...grid, top: showLegend ? 34 : 14 },
    legend,
    tooltip: {
      ...baseTooltip(theme),
      trigger: "axis",
      axisPointer: { type: "shadow" },
      valueFormatter: (value) =>
        `${decimal(Number(value))}${unit ? ` ${unit}` : ""}`,
    },
    xAxis: {
      type: "category",
      data: shaped.categories,
      axisLabel: {
        color: theme.muted,
        fontSize: 11,
        interval: 0,
        hideOverlap: true,
      },
      axisLine: { lineStyle: { color: theme.grid } },
      axisTick: { show: false },
    },
    yAxis: valueAxis,
    animationDuration: 260,
    series: shaped.bars.map((bar) => ({
      name: bar.name,
      type: "bar",
      data: bar.values,
      barMaxWidth: 34,
      itemStyle: { borderRadius: [4, 4, 0, 0] },
      emphasis: { focus: "series" as const },
    })),
  };
}

/** Minimal inline chart for a stat tile's trend, with no axes or labels. */
export function sparklineOption(points: [number, number][]): EChartsOption {
  const colours = palette();
  return {
    color: [colours[0]],
    grid: { left: 0, right: 0, top: 2, bottom: 0 },
    xAxis: { type: "time", show: false },
    yAxis: { type: "value", show: false, scale: true },
    tooltip: { show: false },
    animation: false,
    series: [
      {
        type: "line",
        data: points,
        showSymbol: false,
        smooth: 0.3,
        lineStyle: { width: 1.6 },
        areaStyle: { opacity: 0.14 },
      },
    ],
  };
}

/** Request latency over the recorded history, as p50/p95/max per bucket. */
export function latencyOption(
  buckets: { at: number; p50: number; p95: number; max: number }[],
): EChartsOption {
  const theme = ink();
  const colours = palette();
  const spanMs =
    buckets.length > 1 ? buckets[buckets.length - 1].at - buckets[0].at : 0;
  return {
    color: [colours[0], colours[1], colours[3]],
    grid: { left: 4, right: 8, top: 30, bottom: 4, containLabel: true },
    legend: {
      top: 0,
      left: 0,
      icon: "roundRect",
      itemWidth: 9,
      itemHeight: 9,
      textStyle: { color: theme.muted, fontSize: 11 },
    },
    tooltip: {
      ...baseTooltip(theme),
      valueFormatter: (value) => `${decimal(Number(value))} ms`,
    },
    xAxis: {
      type: "time",
      axisLabel: {
        color: theme.muted,
        fontSize: 11,
        hideOverlap: true,
        formatter: (value: number) => axisLabel(value, spanMs),
      },
      axisLine: { lineStyle: { color: theme.grid } },
      axisTick: { show: false },
    },
    yAxis: {
      type: "value",
      axisLabel: {
        color: theme.muted,
        fontSize: 11,
        formatter: (value: number) => `${decimal(value)} ms`,
      },
      splitLine: { lineStyle: { color: theme.grid, type: "dashed" } },
      axisLine: { show: false },
      axisTick: { show: false },
    },
    series: (["p50", "p95", "max"] as const).map((key) => ({
      name: key,
      type: "line",
      showSymbol: false,
      smooth: 0.2,
      lineStyle: { width: 1.8 },
      data: buckets.map((bucket) => [bucket.at, bucket[key]]),
    })),
  };
}

/** Successful and failed request counts per bucket. */
export function requestsOption(
  buckets: { at: number; ok: number; failed: number }[],
): EChartsOption {
  const theme = ink();
  const spanMs =
    buckets.length > 1 ? buckets[buckets.length - 1].at - buckets[0].at : 0;
  const styles =
    typeof window === "undefined"
      ? null
      : getComputedStyle(document.documentElement);
  const success = styles?.getPropertyValue("--success").trim() || "#059669";
  const danger = styles?.getPropertyValue("--danger").trim() || "#dc2626";
  return {
    color: [success, danger],
    grid: { left: 4, right: 8, top: 30, bottom: 4, containLabel: true },
    legend: {
      top: 0,
      left: 0,
      icon: "roundRect",
      itemWidth: 9,
      itemHeight: 9,
      textStyle: { color: theme.muted, fontSize: 11 },
    },
    tooltip: { ...baseTooltip(theme), axisPointer: { type: "shadow" } },
    xAxis: {
      type: "time",
      axisLabel: {
        color: theme.muted,
        fontSize: 11,
        hideOverlap: true,
        formatter: (value: number) => axisLabel(value, spanMs),
      },
      axisLine: { lineStyle: { color: theme.grid } },
      axisTick: { show: false },
    },
    yAxis: {
      type: "value",
      minInterval: 1,
      axisLabel: { color: theme.muted, fontSize: 11 },
      splitLine: { lineStyle: { color: theme.grid, type: "dashed" } },
      axisLine: { show: false },
      axisTick: { show: false },
    },
    series: [
      {
        name: "2xx–3xx",
        type: "bar",
        stack: "requests",
        data: buckets.map((bucket) => [bucket.at, bucket.ok]),
      },
      {
        name: "4xx–5xx",
        type: "bar",
        stack: "requests",
        data: buckets.map((bucket) => [bucket.at, bucket.failed]),
      },
    ],
  };
}

/**
 * Bucket telemetry samples into fixed windows. Percentiles are computed with
 * nearest-rank on the sorted bucket, which is exact for the small sample counts
 * a 2,000-entry history produces.
 */
export function bucketTelemetry(
  samples: { time: number; duration_ms: number; status: number }[],
  minutes: number,
  bucketCount = 40,
) {
  const now = Date.now();
  const spanMs = minutes * 60_000;
  const width = spanMs / bucketCount;
  const buckets = Array.from({ length: bucketCount }, (_, index) => ({
    at: now - spanMs + index * width + width / 2,
    durations: [] as number[],
    ok: 0,
    failed: 0,
  }));
  for (const sample of samples) {
    const offset = sample.time - (now - spanMs);
    if (offset < 0 || offset > spanMs) continue;
    const index = Math.min(bucketCount - 1, Math.floor(offset / width));
    buckets[index].durations.push(sample.duration_ms);
    if (sample.status >= 400) buckets[index].failed += 1;
    else buckets[index].ok += 1;
  }
  const quantile = (sorted: number[], fraction: number) =>
    sorted.length === 0
      ? 0
      : sorted[
          Math.min(sorted.length - 1, Math.floor(fraction * sorted.length))
        ];
  return buckets.map((bucket) => {
    const sorted = [...bucket.durations].sort((a, b) => a - b);
    return {
      at: bucket.at,
      p50: quantile(sorted, 0.5),
      p95: quantile(sorted, 0.95),
      max: sorted.length > 0 ? sorted[sorted.length - 1] : 0,
      ok: bucket.ok,
      failed: bucket.failed,
      count: sorted.length,
    };
  });
}
