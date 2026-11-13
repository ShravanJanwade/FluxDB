"use client";

import { useMemo, useState } from "react";
import { clock } from "../../lib/format";
import { DAY } from "../../lib/time";
import type { SeriesPoint } from "../../lib/types";
import { extent, linear, niceTicks, timeTicks } from "./scale";
import { useWidth } from "./useWidth";

export interface LineSeries {
  id: string;
  label: string;
  /** Shorter name for the label at the line's end. */
  short?: string;
  color: string;
  points: SeriesPoint[];
  /** The min-max spread of each bucket, drawn as a wash behind the line. */
  band?: { t: number; lo: number | null; hi: number | null }[];
  /** Wash the area under the line; for single-series charts. */
  area?: boolean;
}

export interface Marker {
  t: number;
  v: number;
}

interface Props {
  series: LineSeries[];
  domain: [number, number];
  /** Spacing of the data; a longer gap breaks the line instead of bridging it. */
  stepMs: number;
  format: (value: number) => string;
  axisFormat?: (value: number) => string;
  height?: number;
  includeZero?: boolean;
  /** A shaded reference range, such as a vital sign's normal values. */
  refBand?: { lo: number; hi: number };
  /** Points recorded by hand, drawn as rings. */
  markers?: Marker[];
  markerLabel?: string;
  /** A value to draw as a firmer rule, such as 100 on an indexed chart. */
  rule?: number;
  endLabels?: boolean;
  timeZone?: string;
  label: string;
  refreshing?: boolean;
}

const TOP = 10;
const BOTTOM = 24;

function segments<T extends { t: number }>(points: T[], valid: (p: T) => boolean, maxGap: number): T[][] {
  const runs: T[][] = [];
  let run: T[] = [];
  let previous: number | null = null;
  for (const point of points) {
    if (!valid(point)) {
      if (run.length) runs.push(run);
      run = [];
      previous = null;
      continue;
    }
    if (previous !== null && point.t - previous > maxGap && run.length) {
      runs.push(run);
      run = [];
    }
    run.push(point);
    previous = point.t;
  }
  if (run.length) runs.push(run);
  return runs;
}

export function LineChart({
  series,
  domain,
  stepMs,
  format,
  axisFormat = format,
  height = 240,
  includeZero = false,
  refBand,
  markers = [],
  markerLabel = "Recorded",
  rule,
  endLabels = false,
  timeZone,
  label,
  refreshing = false,
}: Props) {
  const [ref, width] = useWidth<HTMLDivElement>();
  const [hover, setHover] = useState<number | null>(null);

  const model = useMemo(() => {
    const values: (number | null)[] = [];
    for (const s of series) {
      for (const p of s.points) values.push(p.v);
      for (const b of s.band ?? []) values.push(b.lo, b.hi);
    }
    for (const m of markers) values.push(m.v);
    // The reference band is clipped to the data's range rather than stretching it.
    if (includeZero) values.push(0);
    if (rule !== undefined) values.push(rule);
    const range = extent(values) ?? [0, 1];
    const { ticks, domain: yDomain } = niceTicks(range[0], range[1], height > 180 ? 4 : 3);
    const tickLabels = ticks.map(axisFormat);
    const left = Math.max(28, Math.max(...tickLabels.map((text) => text.length)) * 6.4 + 10);
    const lastPoints = series.map((s) => [...s.points].reverse().find((p) => p.v !== null) ?? null);
    const showEnds = endLabels && width >= 480;
    const endTexts = series.map((s, i) => {
      const last = lastPoints[i];
      return last && last.v !== null ? `${s.short ?? s.label} ${format(last.v)}` : "";
    });
    const right = showEnds ? Math.max(...endTexts.map((text) => text.length)) * 6.3 + 18 : 14;
    const plotWidth = Math.max(40, width - left - right);
    const x = linear(domain, [left, left + plotWidth]);
    const y = linear(yDomain, [height - BOTTOM, TOP]);
    const times = [...new Set(series.flatMap((s) => s.points.filter((p) => p.v !== null).map((p) => p.t)))].sort((a, b) => a - b);
    const lookup = series.map((s) => new Map(s.points.map((p) => [p.t, p.v])));

    // End labels only when they clear each other; otherwise the legend carries identity.
    let ends: { y: number; text: string; color: string }[] = [];
    if (showEnds) {
      ends = series
        .map((s, i) => ({ y: lastPoints[i]?.v != null ? y(lastPoints[i]!.v!) : NaN, text: endTexts[i], color: s.color }))
        .filter((end) => Number.isFinite(end.y) && end.text);
      const sorted = [...ends].sort((a, b) => a.y - b.y);
      if (sorted.some((end, i) => i > 0 && end.y - sorted[i - 1].y < 13)) ends = [];
    }
    return { ticks, tickLabels, left, right, plotWidth, x, y, times, lookup, lastPoints, ends, yDomain };
  }, [series, markers, includeZero, rule, height, axisFormat, endLabels, width, format, domain]);

  const { ticks, tickLabels, left, plotWidth, x, y, times, lookup, lastPoints, ends, yDomain } = model;
  const maxGap = stepMs * 1.5;
  const xTicks = timeTicks(domain[0], domain[1], plotWidth, timeZone);
  const withDay = domain[1] - domain[0] > DAY * 0.99;
  const baseValue = Math.max(yDomain[0], Math.min(yDomain[1], 0));

  const hoverT = hover !== null ? times[hover] : null;
  const pick = (clientX: number, rect: DOMRect) => {
    if (!times.length) return;
    const target = domain[0] + ((clientX - rect.left - left) / plotWidth) * (domain[1] - domain[0]);
    let lo = 0;
    let hi = times.length - 1;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (times[mid] < target) lo = mid + 1;
      else hi = mid;
    }
    const index = lo > 0 && Math.abs(times[lo - 1] - target) < Math.abs(times[lo] - target) ? lo - 1 : lo;
    setHover(index);
  };

  const nearbyMarkers = hoverT === null ? [] : markers.filter((m) => Math.abs(m.t - hoverT) <= stepMs / 2);
  const tooltipLeft = hoverT === null ? 0 : x(hoverT);
  const flip = tooltipLeft > left + plotWidth * 0.6;

  return (
    <div ref={ref} className={`chart${refreshing ? " refreshing" : ""}`}>
      <svg
        width={width}
        height={height}
        role="img"
        aria-label={label}
        tabIndex={times.length ? 0 : -1}
        onPointerMove={(event) => pick(event.clientX, event.currentTarget.getBoundingClientRect())}
        onPointerLeave={() => setHover(null)}
        onFocus={() => setHover(times.length - 1)}
        onBlur={() => setHover(null)}
        onKeyDown={(event) => {
          if (!times.length) return;
          if (event.key === "ArrowLeft") setHover((h) => Math.max(0, (h ?? times.length) - 1));
          else if (event.key === "ArrowRight") setHover((h) => Math.min(times.length - 1, (h ?? -1) + 1));
          else if (event.key === "Escape") setHover(null);
          else return;
          event.preventDefault();
        }}
      >
        {refBand && (
          <rect
            x={left}
            width={plotWidth}
            y={y(Math.min(refBand.hi, yDomain[1]))}
            height={Math.max(0, y(Math.max(refBand.lo, yDomain[0])) - y(Math.min(refBand.hi, yDomain[1])))}
            fill="var(--grid)"
            opacity={0.55}
          />
        )}
        {ticks.map((tick, i) => (
          <g key={tick}>
            <line className="grid-line" x1={left} x2={left + plotWidth} y1={y(tick)} y2={y(tick)} />
            <text x={left - 8} y={y(tick)} dy="0.32em" textAnchor="end">
              {tickLabels[i]}
            </text>
          </g>
        ))}
        {rule !== undefined && <line className="axis-line" x1={left} x2={left + plotWidth} y1={y(rule)} y2={y(rule)} />}
        <line className="axis-line" x1={left} x2={left + plotWidth} y1={height - BOTTOM} y2={height - BOTTOM} />
        {xTicks.map((tick) => (
          <text key={tick.t} x={x(tick.t)} y={height - 6} textAnchor="middle">
            {tick.label}
          </text>
        ))}

        {series.map((s) =>
          segments(s.band ?? [], (b) => b.lo !== null && b.hi !== null, maxGap).map((run, i) => {
            const upper = run.map((b) => `${x(b.t).toFixed(1)},${y(b.hi!).toFixed(1)}`);
            const lower = [...run].reverse().map((b) => `${x(b.t).toFixed(1)},${y(b.lo!).toFixed(1)}`);
            return <path key={`${s.id}-band-${i}`} d={`M${upper.join("L")}L${lower.join("L")}Z`} fill={s.color} opacity={0.14} />;
          }),
        )}
        {series.map((s) =>
          s.area
            ? segments(s.points, (p) => p.v !== null, maxGap).map((run, i) => {
                const top = run.map((p) => `${x(p.t).toFixed(1)},${y(p.v!).toFixed(1)}`).join("L");
                const base = y(baseValue).toFixed(1);
                return (
                  <path
                    key={`${s.id}-area-${i}`}
                    d={`M${x(run[0].t).toFixed(1)},${base}L${top}L${x(run[run.length - 1].t).toFixed(1)},${base}Z`}
                    fill={s.color}
                    opacity={0.1}
                  />
                );
              })
            : null,
        )}
        {series.map((s) => (
          <path
            key={`${s.id}-line`}
            d={segments(s.points, (p) => p.v !== null, maxGap)
              .map((run) => "M" + run.map((p) => `${x(p.t).toFixed(1)},${y(p.v!).toFixed(1)}`).join("L"))
              .join("")}
            fill="none"
            stroke={s.color}
            strokeWidth={2}
            strokeLinejoin="round"
            strokeLinecap="round"
          />
        ))}
        {markers.map((m, i) => (
          <circle key={`marker-${i}`} cx={x(m.t)} cy={y(m.v)} r={4.5} fill="var(--surface)" stroke="var(--ink)" strokeWidth={2} />
        ))}
        {series.map((s, i) => {
          const last = lastPoints[i];
          if (!last || last.v === null) return null;
          return <circle key={`${s.id}-end`} cx={x(last.t)} cy={y(last.v)} r={4} fill={s.color} stroke="var(--surface)" strokeWidth={2} />;
        })}
        {ends.map((end) => (
          <text key={end.text} className="end-label" x={left + plotWidth + 10} y={end.y} dy="0.32em">
            {end.text}
          </text>
        ))}

        {hoverT !== null && (
          <g pointerEvents="none">
            <line className="crosshair" x1={x(hoverT)} x2={x(hoverT)} y1={TOP} y2={height - BOTTOM} />
            {series.map((s, i) => {
              const v = lookup[i].get(hoverT);
              return v === null || v === undefined ? null : (
                <circle key={s.id} cx={x(hoverT)} cy={y(v)} r={4} fill={s.color} stroke="var(--surface)" strokeWidth={2} />
              );
            })}
          </g>
        )}
        <rect className="hit" x={left} y={TOP} width={plotWidth} height={height - TOP - BOTTOM} />
      </svg>
      {hoverT !== null && (
        <div
          className="tooltip"
          aria-hidden="true"
          style={{
            top: TOP,
            ...(flip ? { right: Math.max(0, width - tooltipLeft + 12) } : { left: tooltipLeft + 12 }),
          }}
        >
          <div className="when">{clock(hoverT, timeZone, withDay)}</div>
          {series.map((s, i) => {
            const v = lookup[i].get(hoverT);
            return (
              <div className="row" key={s.id}>
                <span className="name">
                  <span className="swatch" style={{ background: s.color }} />
                  {s.label}
                </span>
                <strong>{v === null || v === undefined ? "–" : format(v)}</strong>
              </div>
            );
          })}
          {nearbyMarkers.map((m, i) => (
            <div className="row" key={`m-${i}`}>
              <span className="name">{markerLabel}</span>
              <strong>{format(m.v)}</strong>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
