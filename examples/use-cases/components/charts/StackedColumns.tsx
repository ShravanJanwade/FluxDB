"use client";

import { useMemo, useState } from "react";
import { clock } from "../../lib/format";
import { DAY } from "../../lib/time";
import { linear, niceTicks, timeTicks } from "./scale";
import { useWidth } from "./useWidth";

export interface StackKey {
  id: string;
  label: string;
  color: string;
}

interface Props {
  keys: StackKey[];
  rows: { t: number; values: Record<string, number> }[];
  domain: [number, number];
  bucketMs: number;
  format: (value: number) => string;
  axisFormat?: (value: number) => string;
  height?: number;
  timeZone?: string;
  label: string;
  refreshing?: boolean;
}

const TOP = 10;
const BOTTOM = 24;
const GAP = 2;

/** A column with a 4px rounded top and a square foot. */
function roundedTop(x: number, y: number, w: number, h: number): string {
  const r = Math.min(4, w / 2, h);
  return `M${x},${y + h}V${y + r}Q${x},${y} ${x + r},${y}H${x + w - r}Q${x + w},${y} ${x + w},${y + r}V${y + h}Z`;
}

export function StackedColumns({
  keys,
  rows,
  domain,
  bucketMs,
  format,
  axisFormat = format,
  height = 240,
  timeZone,
  label,
  refreshing = false,
}: Props) {
  const [ref, width] = useWidth<HTMLDivElement>();
  const [hover, setHover] = useState<number | null>(null);

  const totals = rows.map((row) => keys.reduce((sum, key) => sum + Math.max(0, row.values[key.id] ?? 0), 0));
  const { ticks, domain: yDomain } = useMemo(() => niceTicks(0, Math.max(1, ...totals), 4), [totals]);
  const tickLabels = ticks.map(axisFormat);
  const left = Math.max(28, Math.max(...tickLabels.map((text) => text.length)) * 6.4 + 10);
  const plotWidth = Math.max(40, width - left - 14);
  const x = linear(domain, [left, left + plotWidth]);
  const y = linear(yDomain, [height - BOTTOM, TOP]);
  const slot = (plotWidth * bucketMs) / (domain[1] - domain[0]);
  const barWidth = Math.max(2, Math.min(24, slot - GAP));
  const withDay = domain[1] - domain[0] > DAY * 0.99;
  const hovered = hover === null ? null : rows[hover];
  const hoverX = hovered ? x(hovered.t + bucketMs / 2) : 0;
  const flip = hoverX > left + plotWidth * 0.6;

  return (
    <div ref={ref} className={`chart${refreshing ? " refreshing" : ""}`}>
      <svg
        width={width}
        height={height}
        role="img"
        aria-label={label}
        tabIndex={rows.length ? 0 : -1}
        onPointerLeave={() => setHover(null)}
        onFocus={() => setHover(rows.length - 1)}
        onBlur={() => setHover(null)}
        onKeyDown={(event) => {
          if (event.key === "ArrowLeft") setHover((h) => Math.max(0, (h ?? rows.length) - 1));
          else if (event.key === "ArrowRight") setHover((h) => Math.min(rows.length - 1, (h ?? -1) + 1));
          else return;
          event.preventDefault();
        }}
      >
        {ticks.map((tick, i) => (
          <g key={tick}>
            <line className="grid-line" x1={left} x2={left + plotWidth} y1={y(tick)} y2={y(tick)} />
            <text x={left - 8} y={y(tick)} dy="0.32em" textAnchor="end">
              {tickLabels[i]}
            </text>
          </g>
        ))}
        {timeTicks(domain[0], domain[1], plotWidth, timeZone).map((tick) => (
          <text key={tick.t} x={x(tick.t)} y={height - 6} textAnchor="middle">
            {tick.label}
          </text>
        ))}
        {rows.map((row, index) => {
          const center = x(row.t + bucketMs / 2);
          const x0 = center - barWidth / 2;
          let running = 0;
          const stack = keys.filter((key) => (row.values[key.id] ?? 0) > 0);
          return (
            <g key={row.t} opacity={hover === null || hover === index ? 1 : 0.55}>
              {stack.map((key, i) => {
                const value = row.values[key.id] ?? 0;
                const bottom = y(running);
                running += value;
                const top = y(running);
                const isTop = i === stack.length - 1;
                // A 2px surface gap separates neighbouring segments.
                const y0 = top + (isTop ? 0 : GAP / 2);
                const y1 = bottom - (i === 0 ? 0 : GAP / 2);
                if (y1 - y0 < 0.5) return null;
                return isTop ? (
                  <path key={key.id} d={roundedTop(x0, y0, barWidth, y1 - y0)} fill={key.color} />
                ) : (
                  <rect key={key.id} x={x0} y={y0} width={barWidth} height={y1 - y0} fill={key.color} />
                );
              })}
              <rect
                className="hit"
                x={center - Math.max(slot, 6) / 2}
                y={TOP}
                width={Math.max(slot, 6)}
                height={height - TOP - BOTTOM}
                onPointerEnter={() => setHover(index)}
              />
            </g>
          );
        })}
        <line className="axis-line" x1={left} x2={left + plotWidth} y1={height - BOTTOM} y2={height - BOTTOM} />
      </svg>
      {hovered && (
        <div
          className="tooltip"
          aria-hidden="true"
          style={{ top: TOP, ...(flip ? { right: Math.max(0, width - hoverX + 16) } : { left: hoverX + 16 }) }}
        >
          <div className="when">
            {clock(hovered.t, timeZone, withDay)} – {clock(hovered.t + bucketMs, timeZone)}
          </div>
          {[...keys].reverse().map((key) => (
            <div className="row" key={key.id}>
              <span className="name">
                <span className="swatch" style={{ background: key.color, height: 8, width: 8, borderRadius: 2 }} />
                {key.label}
              </span>
              <strong>{format(hovered.values[key.id] ?? 0)}</strong>
            </div>
          ))}
          <div className="row" style={{ borderTop: "1px solid var(--border)", marginTop: 4, paddingTop: 4 }}>
            <span className="name">Total</span>
            <strong>{format(totals[hover!])}</strong>
          </div>
        </div>
      )}
    </div>
  );
}
