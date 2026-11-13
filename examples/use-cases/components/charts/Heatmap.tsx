"use client";

import { useState } from "react";
import { alignDown } from "../../lib/time";
import { timeTicks } from "./scale";
import { useWidth } from "./useWidth";

/** Sequential steps of one hue; more is darker on light, lighter on dark. */
const RAMP = ["var(--seq-200)", "var(--seq-300)", "var(--seq-400)", "var(--seq-500)", "var(--seq-700)"];

interface Props {
  rows: { id: string; label: string }[];
  cells: { row: string; t: number; value: number }[];
  domain: [number, number];
  bucketMs: number;
  /** The value that fills the darkest step. */
  max: number;
  describe: (row: string, t: number, value: number) => string;
  legend: string;
  timeZone?: string;
  label: string;
}

export function Heatmap({ rows, cells, domain, bucketMs, max, describe, legend, timeZone, label }: Props) {
  const [ref, width] = useWidth<HTMLDivElement>();
  const [hover, setHover] = useState<{ row: string; t: number; value: number; x: number; y: number } | null>(null);
  const start = alignDown(domain[0], bucketMs);
  const columns: number[] = [];
  for (let t = start; t <= domain[1]; t += bucketMs) columns.push(t);
  const values = new Map(cells.map((cell) => [`${cell.row}|${cell.t}`, cell.value]));
  const step = (value: number) => (value <= 0 ? null : RAMP[Math.min(RAMP.length - 1, Math.floor((value / max) * RAMP.length))]);
  const labelWidth = 44;
  const gridWidth = Math.max(40, width - labelWidth);
  const ticks = timeTicks(start, start + columns.length * bucketMs, gridWidth, timeZone);

  return (
    <div ref={ref} className="chart" style={{ position: "relative" }}>
      <div
        className="heatmap"
        role="img"
        aria-label={label}
        style={{ gridTemplateColumns: `${labelWidth}px repeat(${columns.length}, minmax(0, 1fr))` }}
        onPointerLeave={() => setHover(null)}
      >
        {rows.map((row) => (
          <div key={row.id} style={{ display: "contents" }}>
            <div className="row-label">{row.label}</div>
            {columns.map((t) => {
              const value = values.get(`${row.id}|${t}`) ?? 0;
              const fill = step(value);
              return (
                <div
                  key={t}
                  className="cell"
                  style={fill ? { background: fill } : undefined}
                  onPointerEnter={(event) => {
                    const box = event.currentTarget.getBoundingClientRect();
                    const parent = ref.current!.getBoundingClientRect();
                    setHover({ row: row.id, t, value, x: box.left - parent.left + box.width / 2, y: box.top - parent.top });
                  }}
                />
              );
            })}
          </div>
        ))}
      </div>
      <div style={{ position: "relative", height: 18, marginLeft: labelWidth, marginTop: 4 }}>
        {ticks.map((tick) => (
          <span
            key={tick.t}
            style={{
              position: "absolute",
              left: `${((tick.t - start) / (columns.length * bucketMs)) * 100}%`,
              transform: "translateX(-50%)",
              fontSize: 11,
              color: "var(--axis-text)",
              whiteSpace: "nowrap",
            }}
          >
            {tick.label}
          </span>
        ))}
      </div>
      <div className="scale-legend">
        <span>{legend}</span>
        <span>0</span>
        <span className="ramp">
          <span style={{ background: "var(--surface-2)" }} />
          {RAMP.map((color) => (
            <span key={color} style={{ background: color }} />
          ))}
        </span>
        <span>{max}</span>
      </div>
      {hover && (
        <div
          className="tooltip"
          aria-hidden="true"
          style={{
            top: hover.y - 8,
            transform: "translateY(-100%)",
            ...(hover.x > width * 0.6 ? { right: Math.max(0, width - hover.x - 8) } : { left: Math.max(0, hover.x - 8) }),
          }}
        >
          {describe(hover.row, hover.t, hover.value)}
        </div>
      )}
    </div>
  );
}
