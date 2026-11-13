import type { SeriesPoint } from "../../lib/types";
import { extent, linear } from "./scale";

/** A small trend line with its latest point marked; no axes. */
export function Sparkline({
  points,
  color = "var(--series-1)",
  width = 110,
  height = 30,
  label,
}: {
  points: SeriesPoint[];
  color?: string;
  width?: number;
  height?: number;
  label: string;
}) {
  const valid = points.filter((p): p is { t: number; v: number } => p.v !== null);
  if (valid.length < 2) return <svg width={width} height={height} role="img" aria-label={label} />;
  const [t0, t1] = [valid[0].t, valid[valid.length - 1].t];
  const range = extent(valid.map((p) => p.v)) ?? [0, 1];
  const pad = (range[1] - range[0]) * 0.1 || 1;
  const x = linear([t0, t1], [4, width - 6]);
  const y = linear([range[0] - pad, range[1] + pad], [height - 4, 4]);
  const last = valid[valid.length - 1];
  return (
    <svg width={width} height={height} role="img" aria-label={label} style={{ display: "block", overflow: "visible" }}>
      <path
        d={"M" + valid.map((p) => `${x(p.t).toFixed(1)},${y(p.v).toFixed(1)}`).join("L")}
        fill="none"
        stroke={color}
        strokeWidth={1.5}
        strokeLinejoin="round"
        strokeLinecap="round"
      />
      <circle cx={x(last.t)} cy={y(last.v)} r={3} fill={color} stroke="var(--surface)" strokeWidth={1.5} />
    </svg>
  );
}
