"use client";

import { useState } from "react";
import { clock, compact, fixed, price, signedPercent } from "../../lib/format";
import { extent, linear, niceTicks, timeTicks } from "./scale";
import { useWidth } from "./useWidth";

export interface Candle {
  t: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

interface Props {
  candles: Candle[];
  domain: [number, number];
  bucketMs: number;
  /** Unit of the volume, e.g. BTC. */
  base: string;
  timeZone?: string;
  label: string;
  refreshing?: boolean;
}

const PRICE_HEIGHT = 250;
const VOLUME_HEIGHT = 64;
const TOP = 10;
const AXIS = 24;

function shortPrice(value: number): string {
  if (Math.abs(value) >= 10_000) return `${fixed(value / 1000, 1)}K`;
  if (Math.abs(value) >= 100) return fixed(value, 0);
  return fixed(value, 2);
}

/**
 * Candlesticks with volume underneath. A rising candle is hollow, a falling
 * one filled, so direction reads without color too.
 */
export function CandleChart({ candles, domain, bucketMs, base, timeZone, label, refreshing = false }: Props) {
  const [ref, width] = useWidth<HTMLDivElement>();
  const [hover, setHover] = useState<number | null>(null);
  const range = extent(candles.flatMap((c) => [c.low, c.high])) ?? [0, 1];
  const { ticks, domain: yDomain } = niceTicks(range[0], range[1], 4);
  const tickLabels = ticks.map(shortPrice);
  const left = Math.max(36, Math.max(...tickLabels.map((text) => text.length)) * 6.4 + 10);
  const plotWidth = Math.max(40, width - left - 14);
  const x = linear(domain, [left, left + plotWidth]);
  const y = linear(yDomain, [PRICE_HEIGHT, TOP]);
  const maxVolume = Math.max(1e-9, ...candles.map((c) => c.volume));
  const volumeTop = PRICE_HEIGHT + 14;
  const v = linear([0, maxVolume], [volumeTop + VOLUME_HEIGHT, volumeTop]);
  const total = volumeTop + VOLUME_HEIGHT + AXIS;
  const slot = (plotWidth * bucketMs) / (domain[1] - domain[0]);
  const body = Math.max(1.5, Math.min(12, slot * 0.62));
  const hovered = hover === null ? null : candles[hover];
  const hoverX = hovered ? x(hovered.t + bucketMs / 2) : 0;
  const flip = hoverX > left + plotWidth * 0.6;

  const pick = (clientX: number, rect: DOMRect) => {
    if (!candles.length) return;
    const t = domain[0] + ((clientX - rect.left - left) / plotWidth) * (domain[1] - domain[0]) - bucketMs / 2;
    let best = 0;
    for (let i = 1; i < candles.length; i++) if (Math.abs(candles[i].t - t) < Math.abs(candles[best].t - t)) best = i;
    setHover(best);
  };

  return (
    <div ref={ref} className={`chart${refreshing ? " refreshing" : ""}`}>
      <svg
        width={width}
        height={total}
        role="img"
        aria-label={label}
        tabIndex={candles.length ? 0 : -1}
        onPointerMove={(event) => pick(event.clientX, event.currentTarget.getBoundingClientRect())}
        onPointerLeave={() => setHover(null)}
        onFocus={() => setHover(candles.length - 1)}
        onBlur={() => setHover(null)}
        onKeyDown={(event) => {
          if (event.key === "ArrowLeft") setHover((h) => Math.max(0, (h ?? candles.length) - 1));
          else if (event.key === "ArrowRight") setHover((h) => Math.min(candles.length - 1, (h ?? -1) + 1));
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
        <text x={left - 8} y={volumeTop + 4} dy="0.32em" textAnchor="end">
          Vol
        </text>
        <line className="axis-line" x1={left} x2={left + plotWidth} y1={volumeTop + VOLUME_HEIGHT} y2={volumeTop + VOLUME_HEIGHT} />
        {timeTicks(domain[0], domain[1], plotWidth, timeZone).map((tick) => (
          <text key={tick.t} x={x(tick.t)} y={total - 6} textAnchor="middle">
            {tick.label}
          </text>
        ))}
        {hovered && (
          <line className="crosshair" x1={hoverX} x2={hoverX} y1={TOP} y2={volumeTop + VOLUME_HEIGHT} pointerEvents="none" />
        )}
        {candles.map((c, i) => {
          const center = x(c.t + bucketMs / 2);
          const up = c.close >= c.open;
          const color = up ? "var(--candle-up)" : "var(--candle-down)";
          const top = y(Math.max(c.open, c.close));
          const bottom = y(Math.min(c.open, c.close));
          const h = Math.max(1, bottom - top);
          const dim = hover !== null && hover !== i;
          return (
            <g key={c.t} opacity={dim ? 0.6 : 1}>
              <line x1={center} x2={center} y1={y(c.high)} y2={y(c.low)} stroke={color} strokeWidth={1} />
              {up ? (
                <rect
                  x={center - body / 2 + 0.75}
                  y={top + 0.75}
                  width={Math.max(0.5, body - 1.5)}
                  height={Math.max(0.5, h - 1.5)}
                  fill="var(--surface)"
                  stroke={color}
                  strokeWidth={1.5}
                />
              ) : (
                <rect x={center - body / 2} y={top} width={body} height={h} fill={color} />
              )}
              <rect
                x={center - body / 2}
                y={v(c.volume)}
                width={body}
                height={Math.max(0.5, volumeTop + VOLUME_HEIGHT - v(c.volume))}
                fill="var(--axis-text)"
                opacity={0.45}
              />
            </g>
          );
        })}
        <rect className="hit" x={left} y={TOP} width={plotWidth} height={volumeTop + VOLUME_HEIGHT - TOP} />
      </svg>
      {hovered && (
        <div
          className="tooltip"
          aria-hidden="true"
          style={{ top: TOP, ...(flip ? { right: Math.max(0, width - hoverX + 16) } : { left: hoverX + 16 }) }}
        >
          <div className="when">
            {clock(hovered.t, timeZone, true)} – {clock(hovered.t + bucketMs, timeZone)}
          </div>
          {(
            [
              ["Open", price(hovered.open)],
              ["High", price(hovered.high)],
              ["Low", price(hovered.low)],
              ["Close", price(hovered.close)],
              ["Change", signedPercent(((hovered.close - hovered.open) / hovered.open) * 100)],
              ["Volume", `${compact(hovered.volume)} ${base}`],
            ] as const
          ).map(([name, value]) => (
            <div className="row" key={name}>
              <span className="name">{name}</span>
              <strong>{value}</strong>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
