"use client";

import { useEffect, useState } from "react";
import { ago } from "../lib/format";
import type { Risk } from "../lib/vitals";
import type { Live } from "./useLiveData";

export function StatTile({
  label,
  value,
  unit,
  sub,
}: {
  label: string;
  value: React.ReactNode;
  unit?: string;
  sub?: React.ReactNode;
}) {
  return (
    <div className="tile">
      <div className="label">{label}</div>
      <div className="value">
        {value}
        {unit && <span className="unit">{unit}</span>}
      </div>
      {sub && <div className="sub">{sub}</div>}
    </div>
  );
}

export function Segmented<T extends string>({
  label,
  options,
  value,
  onChange,
}: {
  label: string;
  options: { value: T; label: string }[];
  value: T;
  onChange: (value: T) => void;
}) {
  return (
    <div className="segmented" role="group" aria-label={label}>
      {options.map((option) => (
        <button key={option.value} type="button" aria-pressed={option.value === value} onClick={() => onChange(option.value)}>
          {option.label}
        </button>
      ))}
    </div>
  );
}

export interface LegendItem {
  label: string;
  color: string;
  kind?: "line" | "box";
}

export function Legend({ items }: { items: LegendItem[] }) {
  return (
    <ul className="legend">
      {items.map((item) => (
        <li key={item.label}>
          <span className={item.kind === "box" ? "key-box" : "key-line"} style={{ background: item.color }} />
          {item.label}
        </li>
      ))}
    </ul>
  );
}

export interface TableView {
  columns: string[];
  rows: (string | number)[][];
}

/** A chart in a card, with a table of the same numbers one click away. */
export function ChartCard({
  title,
  subtitle,
  legend,
  table,
  children,
}: {
  title: string;
  subtitle?: React.ReactNode;
  legend?: LegendItem[];
  table?: TableView;
  children: React.ReactNode;
}) {
  const [showTable, setShowTable] = useState(false);
  return (
    <section className="card" aria-label={title}>
      <div className="card-head">
        <div>
          <h2>{title}</h2>
          {subtitle && <p>{subtitle}</p>}
        </div>
        {table && (
          <button type="button" className="link-button" onClick={() => setShowTable((v) => !v)} aria-pressed={showTable}>
            {showTable ? "Chart" : "Table"}
          </button>
        )}
      </div>
      {legend && legend.length > 1 && !showTable && <Legend items={legend} />}
      {showTable && table ? (
        <div className="table-wrap" style={{ maxHeight: 320, overflowY: "auto" }}>
          <table className="data">
            <thead>
              <tr>
                {table.columns.map((column, i) => (
                  <th key={column} className={i > 0 ? "num" : undefined}>
                    {column}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {table.rows.map((row, r) => (
                <tr key={r}>
                  {row.map((cell, i) => (
                    <td key={i} className={i > 0 ? "num" : undefined}>
                      {cell}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        children
      )}
    </section>
  );
}

const RISK: Record<Risk, { label: string; color: string; icon: "check" | "alert" | "triangle" | "octagon" }> = {
  low: { label: "Low", color: "var(--good)", icon: "check" },
  "low-medium": { label: "Low–medium", color: "var(--warning)", icon: "alert" },
  medium: { label: "Medium", color: "var(--serious)", icon: "triangle" },
  high: { label: "High", color: "var(--critical)", icon: "octagon" },
};

export function riskColor(risk: Risk): string {
  return RISK[risk].color;
}

/** Clinical risk: always an icon and a word beside the color. */
export function RiskBadge({ risk }: { risk: Risk }) {
  const { label, color, icon } = RISK[risk];
  return (
    <span className="badge">
      <svg width="14" height="14" viewBox="0 0 16 16" aria-hidden="true">
        {icon === "check" && (
          <>
            <circle cx="8" cy="8" r="7" fill={color} />
            <path d="M4.8 8.2l2.1 2.1 4.3-4.5" stroke="#fff" strokeWidth="1.8" fill="none" strokeLinecap="round" />
          </>
        )}
        {icon === "alert" && (
          <>
            <circle cx="8" cy="8" r="7" fill={color} />
            <path d="M8 4.2v4.6M8 11.2v.3" stroke="#0b0b0b" strokeWidth="1.8" strokeLinecap="round" />
          </>
        )}
        {icon === "triangle" && (
          <>
            <path d="M8 1.5l7 12.5H1z" fill={color} />
            <path d="M8 6v3.8M8 12v.2" stroke="#fff" strokeWidth="1.7" strokeLinecap="round" />
          </>
        )}
        {icon === "octagon" && (
          <>
            <path d="M5.2 1h5.6L15 5.2v5.6L10.8 15H5.2L1 10.8V5.2z" fill={color} />
            <path d="M8 4.3v4.5M8 11.3v.2" stroke="#fff" strokeWidth="1.8" strokeLinecap="round" />
          </>
        )}
      </svg>
      {label}
    </span>
  );
}

function useNow(intervalMs = 5_000): number {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), intervalMs);
    return () => clearInterval(id);
  }, [intervalMs]);
  return now;
}

/** "Live · updated 12 s ago", or a way to resume after the tab went idle. */
export function LiveStatus<T>({ live }: { live: Live<T> }) {
  const now = useNow();
  if (live.paused) {
    return (
      <span className="live">
        <span className="status-dot" />
        Paused while you were away.{" "}
        <button type="button" className="link-button" onClick={live.resume}>
          Resume
        </button>
      </span>
    );
  }
  if (!live.updatedAt) return null;
  return (
    <span className="live" aria-live="polite">
      <span className={`status-dot ${live.refreshing ? "wait" : "ok"}`} />
      {live.refreshing ? "Updating…" : `Live · updated ${ago(now - live.updatedAt)}`}
    </span>
  );
}

/** Explains a sleeping FluxDB or a configuration problem instead of an empty page. */
export function LoadState<T>({ live }: { live: Live<T> }) {
  const now = useNow(1_000);
  if (live.wakingSince !== null && live.phase !== "error") {
    const seconds = Math.max(0, Math.round((now - live.wakingSince) / 1000));
    return (
      <div className="banner" role="status">
        <span className="status-dot wait icon" />
        <div>
          <strong>Waking FluxDB… {seconds > 0 ? `${seconds} s` : ""}</strong>
          <p>
            FluxDB runs on Azure Container Apps and scales to zero after 30 quiet minutes, so the first request starts
            it. This usually takes 10 to 40 seconds; the page fills in by itself.
          </p>
        </div>
      </div>
    );
  }
  if (live.error) {
    const hint: Record<string, string> = {
      not_configured: "This deployment has no FluxDB connection yet.",
      bad_key: "The FluxDB API key was refused. Create a new key with read and write scopes.",
      missing_bucket: "Create the bucket in the FluxDB console, or set the bucket name variables.",
      quota: "The FluxDB project is full. Shorten the bucket retention or delete data.",
    };
    return (
      <div className="banner" role="alert">
        <span className="status-dot bad icon" />
        <div>
          <strong>{hint[live.error.error] ?? "FluxDB could not answer this request."}</strong>
          <p>{live.error.message}</p>
        </div>
      </div>
    );
  }
  return null;
}
