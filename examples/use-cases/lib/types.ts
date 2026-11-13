/** Shapes the route handlers send to the dashboards. */
import type { Interval } from "./fluxdb";
import type { News2, Vitals } from "./vitals";
import type { WindowKey } from "./time";

/** What one request did in FluxDB, for the "How this page uses FluxDB" panel. */
export interface FluxActivity {
  bucket: string;
  ingest: {
    stream: string;
    written: number;
    range: { from: number; to: number } | null;
    source: string | null;
    requests: number;
    totalMs: number;
    sample: string[];
    error?: string;
  }[];
  queries: {
    label: string;
    sql: string;
    window: { from: number; to: number; interval?: Interval };
    rows: number;
    serverMs: number;
    totalMs: number;
  }[];
  /** The whole request, FluxDB and everything else. */
  totalMs: number;
  generatedAt: number;
}

export interface SeriesPoint {
  t: number;
  v: number | null;
}

export interface WindowInfo {
  key: WindowKey;
  from: number;
  to: number;
  interval: Interval;
  intervalMs: number;
}

export interface EnergyResponse {
  window: WindowInfo;
  timeZone: string;
  buildings: {
    id: string;
    name: string;
    series: SeriesPoint[];
    nowKw: number | null;
    peakKw: number | null;
    netKwh: number | null;
    solarKwh: number | null;
  }[];
  endUse: { t: number; hvac_kw: number; lighting_kw: number; plug_kw: number; ev_kw: number }[];
  solar: SeriesPoint[];
  outdoor: SeriesPoint[];
  kpis: {
    demandNowKw: number | null;
    demandPeakKw: number | null;
    energyTodayKwh: number | null;
    costToday: number | null;
    solarTodayKwh: number | null;
    outdoorNowC: number | null;
  };
  flux: FluxActivity;
}

export interface BedStatus {
  id: string;
  patient: string;
  reason: string;
  spo2Scale: 1 | 2;
  latest: (Vitals & { at: number | null }) | null;
  score: News2 | null;
  hrTrend: SeriesPoint[];
}

export interface VitalsResponse {
  window: WindowInfo;
  timeZone: string;
  beds: BedStatus[];
  selected: {
    id: string;
    series: Record<"hr" | "spo2" | "rr" | "sbp" | "temp_c", { t: number; mean: number | null; lo: number | null; hi: number | null }[]>;
    manual: { t: number; hr: number | null; spo2: number | null; rr: number | null; sbp: number | null; temp_c: number | null }[];
  };
  /** Minutes outside the normal range, per bed per time bucket. */
  alerts: { interval: Interval; intervalMs: number; cells: { bed: string; t: number; minutes: number }[] };
  observations: number;
  flux: FluxActivity;
}

export interface MarketsResponse {
  window: WindowInfo;
  product: string;
  candles: { t: number; open: number; high: number; low: number; close: number; volume: number }[];
  overview: {
    id: string;
    name: string;
    base: string;
    open: number | null;
    last: number | null;
    high: number | null;
    low: number | null;
    volume: number | null;
    stddev: number | null;
  }[];
  /** Each product's close, indexed to 100 at the start of the window. */
  performance: { id: string; series: SeriesPoint[] }[];
  feed: { source: string | null; error: string | null };
  flux: FluxActivity;
}

export interface ApiError {
  error: "not_configured" | "waking" | "bad_key" | "missing_bucket" | "quota" | "invalid" | "rate_limited" | "failed";
  message: string;
}
