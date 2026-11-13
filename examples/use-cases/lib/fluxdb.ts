/**
 * A small FluxDB client for server code. It speaks FluxDB's project API:
 *
 *   POST /api/ingest/v1/write?bucket=…&precision=ms   line protocol in, 204 out
 *   POST /api/ingest/v1/query                          SQL in, rows out
 *   GET  /api/ingest/v1/whoami                         the key's project and buckets
 *
 * Every call carries a project API key (`Authorization: Bearer fdbk_…`). Keep
 * the key on the server: route handlers call FluxDB, browsers call the route
 * handlers.
 */
import { toLine, type Point } from "./lineProtocol";

/** The `$interval` values FluxDB accepts for `GROUP BY time($interval)`. */
export const INTERVALS = ["10s", "30s", "1m", "2m", "5m", "10m", "15m", "30m", "1h", "3h", "6h", "12h", "1d"] as const;
export type Interval = (typeof INTERVALS)[number];

/** Integers arrive as strings (they can exceed 2^53), floats as numbers. */
export type Cell = string | number | boolean | null;
export type Row = Record<string, Cell>;

export interface QueryWindow {
  /** Milliseconds since the epoch; `$timeFilter` covers [from, to]. */
  from: number;
  to: number;
  /** Fills `$interval`. */
  interval?: Interval;
}

export interface QueryResult {
  /** A short name for the query, shown in the app's FluxDB panel. */
  label: string;
  sql: string;
  window: QueryWindow;
  columns: string[];
  rows: Row[];
  /** Time FluxDB spent executing the query. */
  serverMs: number;
  /** Request to answer, network included. */
  totalMs: number;
}

export interface WriteResult {
  bucket: string;
  points: number;
  requests: number;
  totalMs: number;
  /** The first few lines sent, for display. */
  sample: string[];
}

export interface WhoAmI {
  project: { id: string; name: string };
  key: { id: string; name: string; scopes: string[] };
  buckets: { name: string; retention_seconds: number }[];
}

export class FluxDBError extends Error {
  constructor(
    message: string,
    /** HTTP status, or 0 when FluxDB never answered. */
    readonly status: number,
    /** FluxDB's error code, or `unavailable` when it never answered. */
    readonly code: string,
  ) {
    super(message);
    this.name = "FluxDBError";
  }

  /** No answer, or a gateway error: asleep and starting up, or unreachable. */
  get unavailable(): boolean {
    return this.code === "unavailable";
  }
}

export interface FluxDBOptions {
  url: string;
  apiKey: string;
  /** Per request. A sleeping server needs time to start, so be generous. */
  timeoutMs?: number;
  /** Lines per write request; the server accepts up to 10,000 and 2 MiB. */
  batchSize?: number;
}

export class FluxDB {
  readonly url: string;
  private readonly apiKey: string;
  private readonly timeoutMs: number;
  private readonly batchSize: number;

  constructor(options: FluxDBOptions) {
    this.url = options.url.replace(/\/+$/, "");
    this.apiKey = options.apiKey;
    this.timeoutMs = options.timeoutMs ?? 45_000;
    this.batchSize = options.batchSize ?? 5_000;
  }

  /** The key's project and the buckets it can reach. Cheap; wakes a sleeping server. */
  async whoami(): Promise<WhoAmI> {
    const response = await this.request("/api/ingest/v1/whoami", { method: "GET" });
    return (await response.json()) as WhoAmI;
  }

  /** Write points to a bucket, in as many requests as the batch size needs. */
  async write(bucket: string, points: Point[]): Promise<WriteResult> {
    const started = performance.now();
    const lines = points.map(toLine);
    let requests = 0;
    for (let start = 0; start < lines.length; start += this.batchSize) {
      const body = lines.slice(start, start + this.batchSize).join("\n");
      const query = new URLSearchParams({ bucket, precision: "ms" });
      await this.request(`/api/ingest/v1/write?${query}`, {
        method: "POST",
        headers: { "content-type": "text/plain; charset=utf-8" },
        body,
      });
      requests += 1;
    }
    return {
      bucket,
      points: lines.length,
      requests,
      totalMs: Math.round(performance.now() - started),
      sample: lines.slice(0, 3),
    };
  }

  /**
   * Run SQL against a bucket. `$timeFilter` expands to the window, `$interval`
   * to its interval, so `GROUP BY time($interval)` buckets the rows.
   */
  async query(bucket: string, sql: string, window: QueryWindow, label = "Query"): Promise<QueryResult> {
    const started = performance.now();
    const response = await this.request("/api/ingest/v1/query", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        query: sql,
        bucket,
        from: msToNs(window.from),
        to: msToNs(window.to),
        ...(window.interval ? { interval: window.interval } : {}),
      }),
    });
    const answer = (await response.json()) as { columns: string[]; rows: Cell[][]; execution_time_ms: number };
    const rows = answer.rows.map((cells) => {
      const row: Row = {};
      answer.columns.forEach((column, index) => {
        row[column] = cells[index] ?? null;
      });
      return row;
    });
    return {
      label,
      sql,
      window,
      columns: answer.columns,
      rows,
      serverMs: answer.execution_time_ms,
      totalMs: Math.round(performance.now() - started),
    };
  }

  private async request(path: string, init: RequestInit): Promise<Response> {
    let response: Response;
    try {
      response = await fetch(this.url + path, {
        ...init,
        headers: { ...init.headers, authorization: `Bearer ${this.apiKey}` },
        signal: AbortSignal.timeout(this.timeoutMs),
        cache: "no-store",
      });
    } catch (error) {
      const reason = error instanceof Error ? error.message : String(error);
      throw new FluxDBError(`FluxDB did not answer (${reason})`, 0, "unavailable");
    }
    if (response.ok) return response;
    // Azure answers 502-504 while a scaled-to-zero app starts.
    if (response.status >= 502 && response.status <= 504) {
      throw new FluxDBError(`FluxDB is not ready yet (HTTP ${response.status})`, response.status, "unavailable");
    }
    let code = `http_${response.status}`;
    let message = `FluxDB answered HTTP ${response.status}`;
    try {
      const body = (await response.json()) as { code?: string; error?: string };
      if (body.code) code = body.code;
      if (body.error) message = body.error;
    } catch {
      // Not JSON; keep the generic message.
    }
    throw new FluxDBError(message, response.status, code);
  }
}

export function msToNs(ms: number): string {
  return (BigInt(Math.round(ms)) * 1_000_000n).toString();
}

/** A `time` cell (nanoseconds as a string) in milliseconds. */
export function nsToMs(value: Cell): number {
  if (typeof value === "string") return Number(BigInt(value) / 1_000_000n);
  if (typeof value === "number") return Math.round(value / 1_000_000);
  return Number.NaN;
}

/** A cell as a number: floats pass through, integer strings are parsed, anything else is null. */
export function num(value: Cell | undefined): number | null {
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

/** A cell as a string (tags, string fields). */
export function str(value: Cell | undefined): string {
  return value === null || value === undefined ? "" : String(value);
}
