/**
 * Server-side plumbing shared by the route handlers: the configured client,
 * a recorder for the FluxDB panel, and the mapping from failures to answers
 * the dashboards understand. Only route handlers import this file; the API
 * key never reaches a browser bundle.
 */
import { FluxDB, FluxDBError, type QueryResult } from "../fluxdb";
import type { SyncReport } from "../sync";
import type { ApiError, FluxActivity } from "../types";

export interface Settings {
  db: FluxDB;
  buckets: { energy: string; vitals: string; markets: string };
}

export class NotConfiguredError extends Error {}
export class InvalidRequestError extends Error {}
export class RateLimitedError extends Error {}

export function settings(): Settings {
  const url = process.env.FLUXDB_URL?.trim() ?? "";
  const apiKey = process.env.FLUXDB_API_KEY?.trim() ?? "";
  if (!url || !apiKey || apiKey === "fdbk_replace_me") {
    throw new NotConfiguredError("Set FLUXDB_URL and FLUXDB_API_KEY. See .env.example in examples/use-cases.");
  }
  if (!/^https?:\/\//.test(url)) throw new NotConfiguredError("FLUXDB_URL must start with http:// or https://");
  return {
    db: new FluxDB({ url, apiKey }),
    buckets: {
      energy: process.env.FLUXDB_ENERGY_BUCKET?.trim() || "energy",
      vitals: process.env.FLUXDB_VITALS_BUCKET?.trim() || "vitals",
      markets: process.env.FLUXDB_MARKETS_BUCKET?.trim() || "markets",
    },
  };
}

/** Collects what a request did in FluxDB, in order, for the dashboard's FluxDB panel. */
export class Recorder {
  private readonly started = performance.now();
  private readonly queries: QueryResult[] = [];
  private readonly reports: SyncReport[] = [];

  constructor(private readonly bucket: string) {}

  query(result: QueryResult): QueryResult {
    this.queries.push(result);
    return result;
  }

  synced(reports: SyncReport[]): SyncReport[] {
    this.reports.push(...reports);
    return reports;
  }

  activity(): FluxActivity {
    return {
      bucket: this.bucket,
      ingest: this.reports.map((report) => ({
        stream: report.stream,
        written: report.written,
        range: report.range,
        source: report.source,
        requests: report.requests,
        totalMs: report.totalMs,
        sample: report.sample,
        ...(report.error ? { error: report.error } : {}),
      })),
      queries: this.queries.map((query) => ({
        label: query.label,
        sql: query.sql,
        window: query.window,
        rows: query.rows.length,
        serverMs: Math.round(query.serverMs * 100) / 100,
        totalMs: query.totalMs,
      })),
      totalMs: Math.round(performance.now() - this.started),
      generatedAt: Date.now(),
    };
  }
}

/**
 * A dashboard answer. The CDN may serve it for 10 seconds, so a crowd of
 * visitors costs FluxDB one query set, not one per person.
 */
export function ok<T>(data: T): Response {
  return Response.json(data, {
    headers: { "cache-control": "public, max-age=0, s-maxage=10, stale-while-revalidate=20" },
  });
}

function problem(status: number, error: ApiError["error"], message: string, extra: HeadersInit = {}): Response {
  const body: ApiError = { error, message };
  return Response.json(body, { status, headers: { "cache-control": "no-store", ...extra } });
}

export function fail(error: unknown): Response {
  if (error instanceof NotConfiguredError) return problem(500, "not_configured", error.message);
  if (error instanceof InvalidRequestError) return problem(400, "invalid", error.message);
  if (error instanceof RateLimitedError) return problem(429, "rate_limited", error.message);
  if (error instanceof FluxDBError) {
    if (error.unavailable) {
      return problem(503, "waking", "FluxDB is starting up. It sleeps when nobody is using it.", { "retry-after": "3" });
    }
    if (error.status === 401 || error.status === 403) {
      return problem(502, "bad_key", `FluxDB refused the API key: ${error.message}`);
    }
    if (error.code === "not_found") return problem(502, "missing_bucket", error.message);
    if (error.code === "quota_exceeded") return problem(507, "quota", error.message);
    return problem(502, "failed", `FluxDB: ${error.message}`);
  }
  console.error(error);
  return problem(500, "failed", "Something went wrong on the server");
}
