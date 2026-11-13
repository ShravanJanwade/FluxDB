/**
 * Catch-up ingestion. Nothing runs on a timer: when someone opens a dashboard,
 * its route handler fills in every reading since the last one stored, then
 * queries. A `sync_state` measurement in each bucket records how far each
 * stream has been written (its watermark).
 *
 * The simulated streams are pure functions of time, and the market stream
 * re-fetches its last minute, so two visitors filling the same minutes write
 * identical points and FluxDB's upsert keeps one copy.
 */
import type { FluxDB, QueryResult } from "./fluxdb";
import { num, str } from "./fluxdb";
import { toLine, type Point } from "./lineProtocol";
import { alignDown } from "./time";

export const WATERMARK_MEASUREMENT = "sync_state";

export interface Produced {
  points: Point[];
  /** The newest time that is final and need not be produced again. */
  watermark: number;
  /** Where the points came from, for display. */
  source: string;
}

export interface StreamSpec {
  stream: string;
  /** Distance between readings. */
  stepMs: number;
  /** How far back to fill after a quiet spell. */
  maxBackfillMs: number;
  /** Produce the readings in [from, to], both aligned to `stepMs`. */
  produce(from: number, to: number): Promise<Produced>;
}

export interface SyncReport {
  stream: string;
  /** Readings written by this request; 0 when the stream was up to date. */
  written: number;
  range: { from: number; to: number } | null;
  watermark: number | null;
  source: string | null;
  requests: number;
  totalMs: number;
  sample: string[];
  /** Set when the stream's source failed; stored data is still served. */
  error?: string;
}

/** Every stream's watermark in a bucket, in one query. */
export async function readWatermarks(
  db: FluxDB,
  bucket: string,
  now: number,
  lookbackMs: number,
): Promise<{ marks: Map<string, number>; query: QueryResult }> {
  const query = await db.query(
    bucket,
    `SELECT MAX(until_ms) FROM ${WATERMARK_MEASUREMENT} WHERE $timeFilter GROUP BY stream`,
    { from: now - lookbackMs, to: now + 60_000 },
    "Read the sync watermark",
  );
  const marks = new Map<string, number>();
  for (const row of query.rows) {
    const until = num(row.max_until_ms);
    if (until !== null) marks.set(str(row.stream), until);
  }
  return { marks, query };
}

/** The range a stream still needs, or null when it is current. */
export function pendingRange(
  watermark: number | undefined,
  now: number,
  stepMs: number,
  maxBackfillMs: number,
): { from: number; to: number } | null {
  const to = alignDown(now, stepMs);
  const earliest = alignDown(now - maxBackfillMs, stepMs);
  const from = watermark === undefined ? earliest : Math.max(earliest, watermark + stepMs);
  return from > to ? null : { from, to };
}

/** Bring several streams of one bucket up to date with a single write. */
export async function catchUp(
  db: FluxDB,
  bucket: string,
  streams: StreamSpec[],
  marks: Map<string, number>,
  now: number,
): Promise<SyncReport[]> {
  const reports: SyncReport[] = [];
  const points: Point[] = [];
  const watermarks: Point[] = [];
  await Promise.all(
    streams.map(async (spec) => {
      const range = pendingRange(marks.get(spec.stream), now, spec.stepMs, spec.maxBackfillMs);
      const report: SyncReport = {
        stream: spec.stream,
        written: 0,
        range,
        watermark: marks.get(spec.stream) ?? null,
        source: null,
        requests: 0,
        totalMs: 0,
        sample: [],
      };
      reports.push(report);
      if (!range) return;
      try {
        const produced = await spec.produce(range.from, range.to);
        report.source = produced.source;
        if (produced.points.length === 0) return;
        points.push(...produced.points);
        report.written = produced.points.length;
        report.sample = produced.points.slice(-2).map(toLine);
        report.watermark = produced.watermark;
        watermarks.push({
          measurement: WATERMARK_MEASUREMENT,
          tags: { stream: spec.stream },
          fields: { until_ms: produced.watermark },
          time: produced.watermark,
        });
      } catch (error) {
        report.error = error instanceof Error ? error.message : String(error);
      }
    }),
  );
  if (points.length > 0) {
    // Watermarks go last, so a write that fails part-way leaves them behind
    // and the next visit retries the same range.
    const result = await db.write(bucket, [...points, ...watermarks]);
    for (const report of reports) {
      if (report.written === 0) continue;
      report.requests = result.requests;
      report.totalMs = result.totalMs;
    }
  }
  reports.sort((a, b) => (a.stream < b.stream ? -1 : 1));
  return reports;
}
