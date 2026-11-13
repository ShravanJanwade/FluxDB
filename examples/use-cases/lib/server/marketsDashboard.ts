import { nsToMs, num, str, type FluxDB, type QueryWindow } from "../fluxdb";
import { CANDLE_STEP, fetchCandles, PRODUCTS, type Product } from "../markets";
import { catchUp, readWatermarks } from "../sync";
import { DAY, intervalMs, type WindowSpec } from "../time";
import type { MarketsResponse, SeriesPoint } from "../types";
import { Recorder } from "./context";

export async function marketsDashboard(
  db: FluxDB,
  bucket: string,
  spec: WindowSpec,
  product: Product,
  now = Date.now(),
): Promise<MarketsResponse> {
  const recorder = new Recorder(bucket);

  // 1. Fetch the candles closed since the last visit (up to a day) for every product.
  const { marks, query } = await readWatermarks(db, bucket, now, DAY);
  recorder.query(query);
  const reports = recorder.synced(
    await catchUp(
      db,
      bucket,
      PRODUCTS.map((p) => ({
        stream: `candles:${p.id}`,
        stepMs: CANDLE_STEP,
        maxBackfillMs: DAY,
        produce: (from: number, to: number) => fetchCandles(p, from, to, now),
      })),
      marks,
      now,
    ),
  );

  // 2. Let FluxDB roll one-minute bars up into the candles the chart needs.
  const window: QueryWindow = { from: now - spec.ms, to: now, interval: spec.interval };
  const [ohlc, overview, closing] = await Promise.all([
    db.query(
      bucket,
      `SELECT FIRST(open), MAX(high), MIN(low), LAST(close), SUM(volume) FROM candles WHERE $timeFilter AND product = '${product.id}' GROUP BY time($interval)`,
      window,
      `${product.id} candles`,
    ),
    db.query(
      bucket,
      "SELECT FIRST(open), LAST(close), MAX(high), MIN(low), SUM(volume), STDDEV(close) FROM candles WHERE $timeFilter GROUP BY product",
      window,
      "Market overview",
    ),
    db.query(bucket, "SELECT LAST(close) FROM candles WHERE $timeFilter GROUP BY time($interval), product", window, "Closing prices"),
  ]);
  for (const result of [ohlc, overview, closing]) recorder.query(result);

  const byProduct = new Map(overview.rows.map((row) => [str(row.product), row]));
  const failed = reports.find((report) => report.error);
  return {
    window: { key: spec.key, from: window.from, to: window.to, interval: spec.interval, intervalMs: intervalMs(spec.interval) },
    product: product.id,
    candles: ohlc.rows.flatMap((row) => {
      const open = num(row.first_open);
      const high = num(row.max_high);
      const low = num(row.min_low);
      const close = num(row.last_close);
      if (open === null || high === null || low === null || close === null) return [];
      return [{ t: nsToMs(row.time), open, high, low, close, volume: num(row.sum_volume) ?? 0 }];
    }),
    overview: PRODUCTS.map((p) => {
      const row = byProduct.get(p.id);
      return {
        id: p.id,
        name: p.name,
        base: p.base,
        open: num(row?.first_open),
        last: num(row?.last_close),
        high: num(row?.max_high),
        low: num(row?.min_low),
        volume: num(row?.sum_volume),
        stddev: num(row?.stddev_close),
      };
    }),
    performance: PRODUCTS.map((p) => {
      const closes = closing.rows
        .filter((row) => str(row.product) === p.id)
        .map((row) => ({ t: nsToMs(row.time), close: num(row.last_close) }))
        .sort((a, b) => a.t - b.t);
      const base = closes.find((point) => point.close !== null)?.close ?? null;
      const series: SeriesPoint[] = closes.map((point) => ({
        t: point.t,
        v: base && point.close !== null ? (point.close / base) * 100 : null,
      }));
      return { id: p.id, series };
    }),
    feed: {
      source: reports.find((report) => report.source)?.source ?? null,
      error: failed?.error ?? null,
    },
    flux: recorder.activity(),
  };
}
