"use client";

import { useState } from "react";
import { CandleChart } from "../../components/charts/CandleChart";
import { LineChart, type LineSeries } from "../../components/charts/LineChart";
import { FluxPanel } from "../../components/FluxPanel";
import { ChartCard, LiveStatus, LoadState, Segmented } from "../../components/ui";
import { useLiveData } from "../../components/useLiveData";
import { clock, compact, fixed, price, signedPercent } from "../../lib/format";
import { PRODUCTS } from "../../lib/markets";
import { DAY } from "../../lib/time";
import type { MarketsResponse } from "../../lib/types";

type WindowKey = "1h" | "6h" | "24h";

const WINDOWS: { value: WindowKey; label: string }[] = [
  { value: "1h", label: "1H" },
  { value: "6h", label: "6H" },
  { value: "24h", label: "24H" },
];

const COLORS: Record<string, string> = {
  "BTC-USD": "var(--series-1)",
  "ETH-USD": "var(--series-2)",
  "SOL-USD": "var(--series-3)",
};

const CODE = `// app/api/markets/route.ts, abridged
const db = new FluxDB({ url: process.env.FLUXDB_URL, apiKey: process.env.FLUXDB_API_KEY });

// Store the raw one-minute candles from Coinbase as they close
await db.write("markets", candles.map((c) => ({
  measurement: "candles",
  tags: { product: "BTC-USD" },
  fields: { open: c.open, high: c.high, low: c.low, close: c.close, volume: c.volume },
  time: c.time,
})));

// FluxDB builds 5-minute candles from them at query time: no pre-aggregation
const ohlc = await db.query(
  "markets",
  "SELECT FIRST(open), MAX(high), MIN(low), LAST(close), SUM(volume) FROM candles " +
    "WHERE $timeFilter AND product = 'BTC-USD' GROUP BY time($interval)",
  { from: Date.now() - 6 * HOUR, to: Date.now(), interval: "5m" },
);`;

function Change({ from, to }: { from: number | null; to: number | null }) {
  if (from === null || to === null || from === 0) return <span>–</span>;
  const change = ((to - from) / from) * 100;
  const up = change >= 0;
  return (
    <span className={up ? "delta-up" : "delta-down"}>
      {up ? "▲" : "▼"} {signedPercent(change)}
    </span>
  );
}

export function MarketsDashboard() {
  const [windowKey, setWindowKey] = useState<WindowKey>("6h");
  const [product, setProduct] = useState("BTC-USD");
  const live = useLiveData<MarketsResponse>(`/api/markets?window=${windowKey}&product=${product}`, 30_000);
  const data = live.data;
  const domain: [number, number] = data ? [data.window.from, data.window.to] : [0, 1];
  const current = data?.overview.find((o) => o.id === data.product);
  const meta = PRODUCTS.find((p) => p.id === (data?.product ?? product))!;
  const withDay = data ? data.window.to - data.window.from > DAY * 0.99 : false;
  const indexed: LineSeries[] =
    data?.performance.map((p) => ({
      id: p.id,
      label: p.id,
      short: p.id.replace("-USD", ""),
      color: COLORS[p.id],
      points: p.series,
    })) ?? [];

  return (
    <>
      <div className="dash-head">
        <div>
          <p className="eyebrow">Finance · market data</p>
          <h1>Crypto market board</h1>
          <p>
            Real one-minute candles for Bitcoin, Ether and Solana from Coinbase&apos;s public market-data API, stored in FluxDB
            as they close. Every candle, high, low and volatility figure on this page is computed by FluxDB at query time from
            those raw bars. Not financial advice.
          </p>
        </div>
      </div>

      <div className="filters">
        <Segmented
          label="Product"
          options={PRODUCTS.map((p) => ({ value: p.id, label: p.base }))}
          value={product}
          onChange={setProduct}
        />
        <Segmented label="Time range" options={WINDOWS} value={windowKey} onChange={setWindowKey} />
        <LiveStatus live={live} />
      </div>
      <LoadState live={live} />
      {data?.feed.error && (
        <div className="banner" role="status">
          <span className="status-dot bad icon" />
          <div>
            <strong>The market feed did not answer this time.</strong>
            <p>Showing the candles FluxDB already holds. {data.feed.error}</p>
          </div>
        </div>
      )}

      {data && current && (
        <>
          <section className="card" aria-label={`${meta.name} price`}>
            <div className="quote">
              <div>
                <div className="form-note">
                  {meta.name} · {meta.id}
                </div>
                <div className="price">{current.last === null ? "–" : price(current.last)}</div>
              </div>
              <div className="meta">
                <span>
                  Change ({data.window.key.toUpperCase()})
                  <b>
                    <Change from={current.open} to={current.last} />
                  </b>
                </span>
                <span>
                  High<b>{current.high === null ? "–" : price(current.high)}</b>
                </span>
                <span>
                  Low<b>{current.low === null ? "–" : price(current.low)}</b>
                </span>
                <span>
                  Volume<b>{current.volume === null ? "–" : `${compact(current.volume)} ${meta.base}`}</b>
                </span>
              </div>
            </div>
            <CandleChart
              candles={data.candles}
              domain={domain}
              bucketMs={data.window.intervalMs}
              base={meta.base}
              label={`${meta.name} ${data.window.interval} candlestick chart with volume`}
              refreshing={live.refreshing}
            />
            <p className="form-note" style={{ margin: "8px 0 0" }}>
              {data.window.interval} candles rolled up by FluxDB from one-minute bars. Hollow: closed higher than it opened;
              filled: closed lower. Times are your local time.
            </p>
          </section>

          <div className="grid" style={{ marginTop: 16 }}>
            <section className="card" aria-label="Market overview">
              <div className="card-head">
                <div>
                  <h2>Market overview</h2>
                  <p>One GROUP BY product query over the range, STDDEV included</p>
                </div>
              </div>
              <div className="table-wrap">
                <table className="data">
                  <thead>
                    <tr>
                      <th>Product</th>
                      <th className="num">Last</th>
                      <th className="num">Change</th>
                      <th className="num">High</th>
                      <th className="num">Low</th>
                      <th className="num">σ of close</th>
                    </tr>
                  </thead>
                  <tbody>
                    {data.overview.map((o) => (
                      <tr key={o.id}>
                        <td>
                          <span className="series-key" style={{ background: COLORS[o.id] }} />
                          {o.base}
                        </td>
                        <td className="num">{o.last === null ? "–" : price(o.last)}</td>
                        <td className="num">
                          <Change from={o.open} to={o.last} />
                        </td>
                        <td className="num">{o.high === null ? "–" : price(o.high)}</td>
                        <td className="num">{o.low === null ? "–" : price(o.low)}</td>
                        <td className="num">
                          {o.stddev === null || o.last === null ? "–" : `${fixed((o.stddev / o.last) * 100, 2)}%`}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <p className="form-note" style={{ margin: "10px 0 0" }}>
                σ is the standard deviation of one-minute closes in the range, as a share of the last price.
              </p>
            </section>
            <ChartCard
              title="Performance, indexed"
              subtitle="Close as a share of the first close in range (= 100)"
              legend={indexed.map((s) => ({ label: s.label, color: s.color }))}
              table={{
                columns: ["Time", ...indexed.map((s) => s.label)],
                rows: (indexed[0]?.points ?? []).map((point) => [
                  clock(point.t, undefined, withDay),
                  ...indexed.map((s) => {
                    const v = s.points.find((p) => p.t === point.t)?.v;
                    return v == null ? "–" : fixed(v, 2);
                  }),
                ]),
              }}
            >
              <LineChart
                series={indexed}
                domain={domain}
                stepMs={data.window.intervalMs}
                format={(v) => fixed(v, 2)}
                axisFormat={(v) => fixed(v, 1)}
                rule={100}
                endLabels
                height={240}
                refreshing={live.refreshing}
                label="Indexed closing prices for Bitcoin, Ether and Solana"
              />
            </ChartCard>
          </div>
        </>
      )}

      <FluxPanel
        activity={data?.flux ?? null}
        code={CODE}
        intro={
          <p style={{ margin: 0 }}>
            On each refresh the route handler asks Coinbase only for the candles that closed since the watermark (Kraken is the
            fallback feed), writes them, then queries. FluxDB stores one-minute bars; <code>FIRST(open)</code>,{" "}
            <code>MAX(high)</code>, <code>MIN(low)</code>, <code>LAST(close)</code> and <code>SUM(volume)</code> with{" "}
            <code>GROUP BY time($interval)</code> turn them into 5- or 15-minute candles on the fly.
          </p>
        }
      />
    </>
  );
}
