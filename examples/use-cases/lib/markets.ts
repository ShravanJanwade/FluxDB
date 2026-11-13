/**
 * Crypto market data: real one-minute candles (open, high, low, close,
 * volume) from Coinbase Exchange's public API, with Kraken as a fallback.
 * FluxDB stores the raw one-minute bars; the dashboard asks it for 5- and
 * 15-minute candles with `GROUP BY time($interval)`, so nothing is
 * pre-aggregated.
 */
import type { Point } from "./lineProtocol";
import type { Produced } from "./sync";
import { alignDown, MINUTE } from "./time";

export const CANDLE_STEP = MINUTE;

export interface Product {
  id: string;
  name: string;
  base: string;
  kraken: string;
}

export const PRODUCTS: Product[] = [
  { id: "BTC-USD", name: "Bitcoin", base: "BTC", kraken: "XBTUSD" },
  { id: "ETH-USD", name: "Ether", base: "ETH", kraken: "ETHUSD" },
  { id: "SOL-USD", name: "Solana", base: "SOL", kraken: "SOLUSD" },
];

export function findProduct(id: string | null): Product | undefined {
  return PRODUCTS.find((product) => product.id === id);
}

export interface Candle {
  time: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

const USER_AGENT = "fluxdb-use-cases (https://github.com/ShravanJanwade/FluxDB)";

async function getJson(url: string): Promise<unknown> {
  const response = await fetch(url, {
    headers: { accept: "application/json", "user-agent": USER_AGENT },
    signal: AbortSignal.timeout(8_000),
    cache: "no-store",
  });
  if (!response.ok) throw new Error(`HTTP ${response.status} from ${new URL(url).host}`);
  return response.json();
}

const iso = (t: number) => new Date(t).toISOString().replace(".000Z", "Z");

/** Coinbase answers at most 300 candles a request: rows of [time, low, high, open, close, volume], newest first. */
async function coinbaseCandles(product: Product, from: number, to: number): Promise<Candle[]> {
  const candles: Candle[] = [];
  for (let start = from; start <= to; start += 300 * MINUTE) {
    const end = Math.min(to, start + 299 * MINUTE);
    const query = new URLSearchParams({ granularity: "60", start: iso(start), end: iso(end) });
    const rows = (await getJson(`https://api.exchange.coinbase.com/products/${product.id}/candles?${query}`)) as number[][];
    if (!Array.isArray(rows)) throw new Error("Unexpected answer from Coinbase");
    for (const [time, low, high, open, close, volume] of rows) {
      candles.push({ time: time * 1000, open, high, low, close, volume });
    }
  }
  return candles;
}

/** Kraken returns the most recent 720 one-minute candles after `since`, as strings. */
async function krakenCandles(product: Product, from: number, to: number): Promise<Candle[]> {
  const query = new URLSearchParams({ pair: product.kraken, interval: "1", since: String(Math.floor((from - MINUTE) / 1000)) });
  const body = (await getJson(`https://api.kraken.com/0/public/OHLC?${query}`)) as {
    error?: string[];
    result?: Record<string, unknown>;
  };
  if (body.error?.length) throw new Error(`Kraken: ${body.error.join(", ")}`);
  const series = Object.entries(body.result ?? {}).find(([key]) => key !== "last")?.[1];
  if (!Array.isArray(series)) throw new Error("Unexpected answer from Kraken");
  return (series as (string | number)[][]).map(([time, open, high, low, close, , volume]) => ({
    time: Number(time) * 1000,
    open: Number(open),
    high: Number(high),
    low: Number(low),
    close: Number(close),
    volume: Number(volume),
  }));
}

export function candlePoint(product: Product, candle: Candle): Point {
  return {
    measurement: "candles",
    tags: { product: product.id },
    fields: { open: candle.open, high: candle.high, low: candle.low, close: candle.close, volume: candle.volume },
    time: candle.time,
  };
}

/**
 * Candles for [from, to] from the first feed that answers. The minute in
 * progress is written too but left after the watermark, so the next visit
 * fetches it again once it has closed.
 */
export async function fetchCandles(product: Product, from: number, to: number, now: number): Promise<Produced> {
  const failures: string[] = [];
  for (const [source, fetcher] of [
    ["Coinbase Exchange", coinbaseCandles],
    ["Kraken", krakenCandles],
  ] as const) {
    try {
      const byTime = new Map<number, Candle>();
      for (const candle of await fetcher(product, from, to)) {
        const valid = [candle.open, candle.high, candle.low, candle.close, candle.volume].every(Number.isFinite);
        if (valid && candle.time >= from && candle.time <= to) byTime.set(candle.time, candle);
      }
      const candles = [...byTime.values()].sort((a, b) => a.time - b.time);
      const newest = candles.at(-1)?.time ?? from - CANDLE_STEP;
      return {
        points: candles.map((candle) => candlePoint(product, candle)),
        watermark: Math.min(newest, alignDown(now, CANDLE_STEP) - CANDLE_STEP),
        source,
      };
    } catch (error) {
      failures.push(`${source}: ${error instanceof Error ? error.message : String(error)}`);
    }
  }
  throw new Error(`No market feed answered (${failures.join("; ")})`);
}
