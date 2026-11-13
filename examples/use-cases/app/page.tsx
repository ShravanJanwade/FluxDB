import Link from "next/link";
import { StatusPill } from "../components/StatusPill";
import { FLUXDB_CONSOLE_URL, FLUXDB_REPO_URL, SOURCE_URL } from "../lib/links";

const CASES = [
  {
    href: "/energy",
    sector: "Energy",
    title: "Campus energy monitor",
    summary:
      "Submeters in four buildings report demand by end use every five minutes. Facilities staff see who is drawing what, how much the sun covered and what today has cost.",
    shows: [
      "Fields per end use, a tag per building",
      "MEAN per time bucket, split by building",
      "SUM for energy and the daily bill",
    ],
  },
  {
    href: "/healthcare",
    sector: "Healthcare",
    title: "Ward vitals board",
    summary:
      "Bedside monitors chart five vital signs a minute for eight patients. The board scores each one with NEWS2 so the team sees who is deteriorating, and nurses can record observations by hand.",
    shows: [
      "Integer, float, boolean and string fields",
      "LAST() per bed for the live board",
      "COUNT with WHERE … OR … for alert minutes",
    ],
  },
  {
    href: "/markets",
    sector: "Finance",
    title: "Crypto market board",
    summary:
      "Real one-minute candles for BTC, ETH and SOL from Coinbase. FluxDB turns them into 5- and 15-minute candlesticks on request, plus highs, lows and volatility per coin.",
    shows: [
      "Live data from a public API",
      "FIRST / MAX / MIN / LAST to roll up candles",
      "STDDEV per product",
    ],
  },
];

const SNIPPET = `const FLUXDB = process.env.FLUXDB_URL;     // https://your-fluxdb
const KEY = process.env.FLUXDB_API_KEY;     // fdbk_…, server side only

// Write: line protocol, here with millisecond timestamps
await fetch(\`\${FLUXDB}/api/ingest/v1/write?bucket=energy&precision=ms\`, {
  method: "POST",
  headers: { Authorization: \`Bearer \${KEY}\` },
  body: \`power,building=lab hvac_kw=74.2,net_kw=237.7 \${Date.now()}\`,
});

// Query: SQL with a time window and bucketing
const ns = (ms) => (BigInt(ms) * 1_000_000n).toString();
const response = await fetch(\`\${FLUXDB}/api/ingest/v1/query\`, {
  method: "POST",
  headers: { Authorization: \`Bearer \${KEY}\`, "Content-Type": "application/json" },
  body: JSON.stringify({
    bucket: "energy",
    query: "SELECT MEAN(net_kw) FROM power WHERE $timeFilter GROUP BY time($interval), building",
    from: ns(Date.now() - 24 * 3600_000),
    to: ns(Date.now()),
    interval: "15m",
  }),
});
const { columns, rows } = await response.json();`;

export default function Home() {
  return (
    <>
      <section className="hero">
        <p className="eyebrow">FluxDB in practice</p>
        <h1>Three apps that keep their data in FluxDB</h1>
        <p className="lede">
          You would store users and orders in Postgres, MongoDB or Supabase. Readings that arrive every minute, and questions
          like &ldquo;average per 15 minutes, per building&rdquo;, belong in a time-series database. Each app here uses{" "}
          <a href={FLUXDB_REPO_URL}>FluxDB</a> as its only database: a Next.js backend writes to it and queries it, and the
          page draws what comes back.
        </p>
        <StatusPill />
      </section>

      <div className="cases">
        {CASES.map((item) => (
          <Link key={item.href} href={item.href} className="case-card">
            <span className="sector">{item.sector}</span>
            <h2>{item.title}</h2>
            <p>{item.summary}</p>
            <ul>
              {item.shows.map((line) => (
                <li key={line}>{line}</li>
              ))}
            </ul>
            <span className="open">Open the dashboard →</span>
          </Link>
        ))}
      </div>

      <section className="section">
        <h2>How each page works</h2>
        <p>
          There is no cron job and no queue. Every dashboard follows the same four steps, and a panel at the bottom of each page
          shows them for the request you just made: the points written, every SQL query and how long FluxDB took.
        </p>
        <div className="flow">
          <div>
            <h3>The page asks its API route</h3>
            <p>The browser calls a Next.js route handler on Vercel. The FluxDB API key lives only there.</p>
          </div>
          <div>
            <h3>The route catches up</h3>
            <p>
              It reads a watermark from FluxDB and writes every reading since the last visit: simulated meters and monitors, or
              real candles from Coinbase.
            </p>
          </div>
          <div>
            <h3>FluxDB answers SQL</h3>
            <p>
              <code>$timeFilter</code> limits the scan to the window and <code>GROUP BY time($interval)</code> buckets it, in a
              few milliseconds.
            </p>
          </div>
          <div>
            <h3>The page draws the rows</h3>
            <p>Charts, tables and scores come straight from the query results, refreshed while you watch.</p>
          </div>
        </div>
      </section>

      <section className="section">
        <h2>Use FluxDB from your own backend</h2>
        <p>Nothing here needs an SDK: FluxDB speaks HTTP. Any backend that can send a request can write and query.</p>
        <div className="two-col">
          <div className="card">
            <ol style={{ margin: 0, paddingLeft: 20, display: "grid", gap: 10, color: "var(--ink-2)" }}>
              <li>
                Sign in to the <a href={FLUXDB_CONSOLE_URL}>FluxDB console</a> and create a project.
              </li>
              <li>Add a bucket for each dataset and choose how long it keeps data (these apps keep seven days).</li>
              <li>
                Create an API key with read and write scopes. It starts with <code>fdbk_</code>.
              </li>
              <li>
                Put <code>FLUXDB_URL</code> and <code>FLUXDB_API_KEY</code> in your server&apos;s environment and call the two
                endpoints on the right.
              </li>
            </ol>
            <p className="form-note" style={{ margin: "14px 0 0" }}>
              The full source of these apps, including a typed client and the catch-up logic, is in{" "}
              <a href={SOURCE_URL}>examples/use-cases</a>.
            </p>
          </div>
          <pre className="code-block">{SNIPPET}</pre>
        </div>
      </section>
    </>
  );
}
