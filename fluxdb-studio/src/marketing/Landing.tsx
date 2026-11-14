/**
 * The public front page.
 *
 * One idea per section, in the order a visitor needs them: what FluxDB is,
 * the product doing its job, where it is used, what is underneath, how to
 * start, and what it is not. Every number on this page is measured from the
 * running instance or from a benchmark in the repository, and the limits are
 * stated as plainly as the features.
 */

import { useEffect, useState, type ReactNode } from "react";
import { Link } from "react-router-dom";
import {
  ArrowRight,
  ArrowUpRight,
  Bell,
  Building2,
  CandlestickChart,
  Check,
  Copy,
  Gauge,
  Github,
  HeartPulse,
  KeyRound,
  LayoutDashboard,
  Play,
  ScrollText,
  Users,
} from "lucide-react";
import { Logo } from "../components/Logo";
import { ThemeToggle } from "../components/ThemeToggle";
import { api } from "../lib/api";
import { count, decimal, uptime } from "../lib/format";
import type { PublicStats } from "../lib/types";
import "../styles/marketing.css";

const REPOSITORY = "https://github.com/ShravanJanwade/FluxDB";
const USE_CASES_URL = "https://fluxdb-use-cases.vercel.app";

export default function Landing() {
  const [stats, setStats] = useState<PublicStats | null>(null);

  useEffect(() => {
    // The hero quotes this instance's own figures. If the call fails they
    // are left out rather than replaced with placeholders.
    api
      .publicStats()
      .then(setStats)
      .catch(() => setStats(null));
    document.title = "FluxDB: a time-series database you can read end to end";
  }, []);

  return (
    <div className="marketing">
      <Nav />
      <main>
        <Hero stats={stats} />
        <UseCases />
        <Engine />
        <Console />
        <Quickstart />
        <Limits />
        <Closing />
      </main>
      <Footer version={stats?.version} />
    </div>
  );
}

function Nav() {
  const [scrolled, setScrolled] = useState(false);
  useEffect(() => {
    const onScroll = () => setScrolled(window.scrollY > 8);
    onScroll();
    window.addEventListener("scroll", onScroll, { passive: true });
    return () => window.removeEventListener("scroll", onScroll);
  }, []);

  return (
    <header className={`m-nav${scrolled ? " is-scrolled" : ""}`}>
      <div className="m-shell m-nav-inner">
        <Link to="/" className="m-nav-brand" aria-label="FluxDB home">
          <Logo size={26} />
        </Link>
        <nav className="m-nav-links" aria-label="Sections">
          <a href="#use-cases">Use cases</a>
          <a href="#engine">Engine</a>
          <a href="#quickstart">Quickstart</a>
          <Link to="/docs">Docs</Link>
        </nav>
        <div className="m-nav-actions">
          <ThemeToggle compact />
          <a
            className="btn btn-ghost btn-sm btn-icon m-nav-github"
            href={REPOSITORY}
            target="_blank"
            rel="noreferrer noopener"
            aria-label="Source on GitHub"
          >
            <Github size={16} aria-hidden />
          </a>
          <Link className="btn btn-ghost btn-sm m-nav-signin" to="/login">
            Sign in
          </Link>
          <Link className="btn btn-primary btn-sm" to="/signup">
            Start free
          </Link>
        </div>
      </div>
    </header>
  );
}

function Hero({ stats }: { stats: PublicStats | null }) {
  return (
    <section className="m-hero">
      <div className="m-shell m-hero-copy">
        <a
          className="m-chip"
          href={REPOSITORY}
          target="_blank"
          rel="noreferrer noopener"
        >
          <span className="m-chip-dot" aria-hidden />
          <span>
            Open source, written in Rust
            <span className="m-chip-more"> · one binary, one port</span>
          </span>
          <ArrowRight size={13} aria-hidden />
        </a>
        <h1>
          Store metrics. Query them in SQL.
          <span>With the console built in.</span>
        </h1>
        <p className="m-lede">
          FluxDB is a time-series database in Rust: a write-ahead log, a
          skip-list memtable and compressed SSTables underneath, and workspaces,
          API keys, dashboards and alerts on top.
        </p>
        <div className="m-cta">
          <Link className="btn btn-primary btn-lg" to="/login?demo=1">
            <Play size={16} aria-hidden /> Open the live demo
          </Link>
          <Link className="btn btn-lg" to="/signup">
            Create a free workspace
          </Link>
        </div>
        <p className="m-note">
          One click, no email. A seeded fleet with an incident already in it.
        </p>
        <LiveStats stats={stats} />
      </div>
      <div className="m-shell m-shot-wrap">
        <ConsoleShot />
      </div>
    </section>
  );
}

function LiveStats({ stats }: { stats: PublicStats | null }) {
  if (!stats) return null;
  const items = [
    `${count(stats.demo_points)} points in the demo`,
    stats.query_p95_ms !== null
      ? `${decimal(stats.query_p95_ms)} ms API p95`
      : null,
    `up ${uptime(stats.uptime_seconds)}`,
    `engine v${stats.version}`,
  ].filter(Boolean);
  return (
    <p
      className="m-live"
      title="Measured from this deployment just now. Server processing time only."
    >
      <span className="m-live-dot" aria-hidden />
      <span className="m-live-label">Live from this instance</span>
      {items.map((item) => (
        <span key={item}>{item}</span>
      ))}
    </p>
  );
}

/** The query explorer, in miniature: one SQL query finding the incident in the demo fleet. */
function ConsoleShot() {
  const payments = [
    38, 41, 37, 44, 40, 43, 39, 42, 45, 41, 38, 43, 40, 44, 58, 96, 148, 186,
    172, 150, 128, 104, 82, 64, 52, 46, 42, 45, 40, 43, 39, 41,
  ];
  const checkout = payments.map(
    (_, i) => 30 + ((i * 7) % 9) + (i > 15 ? 6 : 0),
  );
  const search = payments.map((_, i) => 18 + ((i * 5) % 7));
  const width = 600;
  const height = 220;
  const max = 210;
  const x = (i: number) => (i * width) / (payments.length - 1);
  const y = (v: number) => height - (v / max) * height;
  const line = (values: number[]) =>
    values.map((v, i) => `${x(i).toFixed(1)},${y(v).toFixed(1)}`).join(" ");
  const peak = payments.indexOf(Math.max(...payments));

  return (
    <figure className="m-shot" aria-label="The FluxDB query explorer">
      <div className="m-shot-bar" aria-hidden>
        <span className="m-dots">
          <i />
          <i />
          <i />
        </span>
        <span>Production fleet / Explorer</span>
      </div>
      <div className="m-shot-body">
        <div className="m-query">
          <span className="m-shot-label">Query</span>
          <pre className="m-sql">
            <code>
              <b>SELECT</b> MAX(latency_p99){"\n"}
              <b>FROM</b> http_requests{"\n"}
              <b>WHERE</b> $timeFilter{"\n"}
              <b>GROUP BY</b> time(<em>&apos;1m&apos;</em>), service
            </code>
          </pre>
          <div className="m-query-run">
            <span className="m-fake-btn">
              <Play size={12} aria-hidden /> Run
            </span>
            <span>9 ms · 1,440 rows</span>
          </div>
          <div className="m-query-result">
            <div>
              <span>service</span>
              <span>max p99</span>
            </div>
            <div className="is-hot">
              <span>payments</span>
              <span>186 ms</span>
            </div>
            <div>
              <span>checkout</span>
              <span>44 ms</span>
            </div>
            <div>
              <span>search</span>
              <span>24 ms</span>
            </div>
          </div>
        </div>
        <div className="m-chart">
          <div className="m-chart-head">
            <div>
              <strong>p99 latency by service</strong>
              <span>Last hour · 1 minute buckets</span>
            </div>
            <span className="badge badge-danger">Alerting</span>
          </div>
          <svg
            viewBox={`0 0 ${width} ${height}`}
            preserveAspectRatio="none"
            aria-hidden
          >
            {[0.25, 0.5, 0.75].map((f) => (
              <line
                key={f}
                x1="0"
                x2={width}
                y1={height * f}
                y2={height * f}
                className="m-grid"
              />
            ))}
            <line
              x1="0"
              x2={width}
              y1={y(120)}
              y2={y(120)}
              className="m-threshold"
            />
            <polyline points={line(search)} className="m-series m-series-c" />
            <polyline points={line(checkout)} className="m-series m-series-b" />
            <polyline points={line(payments)} className="m-series m-series-a" />
            <circle
              cx={x(peak)}
              cy={y(payments[peak])}
              r="4.5"
              className="m-peak"
            />
          </svg>
          <div className="m-chart-legend">
            <span>
              <i className="m-series-a" /> payments
            </span>
            <span>
              <i className="m-series-b" /> checkout
            </span>
            <span>
              <i className="m-series-c" /> search
            </span>
            <span className="m-chart-threshold">threshold 120 ms</span>
          </div>
        </div>
      </div>
    </figure>
  );
}

function UseCases() {
  const cases = [
    {
      Icon: Building2,
      sector: "Energy",
      title: "Campus energy monitor",
      path: "/energy",
      body: "Submeters in four buildings write a reading every five minutes; demand, solar output and the day's bill are queries.",
      query: "MEAN(kw) GROUP BY time($interval), building",
    },
    {
      Icon: HeartPulse,
      sector: "Healthcare",
      title: "Ward vitals board",
      path: "/healthcare",
      body: "Bedside monitors write five vital signs a minute; the board scores every bed and alerts on the outliers.",
      query: "LAST(hr), LAST(spo2) GROUP BY bed",
    },
    {
      Icon: CandlestickChart,
      sector: "Finance",
      title: "Crypto market board",
      path: "/markets",
      body: "Real one-minute candles from Coinbase, rolled up into 5- and 15-minute candlesticks when the chart asks.",
      query: "FIRST, MAX, MIN, LAST GROUP BY time('15m')",
    },
  ];
  return (
    <section className="m-section" id="use-cases">
      <div className="m-shell">
        <SectionHead
          kicker="Use cases"
          title="Already the database behind three apps."
          lede="Each is a Next.js app on Vercel that writes readings and runs SQL over HTTPS with a project API key, the way you would use Postgres."
        />
        <div className="m-cards">
          {cases.map((item) => (
            <article key={item.title} className="m-card">
              <span className="m-card-sector">
                <item.Icon size={15} aria-hidden /> {item.sector}
              </span>
              <h3>{item.title}</h3>
              <p>{item.body}</p>
              <code className="m-card-query">{item.query}</code>
              <a
                className="btn btn-sm"
                href={`${USE_CASES_URL}${item.path}`}
                target="_blank"
                rel="noreferrer noopener"
              >
                Open the app <ArrowUpRight size={14} aria-hidden />
              </a>
            </article>
          ))}
        </div>
      </div>
    </section>
  );
}

function Engine() {
  const path = [
    { name: "Write", detail: "JSON or line protocol" },
    { name: "WAL", detail: "Checksummed, synced" },
    { name: "Memtable", detail: "Sorted skip list" },
    { name: "SSTables", detail: "Typed, LZ4, bloom filters" },
    { name: "Query", detail: "SQL with time buckets" },
  ];
  const features = [
    {
      title: "Durable writes",
      body: "Every point is in the log before it is acknowledged, and a regression suite kills the process mid-write and checks what survives.",
    },
    {
      title: "Compact storage",
      body: "Immutable files encoded per field type and block-compressed, merged by background compaction.",
    },
    {
      title: "Typed points",
      body: "Float, 64-bit integer, string and boolean fields with nanosecond timestamps. Exact integers never round-trip through a JSON double.",
    },
    {
      title: "SQL over time",
      body: "WHERE, IN, BETWEEN, LIKE, aggregates and GROUP BY time('5m') with tags. Anything unsupported says so by name.",
    },
  ];
  return (
    <section className="m-section m-section-alt" id="engine">
      <div className="m-shell">
        <SectionHead
          kicker="The engine"
          title="Small enough to read end to end."
          lede="An LSM storage engine in Rust: each layer is a few hundred lines you can follow, and each is tested by breaking it."
        />
        <ol className="m-path" aria-label="Write path">
          {path.map((step, index) => (
            <li key={step.name}>
              <span className="m-path-index">
                {String(index + 1).padStart(2, "0")}
              </span>
              <strong>{step.name}</strong>
              <span>{step.detail}</span>
            </li>
          ))}
        </ol>
        <div className="m-features">
          {features.map((feature) => (
            <div key={feature.title}>
              <h3>{feature.title}</h3>
              <p>{feature.body}</p>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}

function Console() {
  const items = [
    {
      Icon: Users,
      title: "Workspaces and roles",
      body: "Accounts, projects and buckets, with owner, editor and viewer roles.",
    },
    {
      Icon: KeyRound,
      title: "Project API keys",
      body: "Scoped read and write tokens, stored as a hash and revocable at once.",
    },
    {
      Icon: LayoutDashboard,
      title: "Dashboards",
      body: "Saved panels over live queries, with time ranges and variables.",
    },
    {
      Icon: Bell,
      title: "Monitors and alerts",
      body: "Threshold checks every minute, recording each change of state.",
    },
    {
      Icon: ScrollText,
      title: "Audit trail",
      body: "Every change to a project, by whom and when.",
    },
    {
      Icon: Gauge,
      title: "Observable by default",
      body: "Request latency, point and file counts, Prometheus metrics, OpenAPI.",
    },
  ];
  return (
    <section className="m-section" id="console">
      <div className="m-shell">
        <SectionHead
          kicker="The console"
          title="Everything around the engine."
          lede="An engine on its own is a library. The console makes it something a team can use, from the same process on the same port."
        />
        <div className="m-grid">
          {items.map(({ Icon, title, body }) => (
            <div key={title} className="m-grid-item">
              <Icon size={17} aria-hidden />
              <div>
                <h3>{title}</h3>
                <p>{body}</p>
              </div>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}

const SNIPPETS: { id: string; label: string; code: string }[] = [
  {
    id: "curl",
    label: "curl",
    code: `# Write a point with a project API key from the console.
curl -X POST 'https://YOUR-DEPLOYMENT/api/ingest/v1/points?bucket=production' \\
  -H 'Authorization: Bearer fdbk_YOUR_KEY' \\
  -H 'Content-Type: application/json' \\
  -d '{"points":[{"measurement":"cpu",
        "tags":{"host":"api-01"},
        "timestamp":"1789142400000000000",
        "fields":{"usage":42.8}}]}'

# Then query it.
curl -X POST 'https://YOUR-DEPLOYMENT/api/ingest/v1/query' \\
  -H 'Authorization: Bearer fdbk_YOUR_KEY' \\
  -d '{"bucket":"production",
       "query":"SELECT MEAN(usage) FROM cpu WHERE $timeFilter GROUP BY time($interval), host"}'`,
  },
  {
    id: "line",
    label: "Line protocol",
    code: `# The format your metrics agent already speaks.
curl -X POST 'https://YOUR-DEPLOYMENT/api/ingest/v1/write?bucket=production&precision=s' \\
  -H 'Authorization: Bearer fdbk_YOUR_KEY' \\
  --data-binary '
cpu,host=api-01 usage=42.8,cores=8i 1789142400
http_requests,service=checkout requests=2412i,latency_p99=184.2 1789142400'`,
  },
  {
    id: "local",
    label: "Run locally",
    code: `git clone https://github.com/ShravanJanwade/FluxDB.git
cd FluxDB
node start-all.js

# Then open http://127.0.0.1:5173 and click "Explore the demo".`,
  },
];

function Quickstart() {
  const [active, setActive] = useState(SNIPPETS[0].id);
  const [copied, setCopied] = useState(false);
  const snippet = SNIPPETS.find((s) => s.id === active) ?? SNIPPETS[0];

  const copy = async () => {
    try {
      await navigator.clipboard.writeText(snippet.code);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1600);
    } catch {
      // Clipboard access can be refused; the code stays selectable.
    }
  };

  return (
    <section className="m-section m-section-alt" id="quickstart">
      <div className="m-shell m-split">
        <SectionHead
          kicker="Quickstart"
          title="Your first point in one request."
          lede="Create a project API key in the console, then write and query over HTTPS. Timestamps are nanosecond strings; that is the only surprise in the API."
        >
          <p className="m-split-links">
            <Link to="/docs">
              Read the docs <ArrowRight size={14} aria-hidden />
            </Link>
            <a href="/api/v1/openapi.json">OpenAPI document</a>
          </p>
        </SectionHead>
        <div className="m-code">
          <div className="m-code-tabs" role="tablist" aria-label="Quickstart">
            {SNIPPETS.map((entry) => (
              <button
                key={entry.id}
                type="button"
                role="tab"
                aria-selected={entry.id === active}
                className={entry.id === active ? "is-active" : undefined}
                onClick={() => setActive(entry.id)}
              >
                {entry.label}
              </button>
            ))}
            <button type="button" className="m-code-copy" onClick={copy}>
              {copied ? (
                <Check size={14} aria-hidden />
              ) : (
                <Copy size={14} aria-hidden />
              )}
              {copied ? "Copied" : "Copy"}
            </button>
          </div>
          <pre>
            <code>
              {snippet.code.split("\n").map((line, index) => (
                <span
                  key={index}
                  className={line.startsWith("#") ? "is-comment" : undefined}
                >
                  {line}
                  {"\n"}
                </span>
              ))}
            </code>
          </pre>
        </div>
      </div>
    </section>
  );
}

function Limits() {
  const limits = [
    [
      "Single node",
      "No clustering, replication or failover. One process owns a data directory.",
    ],
    [
      "Memory-bound reads",
      "Queries materialise SSTables in memory, so very large or high-cardinality data needs more work.",
    ],
    [
      "Not InfluxDB",
      "It accepts line protocol for writes. There is no Flux, InfluxQL parity or continuous queries.",
    ],
    [
      "Demo sleeps",
      "The hosted instance scales to zero after 30 idle minutes; the first visit after that takes a few seconds.",
    ],
  ];
  return (
    <section className="m-section" id="limits">
      <div className="m-shell">
        <SectionHead
          kicker="Honest limits"
          title="What FluxDB is not."
          lede="A single-node database built to be understood. Test capacity, backups and fault behaviour for your own workload before trusting it."
        />
        <div className="m-limits">
          {limits.map(([title, body]) => (
            <div key={title}>
              <h3>{title}</h3>
              <p>{body}</p>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}

function Closing() {
  return (
    <section className="m-closing">
      <div className="m-shell">
        <h2>Open the demo and break something.</h2>
        <p>
          A read-only production fleet with an incident in it, and a private
          sandbox you can write to, delete from and compact.
        </p>
        <div className="m-cta">
          <Link className="btn btn-primary btn-lg" to="/login?demo=1">
            <Play size={16} aria-hidden /> Open the live demo
          </Link>
          <a
            className="btn btn-lg"
            href={REPOSITORY}
            target="_blank"
            rel="noreferrer noopener"
          >
            <Github size={16} aria-hidden /> Read the source
          </a>
        </div>
      </div>
    </section>
  );
}

function Footer({ version }: { version?: string }) {
  return (
    <footer className="m-footer">
      <div className="m-shell m-footer-inner">
        <Logo size={22} />
        <span className="m-footer-tag">
          A time-series database you can read end to end
          {version ? ` · engine v${version}` : ""}
        </span>
        <nav aria-label="Footer">
          <Link to="/login?demo=1">Live demo</Link>
          <Link to="/docs">Docs</Link>
          <a href={`${REPOSITORY}/blob/master/docs/ARCHITECTURE.md`}>
            Architecture
          </a>
          <a href={USE_CASES_URL} target="_blank" rel="noreferrer noopener">
            Use cases
          </a>
          <a href={REPOSITORY} target="_blank" rel="noreferrer noopener">
            GitHub <ArrowUpRight size={12} aria-hidden />
          </a>
        </nav>
      </div>
    </footer>
  );
}

function SectionHead({
  kicker,
  title,
  lede,
  children,
}: {
  kicker: string;
  title: string;
  lede: string;
  children?: ReactNode;
}) {
  return (
    <div className="m-head">
      <p className="m-kicker">{kicker}</p>
      <h2>{title}</h2>
      <p>{lede}</p>
      {children}
    </div>
  );
}
