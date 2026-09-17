/**
 * The public front page.
 *
 * Two rules shaped the copy. Every number on this page is measured from the
 * running instance or from a benchmark that ships in the repository — nothing
 * is invented, and the "what this is not" section is as prominent as the
 * feature grid. A database that overstates itself is not credible, and the
 * limits are more interesting than the buzzwords anyway.
 */

import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import {
  ArrowRight,
  BarChart3,
  Bell,
  Check,
  Clock,
  Copy,
  Database,
  FileCode2,
  Gauge,
  Github,
  KeyRound,
  Layers,
  Play,
  Server,
  ShieldCheck,
  Terminal,
  Zap,
} from "lucide-react";
import { Logo } from "../components/Logo";
import { ThemeToggle } from "../components/ThemeToggle";
import { api } from "../lib/api";
import { count, decimal, uptime } from "../lib/format";
import type { PublicStats } from "../lib/types";
import "../styles/marketing.css";

const REPOSITORY = "https://github.com/ShravanJanwade/FluxDB";

export default function Landing() {
  const [stats, setStats] = useState<PublicStats | null>(null);

  useEffect(() => {
    // The hero quotes this instance's own figures. If the call fails the
    // numbers are simply omitted rather than replaced with placeholders.
    api
      .publicStats()
      .then(setStats)
      .catch(() => setStats(null));
    document.title = "FluxDB — a time-series database you can read end to end";
  }, []);

  return (
    <div className="marketing">
      <MarketingNav />
      <main>
        <Hero stats={stats} />
        <Problem />
        <Architecture />
        <Features />
        <Quickstart />
        <ConsoleTour />
        <Deployment />
        <Limits />
        <Faq />
        <ClosingCta />
      </main>
      <MarketingFooter version={stats?.version} />
    </div>
  );
}

function MarketingNav() {
  const [scrolled, setScrolled] = useState(false);
  useEffect(() => {
    const onScroll = () => setScrolled(window.scrollY > 12);
    onScroll();
    window.addEventListener("scroll", onScroll, { passive: true });
    return () => window.removeEventListener("scroll", onScroll);
  }, []);

  return (
    <header className={`m-nav${scrolled ? " is-scrolled" : ""}`}>
      <div className="m-shell m-nav-inner">
        <Link to="/" className="m-nav-brand" aria-label="FluxDB home">
          <Logo />
        </Link>
        <nav className="m-nav-links" aria-label="Sections">
          <a href="#problem">Why</a>
          <a href="#architecture">Architecture</a>
          <a href="#engine">Engine</a>
          <a href="#quickstart">Quickstart</a>
          <a href="#limits">Limits</a>
        </nav>
        <div className="m-nav-actions">
          <ThemeToggle compact />
          <a
            className="btn btn-ghost btn-sm m-nav-github"
            href={REPOSITORY}
            target="_blank"
            rel="noreferrer noopener"
          >
            <Github size={15} aria-hidden /> Source
          </a>
          <Link className="btn btn-sm" to="/login">
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
      <div className="m-hero-glow" aria-hidden />
      <div className="m-shell m-hero-inner">
        <div className="m-hero-copy">
          <span className="m-eyebrow">
            <span className="m-pulse" aria-hidden />
            Written in Rust · LSM storage · SQL over time
          </span>
          <h1>
            A time-series database,
            <br />
            <span className="m-gradient">and the console to run it.</span>
          </h1>
          <p className="m-lede">
            FluxDB stores metrics the way a metrics database should — a
            write-ahead log for durability, a skip-list memtable, compressed
            SSTables, and a SQL subset with time bucketing. Then it gives you a
            hosted workspace, project API keys, dashboards and threshold alerts
            on top, so the storage engine is something you can actually use
            rather than something you have to read the source to believe.
          </p>
          <div className="m-hero-cta">
            <Link className="btn btn-primary btn-lg" to="/signup">
              Create a free workspace <ArrowRight size={17} aria-hidden />
            </Link>
            <Link className="btn btn-lg" to="/login?demo=1">
              <Play size={16} aria-hidden /> Open the live demo
            </Link>
          </div>
          <p className="m-hero-note">
            The demo is one click, needs no email address, and opens a seeded
            production fleet with an incident already in it. Find it with SQL.
          </p>
          <LiveStats stats={stats} />
        </div>
        <HeroChart />
      </div>
    </section>
  );
}

function LiveStats({ stats }: { stats: PublicStats | null }) {
  const items: { label: string; value: string; hint: string }[] = [];
  if (stats) {
    items.push({
      label: "Points in the demo fleet",
      value: count(stats.demo_points),
      hint: "Seeded on this instance",
    });
    if (stats.query_p95_ms !== null) {
      items.push({
        label: "API p95",
        value: `${decimal(stats.query_p95_ms)} ms`,
        hint: `Across ${count(stats.requests_recorded)} recorded requests`,
      });
    }
    items.push({
      label: "Uptime",
      value: uptime(stats.uptime_seconds),
      hint: `Engine v${stats.version}`,
    });
  }
  if (items.length === 0) {
    return null;
  }
  return (
    <dl className="m-livestats">
      {items.map((item) => (
        <div key={item.label}>
          <dt>{item.label}</dt>
          <dd>{item.value}</dd>
          <small>{item.hint}</small>
        </div>
      ))}
      <p className="m-livestats-note">
        Measured from this deployment right now, not from a marketing slide.
        Server processing time only — it excludes network latency.
      </p>
    </dl>
  );
}

/**
 * The hero visual. A hand-drawn series with the same shape as the seeded
 * incident: a quiet baseline, a sharp onset, a slow recovery. Drawn once with a
 * stroke animation, and skipped entirely under reduced-motion preferences.
 */
function HeroChart() {
  const baseline = [
    38, 41, 37, 44, 40, 43, 39, 42, 45, 41, 38, 43, 40, 44, 42, 39,
  ];
  const incident = [58, 96, 148, 186, 172, 150, 128, 104, 82, 64, 52, 46];
  const tail = [42, 45, 40, 43, 39, 41, 44, 40];
  const values = [...baseline, ...incident, ...tail];
  const width = 560;
  const height = 300;
  const maxValue = Math.max(...values) * 1.12;
  const step = width / (values.length - 1);
  const point = (value: number, index: number) =>
    `${(index * step).toFixed(1)},${(height - (value / maxValue) * height).toFixed(1)}`;
  const line = values.map(point).join(" ");
  const area = `0,${height} ${line} ${width},${height}`;
  const peakIndex = values.indexOf(Math.max(...values));

  return (
    <figure className="m-hero-visual" aria-hidden>
      <div className="m-chartcard">
        <div className="m-chartcard-head">
          <div>
            <strong>payments · p99 latency</strong>
            <span>production · us-east-1</span>
          </div>
          <span className="badge badge-danger">Alerting</span>
        </div>
        <svg viewBox={`0 0 ${width} ${height}`} className="m-chartsvg">
          <defs>
            <linearGradient id="m-area" x1="0" y1="0" x2="0" y2="1">
              <stop
                offset="0%"
                stopColor="var(--brand-500)"
                stopOpacity="0.34"
              />
              <stop
                offset="100%"
                stopColor="var(--brand-500)"
                stopOpacity="0"
              />
            </linearGradient>
            <linearGradient id="m-stroke" x1="0" y1="0" x2="1" y2="0">
              <stop offset="0%" stopColor="var(--cyan-500)" />
              <stop offset="55%" stopColor="var(--brand-500)" />
              <stop offset="100%" stopColor="var(--cyan-500)" />
            </linearGradient>
          </defs>
          {[0.25, 0.5, 0.75].map((fraction) => (
            <line
              key={fraction}
              x1="0"
              x2={width}
              y1={height * fraction}
              y2={height * fraction}
              className="m-chartgrid"
            />
          ))}
          <line
            x1="0"
            x2={width}
            y1={height - (120 / maxValue) * height}
            y2={height - (120 / maxValue) * height}
            className="m-chartthreshold"
          />
          <polygon points={area} fill="url(#m-area)" />
          <polyline points={line} className="m-chartline" />
          <circle
            cx={peakIndex * step}
            cy={height - (values[peakIndex] / maxValue) * height}
            r="5"
            className="m-chartpeak"
          />
        </svg>
        <div className="m-chartcard-foot">
          <code>
            SELECT MAX(latency_p99) FROM http_requests GROUP BY
            time(&apos;1m&apos;), service
          </code>
          <span>threshold 120 ms</span>
        </div>
      </div>
    </figure>
  );
}

function Problem() {
  const items = [
    {
      Icon: Clock,
      title: "Metrics are not rows",
      body: "A relational table charges you an index per tag and a row per sample. Time-series data arrives append-only, ordered, and mostly numeric — FluxDB writes it to a log, keeps it sorted in memory, and compresses it into immutable files typed by field.",
    },
    {
      Icon: Terminal,
      title: "A query language you already know",
      body: "No new dialect to learn for the common cases. SELECT, WHERE, AND/OR/NOT, IN, BETWEEN, LIKE, DISTINCT, aggregates, ORDER BY, LIMIT — plus GROUP BY time('1m') for bucketing. Anything unsupported returns an error that names what it cannot do.",
    },
    {
      Icon: ShieldCheck,
      title: "Somewhere to actually keep it",
      body: "An engine on its own is a library. FluxDB adds accounts, projects, roles, revocable API keys, retention, snapshot export and an audit trail — and it will just as happily talk to a server running on your laptop.",
    },
  ];
  return (
    <section className="m-section" id="problem">
      <div className="m-shell">
        <SectionHead
          eyebrow="Why it exists"
          title="Storing time is a different problem"
          lede="Every part of this was built to be read: the compression, the recovery path, the query planner and the tenancy boundary are all a few hundred lines you can follow."
        />
        <div className="m-grid m-grid-3">
          {items.map(({ Icon, title, body }) => (
            <article key={title} className="m-card">
              <span className="m-card-icon">
                <Icon size={19} aria-hidden />
              </span>
              <h3>{title}</h3>
              <p>{body}</p>
            </article>
          ))}
        </div>
      </div>
    </section>
  );
}

/** Write path, read path and the tenancy boundary, drawn rather than described. */
function Architecture() {
  return (
    <section className="m-section m-section-alt" id="architecture">
      <div className="m-shell">
        <SectionHead
          eyebrow="Architecture"
          title="What happens to a point"
          lede="A write is durable before it is acknowledged, and it is never mutated in place afterwards. Everything after that is a background rearrangement of immutable files."
        />
        <div className="m-arch">
          <svg viewBox="0 0 980 360" role="img" aria-labelledby="m-arch-title">
            <title id="m-arch-title">
              A write enters through the API, is appended to the checksummed
              write-ahead log and inserted into the skip-list memtable. Full
              memtables are flushed to compressed, typed SSTables with bloom
              filters and a manifest. Reads merge the memtable with the
              SSTables. Compaction rewrites them to drop obsolete versions and
              tombstones.
            </title>
            <defs>
              <marker
                id="m-arrow"
                viewBox="0 0 10 10"
                refX="9"
                refY="5"
                markerWidth="7"
                markerHeight="7"
                orient="auto-start-reverse"
              >
                <path d="M 0 0 L 10 5 L 0 10 z" fill="var(--border-strong)" />
              </marker>
            </defs>

            {/* Write path */}
            <ArchBox
              x={14}
              y={26}
              w={158}
              h={58}
              label="HTTP API"
              sub="JSON · line protocol"
              tone="accent"
            />
            <ArchBox
              x={228}
              y={26}
              w={158}
              h={58}
              label="Write-ahead log"
              sub="CRC32 · fsync"
              tone="accent"
            />
            <ArchBox
              x={442}
              y={26}
              w={158}
              h={58}
              label="Memtable"
              sub="skip list · sorted"
              tone="accent"
            />
            <ArchBox
              x={656}
              y={26}
              w={158}
              h={58}
              label="SSTable"
              sub="LZ4 · typed blocks"
              tone="accent"
            />

            <ArchArrow x1={172} y1={55} x2={228} y2={55} />
            <ArchArrow x1={386} y1={55} x2={442} y2={55} />
            <ArchArrow x1={600} y1={55} x2={656} y2={55} label="flush" />

            {/* Durable artefacts */}
            <ArchBox
              x={656}
              y={140}
              w={158}
              h={54}
              label="Manifest"
              sub="atomic checkpoint"
            />
            <ArchBox
              x={656}
              y={216}
              w={158}
              h={54}
              label="Bloom filter"
              sub="skip absent series"
            />
            <ArchArrow x1={735} y1={84} x2={735} y2={140} />
            <ArchArrow x1={735} y1={194} x2={735} y2={216} />

            {/* Read path */}
            <ArchBox
              x={14}
              y={178}
              w={158}
              h={58}
              label="SQL query"
              sub="parse · plan"
              tone="cyan"
            />
            <ArchBox
              x={228}
              y={178}
              w={158}
              h={58}
              label="Merge reader"
              sub="memtable + SSTables"
              tone="cyan"
            />
            <ArchBox
              x={442}
              y={178}
              w={158}
              h={58}
              label="Aggregation"
              sub="time buckets · tags"
              tone="cyan"
            />
            <ArchArrow x1={172} y1={207} x2={228} y2={207} />
            <ArchArrow x1={386} y1={207} x2={442} y2={207} />
            <ArchArrow x1={656} y1={243} x2={600} y2={215} />

            {/* Compaction */}
            <ArchBox
              x={656}
              y={292}
              w={158}
              h={50}
              label="Compaction"
              sub="drop tombstones"
            />
            <ArchArrow x1={735} y1={270} x2={735} y2={292} />

            {/* Tenancy */}
            <rect
              x={848}
              y={26}
              width={118}
              height={316}
              rx={12}
              className="m-arch-tenancy"
            />
            <text x={907} y={58} className="m-arch-label" textAnchor="middle">
              Tenancy
            </text>
            <text x={907} y={84} className="m-arch-sub" textAnchor="middle">
              project id
            </text>
            <text x={907} y={102} className="m-arch-sub" textAnchor="middle">
              prefixes every
            </text>
            <text x={907} y={120} className="m-arch-sub" textAnchor="middle">
              database name
            </text>
            <text x={907} y={156} className="m-arch-sub" textAnchor="middle">
              roles checked
            </text>
            <text x={907} y={174} className="m-arch-sub" textAnchor="middle">
              before any
            </text>
            <text x={907} y={192} className="m-arch-sub" textAnchor="middle">
              name resolves
            </text>
            <text x={907} y={228} className="m-arch-sub" textAnchor="middle">
              API keys are
            </text>
            <text x={907} y={246} className="m-arch-sub" textAnchor="middle">
              project scoped
            </text>
          </svg>
        </div>
        <div className="m-arch-notes">
          <div>
            <h4>Durability</h4>
            <p>
              Each WAL entry carries a CRC32 checksum and is synced before the
              write returns. On recovery a truncated trailing entry is dropped
              and anything corrupt in the middle is reported rather than
              silently skipped.
            </p>
          </div>
          <div>
            <h4>Updates and deletes</h4>
            <p>
              Rewriting the same measurement, tag set and timestamp merges
              fields; supplied fields replace existing values. Deletes write
              tombstones, and compaction is what actually reclaims the space.
            </p>
          </div>
          <div>
            <h4>Isolation</h4>
            <p>
              A project owns the <code>t&#123;project&#125;_*</code> prefix of
              engine database names. No request can name a database directly, so
              guessing another tenant&apos;s ids gets you a 404, not their data.
            </p>
          </div>
        </div>
      </div>
    </section>
  );
}

function ArchBox({
  x,
  y,
  w,
  h,
  label,
  sub,
  tone,
}: {
  x: number;
  y: number;
  w: number;
  h: number;
  label: string;
  sub: string;
  tone?: "accent" | "cyan";
}) {
  return (
    <g className={`m-arch-box${tone ? ` tone-${tone}` : ""}`}>
      <rect x={x} y={y} width={w} height={h} rx={10} />
      <text
        x={x + w / 2}
        y={y + h / 2 - 3}
        className="m-arch-label"
        textAnchor="middle"
      >
        {label}
      </text>
      <text
        x={x + w / 2}
        y={y + h / 2 + 15}
        className="m-arch-sub"
        textAnchor="middle"
      >
        {sub}
      </text>
    </g>
  );
}

function ArchArrow({
  x1,
  y1,
  x2,
  y2,
  label,
}: {
  x1: number;
  y1: number;
  x2: number;
  y2: number;
  label?: string;
}) {
  return (
    <g>
      <line
        x1={x1}
        y1={y1}
        x2={x2}
        y2={y2}
        className="m-arch-arrow"
        markerEnd="url(#m-arrow)"
      />
      {label && (
        <text
          x={(x1 + x2) / 2}
          y={y1 - 8}
          className="m-arch-sub"
          textAnchor="middle"
        >
          {label}
        </text>
      )}
    </g>
  );
}

function Features() {
  const features = [
    {
      Icon: Layers,
      title: "LSM storage engine",
      body: "Skip-list memtable, immutable SSTables with per-field type encoding, LZ4 block compression, bloom filters on series keys, and manifest-based full compaction.",
    },
    {
      Icon: ShieldCheck,
      title: "Durability you can test",
      body: "Checksummed WAL with immediate sync, recovery of interrupted trailing writes, corruption detection, and a regression suite that kills and restarts the process mid-write.",
    },
    {
      Icon: Database,
      title: "Typed points",
      body: "Measurement, full tag set, nanosecond timestamp, and float, signed 64-bit integer, string or boolean fields. Exact integers travel as decimal strings so JSON cannot round them.",
    },
    {
      Icon: BarChart3,
      title: "SQL with time buckets",
      body: "Predicates on fields and tags, AND/OR/NOT, IN, BETWEEN, LIKE, IS NULL, DISTINCT, aggregates, ordering, limits, offsets, and GROUP BY time('5m') with tag grouping.",
    },
    {
      Icon: KeyRound,
      title: "Project API keys",
      body: "Scoped read/write tokens for agents and SDKs, over line protocol or JSON. Only a SHA-256 of the secret is stored, the plaintext is shown once, and revocation is immediate.",
    },
    {
      Icon: Bell,
      title: "Monitors and alerts",
      body: "Threshold checks evaluated server-side every minute against real queries, recording state transitions into an alert feed instead of one entry per sweep.",
    },
    {
      Icon: Gauge,
      title: "Observable by default",
      body: "Request latency and error history, per-bucket point counts, SSTable counts and memtable size, plus a Prometheus endpoint and an OpenAPI document.",
    },
    {
      Icon: Server,
      title: "Your server or ours",
      body: "Point the console at a FluxDB on your own machine and the browser talks to it directly — its token stays in the tab and never reaches this host.",
    },
    {
      Icon: Zap,
      title: "Operational controls",
      body: "Retention policies, manual flush and compaction, JSON snapshot export and restore, bounded request bodies and batch sizes, and a concurrency ceiling.",
    },
  ];
  return (
    <section className="m-section" id="engine">
      <div className="m-shell">
        <SectionHead
          eyebrow="Engine and platform"
          title="What is actually implemented"
          lede="Not a roadmap. Every item here has code and tests behind it in the repository."
        />
        <div className="m-grid m-grid-3">
          {features.map(({ Icon, title, body }) => (
            <article key={title} className="m-feature">
              <span className="m-card-icon">
                <Icon size={18} aria-hidden />
              </span>
              <h3>{title}</h3>
              <p>{body}</p>
            </article>
          ))}
        </div>
      </div>
    </section>
  );
}

const SNIPPETS: {
  id: string;
  label: string;
  language: string;
  code: string;
}[] = [
  {
    id: "curl",
    label: "curl",
    language: "bash",
    code: `# Create a project API key in the console, then write points.
curl -X POST 'https://YOUR-DEPLOYMENT/api/ingest/v1/points?bucket=production' \\
  -H 'Authorization: Bearer fdbk_YOUR_KEY' \\
  -H 'Content-Type: application/json' \\
  -d '{"points":[{
        "measurement":"cpu",
        "tags":{"host":"api-01","region":"us-east-1"},
        "timestamp":"1789142400000000000",
        "fields":{"usage":42.8,"cores":{"integer":"8"}}
      }]}'

curl -X POST 'https://YOUR-DEPLOYMENT/api/ingest/v1/query' \\
  -H 'Authorization: Bearer fdbk_YOUR_KEY' \\
  -H 'Content-Type: application/json' \\
  -d '{"bucket":"production",
       "query":"SELECT MEAN(usage) AS cpu FROM cpu WHERE $timeFilter GROUP BY time($interval), host"}'`,
  },
  {
    id: "line",
    label: "Line protocol",
    language: "bash",
    code: `# The format your existing metrics agent already speaks.
curl -X POST 'https://YOUR-DEPLOYMENT/api/ingest/v1/write?bucket=production&precision=s' \\
  -H 'Authorization: Bearer fdbk_YOUR_KEY' \\
  --data-binary '
cpu,host=api-01,region=us-east-1 usage=42.8,cores=8i 1789142400
cpu,host=api-02,region=us-east-1 usage=37.1,cores=8i 1789142400
http_requests,service=checkout requests=2412i,errors=3i,latency_p99=184.2 1789142400'`,
  },
  {
    id: "python",
    label: "Python",
    language: "python",
    code: `from fluxdb import FluxDB   # sdk/python/fluxdb.py

db = FluxDB("http://127.0.0.1:8086", token="YOUR_TOKEN")
db.create_database("observability")

db.write_points("observability", [{
    "measurement": "cpu",
    "tags": {"host": "api-01"},
    "timestamp": "1789142400000000000",
    "fields": {"usage": 42.8, "cores": {"integer": "8"}},
}])

result = db.query("observability", "SELECT MEAN(usage) FROM cpu")
print(result["columns"], result["rows"])`,
  },
  {
    id: "javascript",
    label: "JavaScript",
    language: "javascript",
    code: `import { FluxDB } from "./sdk/javascript/fluxdb.mjs";

const db = new FluxDB("http://127.0.0.1:8086", { token: "YOUR_TOKEN" });
await db.createDatabase("observability");

await db.writePoints("observability", [{
  measurement: "cpu",
  tags: { host: "api-01" },
  timestamp: "1789142400000000000",
  fields: { usage: 42.8, cores: { integer: "8" } },
}]);

const { columns, rows } = await db.query(
  "observability",
  "SELECT MAX(usage) FROM cpu WHERE host = 'api-01'",
);`,
  },
  {
    id: "docker",
    label: "Self-host",
    language: "bash",
    code: `git clone https://github.com/ShravanJanwade/FluxDB.git
cd FluxDB

# One command: builds the engine, installs the console, waits for both.
node start-all.js
# Console on http://127.0.0.1:5173, API on http://127.0.0.1:8086

# Or with containers:
cp .env.example .env    # then replace the placeholder token
docker compose up --build -d`,
  },
];

function Quickstart() {
  const [active, setActive] = useState(SNIPPETS[0].id);
  const [copied, setCopied] = useState(false);
  const snippet = SNIPPETS.find((entry) => entry.id === active) ?? SNIPPETS[0];

  const copy = async () => {
    try {
      await navigator.clipboard.writeText(snippet.code);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1600);
    } catch {
      // Clipboard access can be refused; the code is selectable regardless.
    }
  };

  return (
    <section className="m-section m-section-alt" id="quickstart">
      <div className="m-shell">
        <SectionHead
          eyebrow="Quickstart"
          title="Writing your first point"
          lede={
            'Timestamps are decimal nanosecond strings and exact integers are {"integer": "…"}, because a JSON double cannot carry the full 64-bit range. That is the only surprise in the API.'
          }
        />
        <div className="m-code">
          <div
            className="m-code-tabs"
            role="tablist"
            aria-label="Quickstart language"
          >
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
            <code>{snippet.code}</code>
          </pre>
        </div>
        <p className="m-code-foot">
          <FileCode2 size={15} aria-hidden /> Full reference in the console
          under <strong>Developer resources</strong>, plus an OpenAPI document
          at <code>/api/v1/openapi.json</code> and clients for Python and
          JavaScript in the repository.
        </p>
      </div>
    </section>
  );
}

/** A static rendering of the console's shape. Deliberately not a screenshot:
 *  it stays correct when the console changes, and it restyles with the theme. */
function ConsoleTour() {
  const rows = [
    ["payments", "1,842 ms", "9.1 %", "critical"],
    ["checkout", "612 ms", "1.4 %", "warning"],
    ["search", "118 ms", "0.2 %", "ok"],
    ["catalog", "54 ms", "0.1 %", "ok"],
  ];
  return (
    <section className="m-section" id="console">
      <div className="m-shell m-tour">
        <div className="m-tour-copy">
          <SectionHead
            eyebrow="The console"
            title="Built for the moment something breaks"
            lede="Pick a range, group by service, and read the answer. The workspace is organised the way you already think about infrastructure: organisation, project, bucket, measurement."
            align="left"
          />
          <ul className="m-checklist">
            <li>
              <Check size={16} aria-hidden /> Query workspace with saved
              history, worked examples and results you can chart or export
            </li>
            <li>
              <Check size={16} aria-hidden /> Data explorer with schema
              discovery, per-field charts and point-level edit and delete
            </li>
            <li>
              <Check size={16} aria-hidden /> Dashboards whose panels carry
              <code>$timeFilter</code> and <code>$interval</code>, so one panel
              serves every range
            </li>
            <li>
              <Check size={16} aria-hidden /> Members and roles, revocable API
              keys, retention controls and an audit trail
            </li>
            <li>
              <Check size={16} aria-hidden /> Light and dark, keyboard
              navigation, and a command palette on <kbd>Ctrl</kbd>/<kbd>⌘</kbd>{" "}
              <kbd>K</kbd>
            </li>
          </ul>
          <Link className="btn btn-primary" to="/login?demo=1">
            Try it on the seeded fleet <ArrowRight size={16} aria-hidden />
          </Link>
        </div>
        <div className="m-tour-mock" aria-hidden>
          <div className="m-mock">
            <div className="m-mock-bar">
              <span className="m-mock-dot" />
              <span className="m-mock-dot" />
              <span className="m-mock-dot" />
              <span className="m-mock-url">
                fluxdb · production observability
              </span>
            </div>
            <div className="m-mock-body">
              <aside className="m-mock-side">
                {[
                  "Overview",
                  "Buckets",
                  "Explorer",
                  "Query",
                  "Dashboards",
                  "Monitors",
                ].map((item, index) => (
                  <span
                    key={item}
                    className={index === 3 ? "is-active" : undefined}
                  >
                    {item}
                  </span>
                ))}
              </aside>
              <div className="m-mock-main">
                <div className="m-mock-query">
                  <code>
                    SELECT MAX(latency_p99) AS p99, MEAN(error_rate) AS errors
                    <br />
                    FROM http_requests WHERE $timeFilter GROUP BY service
                    <br />
                    ORDER BY p99 DESC
                  </code>
                </div>
                <table className="m-mock-table">
                  <thead>
                    <tr>
                      <th>service</th>
                      <th>p99</th>
                      <th>errors</th>
                      <th>state</th>
                    </tr>
                  </thead>
                  <tbody>
                    {rows.map((row) => (
                      <tr key={row[0]}>
                        <td>{row[0]}</td>
                        <td>{row[1]}</td>
                        <td>{row[2]}</td>
                        <td>
                          <span
                            className={`badge badge-${
                              row[3] === "critical"
                                ? "danger"
                                : row[3] === "warning"
                                  ? "warning"
                                  : "success"
                            }`}
                          >
                            {row[3]}
                          </span>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                <div className="m-mock-foot">
                  4 rows · 11.4 ms · bucket production
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}

function Deployment() {
  const options = [
    {
      Icon: Zap,
      title: "Hosted workspace",
      price: "Free",
      body: "Sign in with email or GitHub and get an organisation, projects, buckets and API keys. Sized for evaluation and side projects, not for your production fleet.",
      points: [
        "5 projects, 8 buckets each",
        "Project API keys with scopes",
        "Dashboards, monitors, audit trail",
      ],
      cta: { to: "/signup", label: "Create a workspace" },
      featured: true,
    },
    {
      Icon: Server,
      title: "Your own server",
      price: "Self-hosted",
      body: "Run the Rust binary or the container anywhere, then point this console at it. The browser talks to your server directly and its token never reaches this host.",
      points: [
        "No limits beyond your disk",
        "Single bearer token, explicit CORS",
        "Same console, same SQL, same SDKs",
      ],
      cta: { to: "/docs", label: "Read the setup guide" },
    },
    {
      Icon: Terminal,
      title: "Embedded and offline",
      price: "Library + CLI",
      body: "Use the storage engine as a Rust crate, or open a data directory offline with the CLI to list databases and run queries without starting a server.",
      points: [
        "fluxdb-core as a dependency",
        "Offline CLI over a data directory",
        "JSON snapshot export and restore",
      ],
      cta: { href: REPOSITORY, label: "Browse the source" },
    },
  ];
  return (
    <section className="m-section m-section-alt" id="deploy">
      <div className="m-shell">
        <SectionHead
          eyebrow="Deployment"
          title="Three ways to run it"
          lede="The hosted workspace and the self-hosted server are the same code. Nothing in the console is gated behind the hosted plan."
        />
        <div className="m-grid m-grid-3">
          {options.map((option) => (
            <article
              key={option.title}
              className={`m-plan${option.featured ? " is-featured" : ""}`}
            >
              <span className="m-card-icon">
                <option.Icon size={18} aria-hidden />
              </span>
              <h3>{option.title}</h3>
              <span className="m-plan-price">{option.price}</span>
              <p>{option.body}</p>
              <ul>
                {option.points.map((point) => (
                  <li key={point}>
                    <Check size={14} aria-hidden /> {point}
                  </li>
                ))}
              </ul>
              {option.cta.to ? (
                <Link
                  className={`btn ${option.featured ? "btn-primary" : ""}`}
                  to={option.cta.to}
                >
                  {option.cta.label}
                </Link>
              ) : (
                <a
                  className="btn"
                  href={option.cta.href}
                  target="_blank"
                  rel="noreferrer noopener"
                >
                  {option.cta.label}
                </a>
              )}
            </article>
          ))}
        </div>
      </div>
    </section>
  );
}

/** The section most projects leave out. It is here because knowing the edges of
 *  a system is the difference between a demo and engineering. */
function Limits() {
  const limits = [
    [
      "Single node",
      "No clustering, replication, sharding or failover. One process owns a data directory, and only one process may open it.",
    ],
    [
      "Memory-bound reads",
      "The reader materialises SSTables and query snapshots in memory. Large datasets and high-cardinality tag sets need streaming and real indexing work before they would hold up.",
    ],
    [
      "Not InfluxDB",
      "No Flux language, no continuous queries, no downsampling tasks, no InfluxQL parity. The write endpoints accept line protocol; that is where the compatibility stops.",
    ],
    [
      "Bounded telemetry",
      "Request history is the last 2,000 application requests, held in memory and reset on restart. It measures server processing time, not network latency or disk IOPS.",
    ],
    [
      "Hosted demo is ephemeral",
      "The free deployment's time-series data lives on a container filesystem that does not survive a redeploy. Accounts persist in managed Postgres; the points do not.",
    ],
    [
      "No email delivery",
      "Sign-up needs no verification and invitations are not mailed — they are applied when the invited address registers. A production deployment would need a mail provider.",
    ],
  ];
  return (
    <section className="m-section" id="limits">
      <div className="m-shell">
        <SectionHead
          eyebrow="Honest limits"
          title="What FluxDB is not"
          lede="This is a single-node database built to be understood. Validate capacity, backups, restore and fault behaviour for your own workload before trusting it with anything you cannot lose."
        />
        <div className="m-limits">
          {limits.map(([title, body]) => (
            <div key={title}>
              <h4>{title}</h4>
              <p>{body}</p>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}

function Faq() {
  const entries = [
    [
      "Is the demo data real?",
      "It is explicitly synthetic and generated from a seeded pseudo-random generator, so every deployment produces the same shape. It is labelled as sample data everywhere it appears. The latency and point-count figures on this page, by contrast, are measured from the running instance.",
    ],
    [
      "Can I point it at my own server?",
      "Yes. Open Connections in the console, add your server's address, and the browser talks to it directly over /api/v1 with a token you enter per session. Nothing about your server or its token is stored here. A browser reaching a server on your own machine may prompt for local network access.",
    ],
    [
      "What happens to a guest workspace?",
      "It gets a private, writable copy of the sample data and is deleted automatically, with everything in it, 24 hours later. The shared showcase project is read-only for everyone including guests.",
    ],
    [
      "How are passwords and API keys stored?",
      "Passwords use Argon2id with per-password salts. API key secrets are 256 random bits, stored as a SHA-256 digest — a stretching KDF would add tens of milliseconds to every ingest request and buy nothing against a secret of that size. Session cookies are HttpOnly, SameSite=Lax, and only the hash of the cookie value is stored.",
    ],
    [
      "Why SQL rather than a purpose-built language?",
      "Because the interesting part of this project is the storage engine, and SQL meant the query surface could be a documented subset with clear errors instead of a new dialect nobody knows. GROUP BY time('5m') covers the bucketing that a metrics query actually needs.",
    ],
    [
      "How fast is it?",
      "That depends entirely on your hardware and workload, so there is no single number worth quoting. A benchmark script in the repository measures point throughput, batch write p95 and query p95 on your own host and prints the workload it used.",
    ],
  ];
  return (
    <section className="m-section m-section-alt">
      <div className="m-shell">
        <SectionHead eyebrow="Questions" title="Details worth knowing" />
        <div className="m-faq">
          {entries.map(([question, answer]) => (
            <details key={question}>
              <summary>{question}</summary>
              <p>{answer}</p>
            </details>
          ))}
        </div>
      </div>
    </section>
  );
}

function ClosingCta() {
  return (
    <section className="m-closing">
      <div className="m-shell">
        <div className="m-closing-card">
          <h2>Open the demo and break something</h2>
          <p>
            One click, no email address. You get a read-only production fleet
            with an incident in it and a private sandbox you can write to,
            delete from and compact.
          </p>
          <div className="m-hero-cta">
            <Link className="btn btn-primary btn-lg" to="/login?demo=1">
              <Play size={17} aria-hidden /> Start the demo
            </Link>
            <Link className="btn btn-lg" to="/signup">
              Create a free account
            </Link>
          </div>
        </div>
      </div>
    </section>
  );
}

function MarketingFooter({ version }: { version?: string }) {
  return (
    <footer className="m-footer">
      <div className="m-shell m-footer-inner">
        <div>
          <Logo />
          <p>
            A single-node time-series database in Rust, with a multi-tenant
            console. Built as a portfolio project and documented as one.
          </p>
          {version && (
            <span className="m-footer-version">Engine v{version}</span>
          )}
        </div>
        <nav aria-label="Footer">
          <div>
            <h5>Product</h5>
            <Link to="/signup">Create a workspace</Link>
            <Link to="/login?demo=1">Live demo</Link>
            <Link to="/docs">Documentation</Link>
          </div>
          <div>
            <h5>Reference</h5>
            <a href="/api/v1/openapi.json">OpenAPI document</a>
            <a href={`${REPOSITORY}/blob/master/docs/ARCHITECTURE.md`}>
              Architecture notes
            </a>
            <a href={`${REPOSITORY}/blob/master/docs/VERIFICATION.md`}>
              Verification record
            </a>
          </div>
          <div>
            <h5>Source</h5>
            <a href={REPOSITORY} target="_blank" rel="noreferrer noopener">
              GitHub repository
            </a>
            <a href={`${REPOSITORY}/tree/master/sdk`}>
              Python &amp; JS clients
            </a>
            <a href={`${REPOSITORY}/blob/master/README.md`}>Run it locally</a>
          </div>
        </nav>
      </div>
      <div className="m-shell m-footer-legal">
        <span>
          Sample data throughout the demo is synthetic and labelled as such.
        </span>
        <span>MIT licensed</span>
      </div>
    </footer>
  );
}

function SectionHead({
  eyebrow,
  title,
  lede,
  align = "center",
}: {
  eyebrow: string;
  title: string;
  lede?: string;
  align?: "center" | "left";
}) {
  return (
    <div className={`m-head${align === "left" ? " m-head-left" : ""}`}>
      <span className="m-eyebrow m-eyebrow-plain">{eyebrow}</span>
      <h2>{title}</h2>
      {lede && <p>{lede}</p>}
    </div>
  );
}
