/**
 * Developer resources.
 *
 * One searchable page rather than a documentation site: everything someone
 * needs to write to FluxDB, query it, run it themselves, and understand where
 * it stops. Reachable without signing in.
 */

import { useMemo, useState, type ReactNode } from "react";
import { Link } from "react-router-dom";
import { ArrowLeft, ExternalLink, Search } from "lucide-react";
import { Logo } from "../components/Logo";
import { ThemeToggle } from "../components/ThemeToggle";
import { CodeBlock, DataTable, Notice, type Column } from "../components/ui";
import { useSession } from "../lib/session";
import "../styles/console.css";
import "../styles/docs.css";

const REPOSITORY = "https://github.com/ShravanJanwade/FluxDB";

type Topic = {
  id: string;
  title: string;
  /** Plain text used for filtering, so a search matches prose and code alike. */
  keywords: string;
  body: ReactNode;
};

type Endpoint = {
  method: string;
  path: string;
  purpose: string;
  auth: string;
};

const CLOUD_ENDPOINTS: Endpoint[] = [
  {
    method: "POST",
    path: "/api/cloud/auth/signup",
    purpose: "Create an account and its first workspace",
    auth: "None",
  },
  {
    method: "POST",
    path: "/api/cloud/auth/login",
    purpose: "Sign in and receive the session cookie",
    auth: "None",
  },
  {
    method: "POST",
    path: "/api/cloud/auth/guest",
    purpose: "Start a temporary demo workspace",
    auth: "None",
  },
  {
    method: "GET",
    path: "/api/cloud/auth/session",
    purpose: "Current account, workspaces and projects",
    auth: "Session",
  },
  {
    method: "GET",
    path: "/api/cloud/projects/:project",
    purpose: "Buckets, keys, monitors, alerts and usage",
    auth: "Session",
  },
  {
    method: "POST",
    path: "/api/cloud/projects/:project/buckets",
    purpose: "Create a bucket",
    auth: "Session · member",
  },
  {
    method: "GET",
    path: "/api/cloud/projects/:project/buckets/:bucket/points",
    purpose: "Paginated read, newest first",
    auth: "Session",
  },
  {
    method: "POST",
    path: "/api/cloud/projects/:project/buckets/:bucket/points",
    purpose: "Write or upsert a batch",
    auth: "Session · member",
  },
  {
    method: "DELETE",
    path: "/api/cloud/projects/:project/buckets/:bucket/points",
    purpose: "Delete a time range, optionally exact series",
    auth: "Session · member",
  },
  {
    method: "POST",
    path: "/api/cloud/projects/:project/buckets/:bucket/query",
    purpose: "Run SQL with macro expansion",
    auth: "Session",
  },
  {
    method: "GET",
    path: "/api/cloud/projects/:project/buckets/:bucket/schema",
    purpose: "Measurements, tag values and field types",
    auth: "Session",
  },
  {
    method: "GET",
    path: "/api/cloud/projects/:project/buckets/:bucket/export",
    purpose: "Full JSON snapshot",
    auth: "Session",
  },
  {
    method: "GET",
    path: "/api/cloud/public/stats",
    purpose: "Instance version, uptime and request percentiles",
    auth: "None",
  },
];

const INGEST_ENDPOINTS: Endpoint[] = [
  {
    method: "GET",
    path: "/api/ingest/v1/whoami",
    purpose: "Project, key name, scopes and reachable buckets",
    auth: "API key",
  },
  {
    method: "POST",
    path: "/api/ingest/v1/write",
    purpose: "Line protocol. ?bucket= and ?precision=ns|us|ms|s",
    auth: "API key · write",
  },
  {
    method: "POST",
    path: "/api/ingest/v1/points",
    purpose: "JSON batch of up to 10,000 points",
    auth: "API key · write",
  },
  {
    method: "POST",
    path: "/api/ingest/v1/query",
    purpose: "SQL against one bucket",
    auth: "API key · read",
  },
];

const V1_ENDPOINTS: Endpoint[] = [
  {
    method: "GET",
    path: "/health",
    purpose: "Liveness and engine version",
    auth: "None",
  },
  {
    method: "GET",
    path: "/api/v1/databases",
    purpose: "List databases",
    auth: "Token",
  },
  {
    method: "POST",
    path: "/api/v1/databases/:name",
    purpose: "Create a database",
    auth: "Token",
  },
  {
    method: "DELETE",
    path: "/api/v1/databases/:name",
    purpose: "Drop a database and its files",
    auth: "Token",
  },
  {
    method: "GET",
    path: "/api/v1/databases/:name/points",
    purpose: "Paginated read",
    auth: "Token",
  },
  {
    method: "POST",
    path: "/api/v1/databases/:name/points",
    purpose: "Write or upsert a batch",
    auth: "Token",
  },
  {
    method: "POST",
    path: "/api/v1/databases/:name/query",
    purpose: "Run SQL",
    auth: "Token",
  },
  {
    method: "PUT",
    path: "/api/v1/databases/:name/retention",
    purpose: "Set the retention policy in seconds",
    auth: "Token",
  },
  {
    method: "POST",
    path: "/api/v1/databases/:name/flush",
    purpose: "Flush the memtable to an SSTable",
    auth: "Token",
  },
  {
    method: "POST",
    path: "/api/v1/databases/:name/compact",
    purpose: "Rewrite files, dropping obsolete versions",
    auth: "Token",
  },
  {
    method: "POST",
    path: "/write",
    purpose: "Line protocol, InfluxDB-compatible path",
    auth: "Token",
  },
  {
    method: "GET",
    path: "/metrics",
    purpose: "Prometheus gauges for databases, points and bytes",
    auth: "Token",
  },
  {
    method: "GET",
    path: "/api/v1/openapi.json",
    purpose: "Machine-readable API description",
    auth: "Token",
  },
];

const endpointColumns: Column<Endpoint>[] = [
  {
    key: "method",
    header: "Method",
    width: "88px",
    render: (row) => <span className="docs-method">{row.method}</span>,
  },
  {
    key: "path",
    header: "Path",
    render: (row) => <span className="mono">{row.path}</span>,
  },
  {
    key: "purpose",
    header: "Purpose",
    render: (row) => row.purpose,
  },
  {
    key: "auth",
    header: "Auth",
    width: "150px",
    secondary: true,
    render: (row) => <span className="badge">{row.auth}</span>,
  },
];

function topics(origin: string): Topic[] {
  return [
    {
      id: "start",
      title: "Getting started",
      keywords: "quickstart start hosted self-host run local install",
      body: (
        <>
          <p>
            There are two ways to use FluxDB, and they run the same code. The
            hosted workspace gives you an organisation, projects, buckets and
            API keys behind a sign-in. Self-hosting gives you one process, one
            bearer token, and no limits beyond your disk.
          </p>
          <h4>Hosted</h4>
          <ol className="docs-steps">
            <li>
              Create a workspace, or open the demo — it needs no email address.
            </li>
            <li>
              Create a bucket, or load the labelled sample dataset into the one
              you were given.
            </li>
            <li>
              Issue a project API key, then point an agent or a script at the
              ingest endpoints.
            </li>
          </ol>
          <h4>Self-hosted</h4>
          <CodeBlock
            language="bash"
            code={`git clone ${REPOSITORY}.git
cd FluxDB

# Builds the engine, installs the console's dependencies, waits for both.
node start-all.js
# API on http://127.0.0.1:8086 · console on http://127.0.0.1:5173

# Or with containers:
cp .env.example .env    # replace the placeholder token
docker compose up --build -d`}
          />
          <p>
            Requires Rust 1.89 or newer and Node.js 22.12 or newer. Python 3.10+
            is optional, for the SDK, the demo generator, the restore tool and
            the smoke suite.
          </p>
        </>
      ),
    },
    {
      id: "model",
      title: "The data model",
      keywords:
        "point measurement tag field timestamp integer upsert series cardinality",
      body: (
        <>
          <p>
            A point is a <strong>measurement</strong>, a complete{" "}
            <strong>tag set</strong>, a nanosecond <strong>timestamp</strong>,
            and one or more <strong>fields</strong>. Fields are float, signed
            64-bit integer, string or boolean. Together the measurement and the
            full tag set identify a <em>series</em>.
          </p>
          <CodeBlock
            language="json"
            code={`{
  "measurement": "cpu",
  "tags": { "host": "api-01", "region": "us-east-1" },
  "timestamp": "1789142400000000000",
  "fields": {
    "usage": 42.8,                    // float
    "cores": { "integer": "8" },      // exact 64-bit integer
    "healthy": true,                  // boolean
    "kernel": "6.8.0"                 // string
  }
}`}
          />
          <Notice tone="info" title="Two conventions worth knowing">
            Timestamps are <strong>decimal strings</strong>, and exact integers
            are <code>&#123;"integer": "…"&#125;</code>. Both exist because an
            IEEE-754 double cannot represent the whole <code>i64</code> range,
            so sending them as JSON numbers would silently round values. Query
            results return them as strings for the same reason.
          </Notice>
          <h4>Updates and deletes</h4>
          <p>
            Writing the same measurement, tag set and timestamp again is an{" "}
            <strong>upsert</strong>: supplied fields replace existing values and
            unsupplied fields are left alone. Changing any tag, or the
            timestamp, addresses a different point instead. Deleting takes an
            inclusive start and end; <code>exact: true</code> requires the tag
            set to match a series exactly rather than acting as a filter. A
            delete removes all of a point's fields and writes a tombstone;
            compaction reclaims the space afterwards.
          </p>
        </>
      ),
    },
    {
      id: "writing",
      title: "Writing data",
      keywords:
        "write ingest line protocol telegraf batch curl python javascript sdk",
      body: (
        <>
          <p>
            Two formats, both accepted by the hosted ingest endpoints with a
            project API key and by a self-hosted server with its bearer token. A
            batch holds 1 to 10,000 points and a request body is capped at 2
            MiB.
          </p>
          <h4>Line protocol</h4>
          <CodeBlock
            language="bash"
            code={`curl -X POST '${origin}/api/ingest/v1/write?bucket=production&precision=s' \\
  -H 'Authorization: Bearer fdbk_YOUR_KEY' \\
  --data-binary '
cpu,host=api-01,region=us-east-1 usage=42.8,cores=8i 1789142400
http_requests,service=checkout requests=2412i,errors=3i,latency_p99=184.2 1789142400'`}
          />
          <p>
            Integers carry an <code>i</code> suffix, strings are quoted, and
            commas, spaces and equals signs inside keys or tag values are
            escaped with a backslash. <code>precision</code> accepts{" "}
            <code>ns</code>, <code>us</code>, <code>ms</code> or <code>s</code>{" "}
            and defaults to nanoseconds. A line with no timestamp is stamped on
            arrival.
          </p>
          <h4>JSON</h4>
          <CodeBlock
            language="bash"
            code={`curl -X POST '${origin}/api/ingest/v1/points?bucket=production' \\
  -H 'Authorization: Bearer fdbk_YOUR_KEY' \\
  -H 'Content-Type: application/json' \\
  -d '{"points":[{"measurement":"cpu","tags":{"host":"api-01"},
        "timestamp":"1789142400000000000","fields":{"usage":42.8}}]}'`}
          />
          <p>
            An invalid point rejects the whole batch with a message naming the
            field at fault, rather than writing some of it.
          </p>
        </>
      ),
    },
    {
      id: "sql",
      title: "Querying with SQL",
      keywords:
        "sql select where group by time aggregate macro timefilter interval subset",
      body: (
        <>
          <p>
            A documented subset of SQL. Anything outside it returns an error
            that names what is unsupported, so a query never silently means
            something different from what you wrote.
          </p>
          <CodeBlock
            language="sql"
            code={`-- Which service broke, and how badly?
SELECT MAX(latency_p99) AS p99, SUM(errors) AS errors
FROM http_requests
WHERE $timeFilter
GROUP BY service
ORDER BY p99 DESC

-- One series per host, bucketed for the chart
SELECT MEAN(usage) AS cpu
FROM cpu
WHERE $timeFilter
GROUP BY time($interval), host

-- Raw points, newest first
SELECT * FROM http_requests
WHERE service = 'payments' AND $timeFilter
ORDER BY time DESC LIMIT 50`}
          />
          <h4>Supported</h4>
          <ul className="docs-list">
            <li>
              <code>SELECT *</code>, named fields, and{" "}
              <code>COUNT/SUM/MEAN/MIN/MAX/FIRST/LAST</code> with{" "}
              <code>AS</code> aliases
            </li>
            <li>
              <code>WHERE</code> with <code>AND</code>, <code>OR</code>,{" "}
              <code>NOT</code>, <code>IN</code>, <code>BETWEEN</code>,{" "}
              <code>LIKE</code>, <code>IS NULL</code> and comparisons on fields
              and tags
            </li>
            <li>
              <code>GROUP BY time('5m')</code> with optional tag columns, and{" "}
              <code>HAVING</code>
            </li>
            <li>
              <code>DISTINCT</code>, <code>ORDER BY</code>, <code>LIMIT</code>,{" "}
              <code>OFFSET</code>
            </li>
          </ul>
          <h4>Not supported</h4>
          <ul className="docs-list">
            <li>
              Joins across measurements, subqueries, window functions, computed
              SELECT expressions, and <code>now()</code>
            </li>
            <li>
              <code>PERCENTILE</code>, <code>DISTINCT</code> inside an
              aggregate, and aggregate <code>FILTER</code>/<code>OVER</code>{" "}
              clauses
            </li>
            <li>
              The Flux language, continuous queries and downsampling tasks
            </li>
          </ul>
          <h4>Macros</h4>
          <p>
            There is no <code>now()</code>, so a query carries no implicit
            notion of "recent". Instead the API expands four macros from the
            range you send, which is what lets a saved dashboard panel serve
            every range:
          </p>
          <ul className="docs-list">
            <li>
              <code>$timeFilter</code> →{" "}
              <code>time &gt;= … AND time &lt;= …</code>
            </li>
            <li>
              <code>$interval</code> → a quoted bucket width chosen for
              readability, or the one you asked for
            </li>
            <li>
              <code>$from</code>, <code>$to</code> → the raw nanosecond bounds
            </li>
          </ul>
          <p>
            Accepted intervals are 10s, 30s, 1m, 2m, 5m, 10m, 15m, 30m, 1h, 3h,
            6h, 12h and 1d. Writing absolute nanosecond literals or RFC 3339
            strings directly works too.
          </p>
        </>
      ),
    },
    {
      id: "api",
      title: "API reference",
      keywords: "api rest endpoints reference openapi http routes",
      body: (
        <>
          <p>
            Three surfaces. The hosted control plane uses the session cookie and
            addresses data as project and bucket. The ingest endpoints use a
            project API key. A self-hosted server exposes the single-tenant{" "}
            <code>/api/v1</code> surface behind one bearer token.
          </p>
          <h4>Control plane · session cookie</h4>
          <DataTable
            columns={endpointColumns}
            rows={CLOUD_ENDPOINTS}
            rowKey={(row) => `${row.method}${row.path}`}
            dense
          />
          <h4>Ingest · project API key</h4>
          <DataTable
            columns={endpointColumns}
            rows={INGEST_ENDPOINTS}
            rowKey={(row) => `${row.method}${row.path}`}
            dense
          />
          <h4>Self-hosted · bearer token</h4>
          <DataTable
            columns={endpointColumns}
            rows={V1_ENDPOINTS}
            rowKey={(row) => `${row.method}${row.path}`}
            dense
          />
          <Notice tone="info">
            Errors are always <code>&#123;"error": "…"&#125;</code> with a
            machine-readable <code>code</code> on control-plane responses. 402
            means a hosted-plan quota rather than a malformed request, and 429
            carries a rate limit.
          </Notice>
        </>
      ),
    },
    {
      id: "sdk",
      title: "Clients",
      keywords: "sdk python javascript client library cli",
      body: (
        <>
          <p>
            Small, dependency-free clients for the token API, plus an offline
            CLI for a data directory no server is holding open.
          </p>
          <CodeBlock
            language="python"
            code={`from fluxdb import FluxDB   # sdk/python/fluxdb.py

db = FluxDB("http://127.0.0.1:8086", token="YOUR_TOKEN")
db.create_database("observability")
db.write_points("observability", [{
    "measurement": "cpu",
    "tags": {"host": "api-01"},
    "timestamp": "1789142400000000000",
    "fields": {"usage": 42.8},
}])
print(db.query("observability", "SELECT MEAN(usage) FROM cpu"))`}
          />
          <CodeBlock
            language="javascript"
            code={`import { FluxDB } from "./sdk/javascript/fluxdb.mjs";

const db = new FluxDB("http://127.0.0.1:8086", { token: "YOUR_TOKEN" });
await db.writePoints("observability", [{
  measurement: "cpu",
  tags: { host: "api-01" },
  timestamp: "1789142400000000000",
  fields: { usage: 42.8 },
}]);`}
          />
          <CodeBlock
            language="bash"
            code={`# Offline CLI. Stop the server first: a data directory has one owner.
cd fluxdb
cargo run -p fluxdb-cli -- --data-dir ./data list
cargo run -p fluxdb-cli -- --data-dir ./data query observability \\
  "SELECT COUNT(*) FROM cpu"`}
          />
        </>
      ),
    },
    {
      id: "operations",
      title: "Operations",
      keywords:
        "retention flush compact backup export restore snapshot maintenance",
      body: (
        <>
          <h4>Retention</h4>
          <p>
            Measured against wall-clock time and enforced by a sweep roughly
            every 60 seconds. Zero keeps data indefinitely. Shortening a policy
            expires older data on the next sweep, and that is not reversible —
            export first.
          </p>
          <h4>Flush and compact</h4>
          <p>
            <strong>Flush</strong> turns the current memtable into an SSTable,
            which is why a freshly written bucket can report zero bytes on disk
            while its points are perfectly readable. <strong>Compact</strong>{" "}
            rewrites the SSTables, dropping obsolete point versions and
            tombstones. Both run automatically; the buttons exist so you can
            watch them work.
          </p>
          <h4>Backup and restore</h4>
          <CodeBlock
            language="bash"
            code={`# Export from the console, or over the API:
curl -H 'Authorization: Bearer YOUR_TOKEN' \\
  http://127.0.0.1:8086/api/v1/databases/observability/export > snapshot.json

# Restore into a new database:
python scripts/restore.py snapshot.json --database restored \\
  --url http://127.0.0.1:8086`}
          />
          <Notice tone="warning" title="Restore is not transactional">
            It writes bounded batches, verifies the point count, compacts, and
            applies retention last. If it is interrupted, inspect the partially
            created destination and retry into a fresh database rather than the
            same one. Never copy live WAL or SSTable files as a backup.
          </Notice>
        </>
      ),
    },
    {
      id: "selfhost",
      title: "Self-hosting and configuration",
      keywords:
        "configuration environment variables docker deploy cors token postgres",
      body: (
        <>
          <table className="docs-table">
            <thead>
              <tr>
                <th>Variable</th>
                <th>Default</th>
                <th>Purpose</th>
              </tr>
            </thead>
            <tbody>
              {[
                ["FLUXDB_ADDR", "127.0.0.1:8086", "Listen address and port"],
                ["FLUXDB_DATA_DIR", "data", "Time-series storage directory"],
                [
                  "FLUXDB_TOKEN",
                  "unset",
                  "Shared bearer token for /api/v1. At least 32 characters is required for a non-loopback bind",
                ],
                [
                  "FLUXDB_CORS_ORIGINS",
                  "localhost:5173, :4173",
                  "Browser origins allowed to call the API",
                ],
                [
                  "FLUXDB_CLOUD",
                  "on",
                  "Set to off to run a plain single-tenant server with no accounts",
                ],
                [
                  "DATABASE_URL",
                  "unset",
                  "Postgres for the control plane. Falls back to a SQLite file beside the data directory",
                ],
                [
                  "FLUXDB_SESSION_SECRET",
                  "generated",
                  "Signs OAuth state. Set 32+ characters so sessions survive a restart",
                ],
                [
                  "GITHUB_CLIENT_ID / _SECRET",
                  "unset",
                  "Enables GitHub sign-in when both are present",
                ],
                [
                  "PUBLIC_BASE_URL",
                  "derived",
                  "Absolute base URL used for OAuth callbacks and cookie security",
                ],
              ].map(([variable, value, purpose]) => (
                <tr key={variable}>
                  <td className="mono">{variable}</td>
                  <td className="mono cell-muted">{value}</td>
                  <td>{purpose}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <p>
            A data directory can be opened by exactly one process. Use an
            absolute <code>FLUXDB_DATA_DIR</code> when running as a service, put
            an HTTPS reverse proxy in front of anything reachable from outside
            the host, and never expose a development Vite server as the public
            deployment.
          </p>
        </>
      ),
    },
    {
      id: "security",
      title: "Security model",
      keywords:
        "security password argon2 session cookie csrf tenancy isolation ssrf",
      body: (
        <>
          <ul className="docs-list">
            <li>
              <strong>Passwords</strong> use Argon2id with a per-password salt.
              Sign-in is rate limited per account and per address, and the
              unknown-account branch still spends time hashing so it cannot be
              distinguished by timing.
            </li>
            <li>
              <strong>Sessions</strong> live in an <code>HttpOnly</code>,{" "}
              <code>SameSite=Lax</code> cookie, <code>Secure</code> whenever the
              deployment is reached over HTTPS. Only the SHA-256 of the cookie
              value is stored, so a leaked control-plane database does not hand
              over live sessions. State-changing requests additionally have
              their <code>Origin</code> checked.
            </li>
            <li>
              <strong>API keys</strong> are 256 random bits stored as a SHA-256
              digest, scoped to one project, and revocable immediately.
            </li>
            <li>
              <strong>Tenancy</strong> is enforced by name. Every project owns
              the <code>t&#123;project&#125;_*</code> prefix of engine database
              names and no request can name a database directly: a bucket id is
              resolved to a physical database only after the caller's role in
              the owning organisation has been checked. Unauthorised reads
              answer 404 rather than 403, so ids cannot be probed for existence.
            </li>
            <li>
              <strong>Self-hosted tokens</strong> never reach this host in
              browser-direct mode. The proxied mode forwards only{" "}
              <code>/api/v1</code> paths, takes the token per request without
              storing it, and refuses targets that resolve inside a private
              network.
            </li>
          </ul>
          <Notice tone="warning" title="What is missing">
            There is no email verification, no password reset by mail, and no
            multi-factor authentication, because this deployment has no mail
            provider. Those are the first three things a real product would add.
          </Notice>
        </>
      ),
    },
    {
      id: "limits",
      title: "Limits",
      keywords: "limits scale cardinality memory single node not production",
      body: (
        <>
          <ul className="docs-list">
            <li>
              Single node. No clustering, replication, sharding or failover, and
              one process per data directory.
            </li>
            <li>
              The reader materialises SSTables and query snapshots in memory.
              Large datasets and high-cardinality tag sets need streaming and
              real indexing before they would hold up. Export is memory-bound
              for the same reason.
            </li>
            <li>
              Request history is the last 2,000 application requests, in memory,
              reset on restart. It measures server processing time only.
            </li>
            <li>
              Hosted-plan quotas: 5 projects per workspace, 8 buckets per
              project, 2,000,000 points per project. Guest workspaces are
              deleted after 24 hours.
            </li>
            <li>
              The hosted demo's time-series data lives on a container filesystem
              that does not survive a redeploy. Accounts persist in managed
              Postgres; the points do not.
            </li>
          </ul>
          <p>
            Tests reduce regressions; they cannot establish production readiness
            for every workload. Validate capacity, backups, restore, fault
            behaviour and HTTPS configuration for your own environment before
            trusting FluxDB with data you cannot lose.
          </p>
        </>
      ),
    },
  ];
}

export default function Docs() {
  const { status, defaultProjectId } = useSession();
  const [term, setTerm] = useState("");
  const origin = useMemo(() => location.origin, []);
  const all = useMemo(() => topics(origin), [origin]);

  const visible = useMemo(() => {
    const needle = term.trim().toLowerCase();
    if (!needle) return all;
    return all.filter((topic) =>
      `${topic.title} ${topic.keywords}`.toLowerCase().includes(needle),
    );
  }, [all, term]);

  return (
    <div className="docs">
      <header className="docs-top">
        <div className="docs-top-inner">
          {status === "authenticated" && defaultProjectId ? (
            <Link
              className="btn btn-sm btn-ghost"
              to={`/app/p/${defaultProjectId}`}
            >
              <ArrowLeft size={15} aria-hidden /> Back to the console
            </Link>
          ) : (
            <Link to="/" aria-label="FluxDB home">
              <Logo size={24} />
            </Link>
          )}
          <div className="docs-search">
            <Search size={15} aria-hidden />
            <input
              value={term}
              onChange={(event) => setTerm(event.target.value)}
              placeholder="Search the guide"
              aria-label="Search the guide"
            />
          </div>
          <a
            className="btn btn-sm"
            href={REPOSITORY}
            target="_blank"
            rel="noreferrer noopener"
          >
            Source <ExternalLink size={13} aria-hidden />
          </a>
          <ThemeToggle compact />
        </div>
      </header>

      <div className="docs-body">
        <nav className="docs-nav" aria-label="Contents">
          <span className="nav-label">Contents</span>
          {all.map((topic) => (
            <a key={topic.id} href={`#${topic.id}`}>
              {topic.title}
            </a>
          ))}
          <span className="nav-label">Elsewhere</span>
          <a href="/api/v1/openapi.json">OpenAPI document</a>
          <a href={`${REPOSITORY}/blob/master/docs/ARCHITECTURE.md`}>
            Architecture notes
          </a>
          <a href={`${REPOSITORY}/blob/master/docs/VERIFICATION.md`}>
            Verification record
          </a>
        </nav>

        <main className="docs-main">
          <h1>Developer resources</h1>
          <p className="docs-lede">
            Everything needed to write to FluxDB, query it, run it yourself, and
            know where it stops.
          </p>
          {visible.length === 0 && (
            <Notice tone="info">
              Nothing in the guide matches “{term}”. Clear the search to see all
              sections.
            </Notice>
          )}
          {visible.map((topic) => (
            <section key={topic.id} id={topic.id} className="docs-topic">
              <h2>{topic.title}</h2>
              {topic.body}
            </section>
          ))}
        </main>
      </div>
    </div>
  );
}
