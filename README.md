# FluxDB

A single-node time-series database written in Rust — write-ahead log, skip-list
memtable, compressed typed SSTables, bloom filters, compaction, and a SQL subset
with time bucketing — plus the multi-tenant console and control plane that make
it usable: accounts, projects, buckets, roles, revocable API keys, saved
dashboards, threshold monitors and an audit trail.

It runs as **one process on one port**. The same binary serves the storage
engine, the control plane, the token API and the browser console.

```sh
git clone https://github.com/ShravanJanwade/FluxDB.git
cd FluxDB
node start-all.js
```

Then open **http://127.0.0.1:5173** and click **Explore the demo** — no account
needed. You get a seeded production fleet with an incident in it, and one SQL
query finds the failing service.

---

## Contents

- [What it is](#what-it-is)
- [The demo dataset](#the-demo-dataset)
- [Run it locally](#run-it-locally)
- [Deploy it](#deploy-it)
- [Sending data](#sending-data)
- [The SQL subset](#the-sql-subset)
- [Architecture](#architecture)
- [Security model](#security-model)
- [Configuration](#configuration)
- [Operations](#operations)
- [Verification](#verification)
- [What FluxDB is not](#what-fluxdb-is-not)

---

## What it is

Two layers in one binary.

**The engine** (`fluxdb-core`) stores points. A point is a measurement, a
complete tag set, a nanosecond timestamp, and one or more typed fields. Writes
go to a checksummed write-ahead log and a sorted in-memory skip list; full
memtables are flushed to immutable SSTables with per-field type encoding, LZ4
block compression and bloom filters over series keys; a manifest makes each
checkpoint atomic; compaction rewrites files to drop obsolete versions and
tombstones.

**The control plane** (`fluxdb-server::cloud`) is what turns that into something
you can hand to other people: accounts with Argon2id passwords and GitHub
sign-in, organizations and projects, four roles, project-scoped API keys for
agents and SDKs, saved dashboards, monitors evaluated server-side every minute,
per-organization audit trail, and hosted-plan quotas.

Tenancy is enforced by name. Each project owns the `t{project_id}_*` prefix of
engine database names, and **no request can name an engine database directly**:
a caller addresses a bucket by id, the control plane resolves the owning project,
checks the caller's role in that organization, and only then maps to a physical
database. Guessing another tenant's ids returns 404, not their data — there are
integration tests for exactly that.

You can also point the console at a FluxDB you run yourself. The browser talks
to it directly and its token stays in the browser tab; it is never sent to the
host serving the console.

## The demo dataset

Everything in the demo is explicitly synthetic and generated from a seeded
pseudo-random generator, so every deployment produces the same shape.

It is a small production fleet: eight hosts, six services, and — about 90
minutes before "now" — a payment dependency that starts timing out. The
`payments` and `checkout` services' p99 latency and error rate climb, and the
database host they depend on saturates. The shipped dashboard, the four shipped
monitors and the worked query examples all exist so a visitor can find that
incident:

```sql
SELECT MAX(latency_p99) AS p99, SUM(errors) AS errors
FROM http_requests
WHERE $timeFilter
GROUP BY service
ORDER BY p99 DESC
```

```
service        p99         errors
payments       1,224.03    3,007
checkout       1,208.74    4,448
search           130.86    2,092
notifications    106.46    1,077
auth              85.61    1,415
catalog           67.69    1,804
```

A guest workspace gets a private, writable copy of a smaller sample plus
read-only access to the shared showcase. It is deleted automatically, with
everything in it, after 24 hours.

## Run it locally

Needs Rust 1.89+ and Node.js 22.12+. Python 3.10+ is optional, for the SDK, the
restore tool and the smoke suite.

```sh
node start-all.js
```

That builds the engine, installs the console's dependencies from the lockfile,
starts both, waits for readiness, and stops them together on Ctrl+C. It refuses
an occupied port rather than stopping whatever is already there.
`node start-all.js --verify` starts, checks the server, the browser proxy and the
control plane, then exits. Windows users can run `run-fluxdb.bat`; Linux and
macOS `bash run-fluxdb.sh`.

To run the pieces separately:

```sh
# Terminal 1 — API on http://127.0.0.1:8086
cd fluxdb && cargo run --release -p fluxdb-server --bin fluxdb

# Terminal 2 — console on http://127.0.0.1:5173, proxying /api to the server
cd fluxdb-studio && npm ci && npm run dev
```

The server stores time-series data in `data` relative to its working directory
and keeps control-plane metadata in `data/control.sqlite` beside it. A data
directory can be opened by exactly one process.

To serve the built console from the server instead of running Vite — which is
how a deployment works:

```sh
cd fluxdb-studio && npm run build && cd ..
FLUXDB_STATIC_DIR=$PWD/fluxdb-studio/dist \
FLUXDB_SESSION_SECRET=$(python -c "import secrets; print(secrets.token_hex(32))") \
  ./fluxdb/target/release/fluxdb
# Everything on http://127.0.0.1:8086
```

## Deploy it

One container. See [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) for the full guide,
including the free Render + managed Postgres path and the certificate setup for
your own VM.

```sh
cp .env.example .env     # then replace both secrets
docker compose up --build -d
# http://localhost:8080
```

The short version for a hosted deployment: set `FLUXDB_TOKEN`,
`FLUXDB_SESSION_SECRET`, `PUBLIC_BASE_URL`, and `DATABASE_URL` pointing at a
managed Postgres. That last one matters — a hosted container's filesystem
usually does not survive a redeploy, and without an external metadata store
every account goes with it.

## Sending data

Create a project API key in the console, then use either format. Both are
accepted by the hosted ingest endpoints with a project key, and by a self-hosted
server's `/api/v1` with its bearer token.

```sh
# InfluxDB line protocol — what most metrics agents already emit.
curl -X POST 'https://YOUR-DEPLOYMENT/api/ingest/v1/write?bucket=production&precision=s' \
  -H 'Authorization: Bearer fdbk_YOUR_KEY' \
  --data-binary '
cpu,host=api-01,region=us-east-1 usage=42.8,cores=8i 1789142400
http_requests,service=checkout requests=2412i,errors=3i,latency_p99=184.2 1789142400'

# JSON batch, up to 10,000 points.
curl -X POST 'https://YOUR-DEPLOYMENT/api/ingest/v1/points?bucket=production' \
  -H 'Authorization: Bearer fdbk_YOUR_KEY' \
  -H 'Content-Type: application/json' \
  -d '{"points":[{
        "measurement":"cpu",
        "tags":{"host":"api-01"},
        "timestamp":"1789142400000000000",
        "fields":{"usage":42.8,"cores":{"integer":"8"},"healthy":true}
      }]}'

# Read it back.
curl -X POST 'https://YOUR-DEPLOYMENT/api/ingest/v1/query' \
  -H 'Authorization: Bearer fdbk_YOUR_KEY' \
  -H 'Content-Type: application/json' \
  -d '{"bucket":"production","interval":"5m",
       "query":"SELECT MEAN(usage) AS cpu FROM cpu WHERE $timeFilter GROUP BY time($interval), host"}'
```

**Two conventions worth knowing.** Timestamps are decimal nanosecond *strings*,
and exact 64-bit integers are `{"integer": "…"}`. Both exist because an
IEEE-754 double cannot represent the whole `i64` range, so sending them as JSON
numbers would silently round values. Query results return them as strings for
the same reason.

Writing the same measurement, tag set and timestamp again is an **upsert**:
supplied fields replace existing values and unsupplied fields are left alone.
Changing any tag, or the timestamp, addresses a different point.

There are dependency-free clients for [Python](sdk/python/fluxdb.py) and
[JavaScript](sdk/javascript/fluxdb.mjs), an OpenAPI document at
`/api/v1/openapi.json`, and an offline CLI for a data directory no server holds
open.

## The SQL subset

Supported: `SELECT *` and named fields;
`COUNT/SUM/MEAN/MIN/MAX/FIRST/LAST` with `AS` aliases; `WHERE` with `AND`,
`OR`, `NOT`, `IN`, `BETWEEN`, `LIKE`, `IS NULL` and comparisons on fields and
tags; `GROUP BY time('5m')` with optional tag columns; `HAVING`; `DISTINCT`;
`ORDER BY`; `LIMIT`; `OFFSET`.

Not supported: joins across measurements, subqueries, window functions,
computed `SELECT` expressions, `PERCENTILE`, `now()`, the Flux language,
continuous queries and downsampling tasks. Anything outside the subset returns
an error that names what it cannot do, rather than quietly meaning something
else.

Because there is no `now()`, a query carries no implicit notion of "recent".
The API expands four macros from the range you send, which is what lets one
saved dashboard panel serve every range:

| Macro | Becomes |
| --- | --- |
| `$timeFilter` | `time >= <from> AND time <= <to>` |
| `$interval` | a quoted bucket width chosen for chart readability, or the one you asked for |
| `$from`, `$to` | the raw nanosecond bounds |

## Architecture

```
            ┌──────────── write path ────────────┐
 HTTP ─────▶ WAL (CRC32, fsync) ─▶ memtable ─▶ SSTable ─▶ manifest
                                  (skip list)  (LZ4,      (atomic
                                               typed)     checkpoint)
            ┌──────────── read path ─────────────┐              │
 SQL ──▶ parse ─▶ plan ─▶ merge reader ─▶ aggregate            bloom
                          (memtable + SSTables)                filter
                                  ▲                              │
                                  └────── compaction ◀───────────┘
                                       (drops tombstones)
```

A write is durable before it is acknowledged and never mutated in place
afterwards; everything after that is a background rearrangement of immutable
files. On recovery a truncated trailing WAL entry is dropped and anything
corrupt in the middle is reported rather than silently skipped.

Control-plane metadata is a small set of typed JSON documents in one table with
explicit secondary index columns, which is what lets it run unchanged on SQLite
locally and on managed Postgres in a deployment. Uniqueness of email addresses,
organization slugs and bucket names is enforced by that table's index rather
than by checks at the call sites.

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the durability and
concurrency details.

## Security model

- **Passwords** use Argon2id with per-password salts. Sign-in is rate limited
  per account and per address, and the unknown-account branch still spends time
  hashing so it cannot be distinguished by response timing.
- **Sessions** live in an `HttpOnly`, `SameSite=Lax` cookie, `Secure` whenever
  the deployment is reached over HTTPS. Only the SHA-256 of the cookie value is
  stored, so a leaked control-plane database does not hand over live sessions.
  Mutating requests also have their `Origin` checked.
- **API keys** are 256 random bits stored as a SHA-256 digest, scoped to one
  project, shown exactly once, and revocable immediately. A stretching KDF is
  deliberately not used here: it would add tens of milliseconds to every ingest
  request and buys nothing against a secret of that size.
- **GitHub OAuth** uses the authorization-code flow with signed state and a
  per-browser nonce cookie, and only same-origin paths are accepted as a
  post-sign-in destination.
- **The token API** (`/api/v1`) can name any engine database, so it sits
  underneath project isolation. A server with accounts enabled therefore always
  has a token in force — the operator's, or a generated one logged at startup.
- **Self-hosted proxying** forwards only `/api/v1` paths, takes the target token
  per request without storing it, and refuses targets that resolve inside a
  private network.

Missing, and worth saying plainly: there is no email verification, no password
reset by mail and no multi-factor authentication, because this deployment has no
mail provider. Those are the first three things a real product would add.

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `FLUXDB_ADDR` | `127.0.0.1:8086` | Listen address. `PORT` is used instead when a platform assigns one. |
| `FLUXDB_DATA_DIR` | `data` | Time-series storage directory |
| `FLUXDB_STATIC_DIR` | unset | Built console to serve from this process |
| `FLUXDB_TOKEN` | unset | Administration token for `/api/v1`. At least 32 characters is required for a non-loopback bind. |
| `FLUXDB_SESSION_SECRET` | generated | Signs OAuth state. Set 32+ characters so sessions survive a restart. |
| `FLUXDB_CLOUD` | on | `off` runs a plain single-tenant server with no accounts |
| `DATABASE_URL` | unset | Postgres for control-plane metadata. Falls back to SQLite beside the data directory. |
| `PUBLIC_BASE_URL` | derived | Absolute base URL, used for OAuth callbacks and cookie security |
| `GITHUB_CLIENT_ID` / `_SECRET` | unset | Enables GitHub sign-in when both are present |
| `FLUXDB_CORS_ORIGINS` | localhost 5173/4173 | Browser origins allowed to call the API |
| `GEMINI_API_KEY` / `GEMINI_MODEL` | unset | Optional server-side key for the AI assistant |

Never put a provider key in a `VITE_*` variable — those are compiled into the
browser bundle.

## Operations

**Retention** is measured against wall-clock time and enforced by a sweep
roughly every 60 seconds; zero keeps data indefinitely. Shortening a policy
expires older data on the next sweep, and that is not reversible.

**Flush** turns the current memtable into an SSTable, which is why a freshly
written bucket can report zero bytes on disk while its points are perfectly
readable. **Compact** rewrites the SSTables to drop obsolete versions and
tombstones. Both run automatically.

**Backups** are logical JSON snapshots — from the console, or over the API:

```sh
curl -H 'Authorization: Bearer YOUR_TOKEN' \
  http://127.0.0.1:8086/api/v1/databases/observability/export > snapshot.json

python scripts/restore.py snapshot.json --database restored \
  --url http://127.0.0.1:8086
```

Restore is not transactional across batches: if it is interrupted, inspect the
partially created destination and retry into a fresh database rather than the
same one. Never copy live WAL or SSTable files as a backup.

The **offline CLI** works on a data directory no server is holding open:

```sh
cd fluxdb
cargo run -p fluxdb-cli -- --data-dir ./data list
cargo run -p fluxdb-cli -- --data-dir ./data query observability "SELECT COUNT(*) FROM cpu"
```

## Verification

```sh
cd fluxdb
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked          # engine, SQL, and control-plane end to end
cd ..
python scripts/smoke.py                  # isolated authenticated server, both SDKs
cd fluxdb-studio
npm ci && npm run typecheck && npm test && npm run build
```

The Rust suite includes integration tests that drive the real router: one
account cannot reach another's project by any route, roles gate writes and
administration, API key scopes are enforced, the showcase project is read-only
for everyone, proxy targets inside private networks are refused, and `/api/v1`
is unreachable without its token whenever accounts exist.

To measure your own host, with a server running:

```sh
python scripts/benchmark.py --points 10000 --batch 500
```

That reports the workload size, the platform, HTTP point throughput, batch write
p95, query p95 and the verified stored count. These are workload-specific
measurements on your hardware, not a universal throughput claim — which is why
no throughput number appears anywhere else in this README. The console's
**Instance health** page shows the same kind of figures for the running
instance, measured from the last 2,000 requests.

CI runs the Rust suite on Linux and Windows, type-checks and tests the console,
and builds the container image and boots it to confirm it serves the API, the
console and a deep link while refusing anonymous `/api/v1`. A workflow
definition is not evidence of a successful hosted run.

## What FluxDB is not

- **Single node.** No clustering, replication, sharding or failover. One process
  owns a data directory.
- **Memory-bound on reads.** The reader materialises SSTables and query
  snapshots in memory. Large datasets and high-cardinality tag sets need
  streaming and real indexing before they would hold up; export is memory-bound
  for the same reason. The Instance health page shows this honestly — query p95
  rises with the size of the bucket being scanned.
- **Not InfluxDB.** The write endpoints accept line protocol; that is where the
  compatibility stops.
- **Bounded telemetry.** Request history is the last 2,000 application requests,
  in memory, reset on restart. It measures server processing time only — not
  network latency, CPU utilisation or disk IOPS.
- **Ephemeral hosted data.** On a free hosted plan the time-series data lives on
  a container filesystem that does not survive a redeploy. Accounts persist in
  managed Postgres; the points do not.

Tests reduce regressions; they cannot establish production readiness for every
workload. Validate capacity, backups, restore, fault behaviour and HTTPS
configuration for your own environment before trusting FluxDB with data you
cannot lose.

MIT licensed.
