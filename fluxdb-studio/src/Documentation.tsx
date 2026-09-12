import { useState } from "react";
import { ArrowUpRight, BookOpen, Check, Copy, Search } from "lucide-react";

type Section = {
  id: string;
  title: string;
  intro: string;
  blocks: { title: string; text?: string; code?: string; language?: string }[];
};
export default function Documentation({
  baseUrl,
  database,
}: {
  baseUrl: string;
  database: string;
}) {
  const [topic, setTopic] = useState("start");
  const [search, setSearch] = useState("");
  const base = baseUrl || window.location.origin;
  const db = database || "observability";
  const root = `${base}/api/v1/databases/${encodeURIComponent(db)}`;
  const bashQuote = (value: string) => "'" + value.replace(/'/g, "'\\''") + "'";
  const curl = (method: string, path: string, body?: object) =>
    `curl -X ${method} ${bashQuote(path)} \\\n  -H 'Authorization: Bearer YOUR_TOKEN'${body ? ` \\\n  -H 'Content-Type: application/json' \\\n  -d ${bashQuote(JSON.stringify(body))}` : ""}`;
  const point = {
    measurement: "cpu",
    tags: { host: "api-01", region: "us-east-1" },
    timestamp: "1789142400000000123",
    fields: { usage: 42.8, healthy: true, requests: { integer: "1200" } },
  };
  const sections: Section[] = [
    {
      id: "assistant",
      title: "AI agent & Gemini setup",
      intro:
        "FluxDB Copilot is available from Ask AI on every page. It uses native Gemini function calling to inspect schema, run optional bounded reads, and prepare validated database operations you can execute in the console.",
      blocks: [
        {
          title: "Connect Gemini",
          text: "Open Ask AI → Assistant settings. Enter your Gemini API key and a model ID enabled in your Google AI Studio project. Click Check key & load available models to verify access and choose a returned ID. The session key stays in browser memory and is sent only to the selected FluxDB server over the assistant request header; re-enter it after a page reload. It is never saved to browser storage. Only connect to servers you trust. Use HTTPS remotely.",
        },
        {
          title: "Server-managed key",
          text: "Alternatively create .env in the repository root. Restart the backend after changes. Docker Compose and node start-all.js load this file. The Rust server also reads .env from its current directory or ancestors. Never use a VITE_ variable for a provider key. Keep your FluxDB bearer token separate.",
          code: "GEMINI_API_KEY=your_google_ai_studio_api_key\nGEMINI_MODEL=gemini-2.5-pro",
          language: ".env · repository root",
        },
        {
          title: "Read → plan → review → execute",
          text: "Select a database, describe your objective, and inspect the tool steps. Copilot can inspect measurement names, field types, and tag keys. Enable Allow read-only data analysis to let it execute SELECT statements with LIMIT 1..100 and send results to Gemini. Otherwise it can prepare queries for local execution without reading rows itself. Run operation displays the real API result; Open in editor transfers SQL to Query workspace.",
        },
        {
          title: "Writes and commands",
          text: "The agent prepares writes/upserts, point deletions, database creation/deletion, retention changes, flush, compaction, and snapshot export. Review the exact payload and target before Confirm & run. Destructive operations require typing the database name. Updates preserve the original measurement, complete tags and timestamp. SQL INSERT/UPDATE/DELETE are unsupported; use the validated operation cards. Arbitrary shell commands cannot run.",
          code: "Inspect my schema and show the latest 10 CPU points.\nWrite a current cpu point for host api-01 with usage 42.8.\nUpdate usage to 18 on the point at timestamp 1789142400000000123 with host api-01.\nPrepare a snapshot export for this database.\nExplain the WAL and compaction workflow.",
          language: "Example prompts",
        },
        {
          title: "Results and conversation scope",
          text: "Completed operations show local results and cannot be run twice from the same card. Follow up to analyze results when data analysis is enabled. Changing database or server clears the conversation and proposals. A turn allows four provider rounds and eight tool calls, with a 100-second deadline. Multiple proposals are independent operations, not a transaction. Export downloads remain local. Cancel generation does not undo completed reads.",
        },
        {
          title: "Troubleshooting",
          text: "Endpoint unavailable / HTTP 404: restart the updated backend. HTTP 401 from FluxDB: reconnect with your server token. Missing key: configure a session or server key. Gemini rejected key: check API project permissions. Provider 429: inspect quota and billing in AI Studio. Model errors: choose a generateContent model with function calling available to your project. API access and billing are managed separately from the Gemini chat application. The interface reports provider HTTP status without exposing keys.",
        },
        {
          title: "Scope and privacy",
          text: "The bundled system prompt covers this project's supported SQL, exact types, APIs, and architecture. Copilot is instructed to help only with FluxDB. Messages and requested schema metadata go to Google; row data requires the analysis opt-in. Host-enforced tool validation scopes existing-database actions to the selected database and prevents automatic mutations. Model replies can still be mistaken: verify the displayed SQL and payload.",
        },
      ],
    },
    {
      id: "start",
      title: "Getting started",
      intro:
        "Go from a running server to your first query. FluxDB is a single-node time-series database written in Rust, with a browser console and an HTTP API.",
      blocks: [
        {
          title: "1. Start the database server",
          text: "From the repository root. Requires Rust 1.89 or later. The server listens on 127.0.0.1:8086 and stores data under fluxdb/data by default.",
          code: "cd fluxdb\ncargo run --release -p fluxdb-server --bin fluxdb",
          language: "Terminal",
        },
        {
          title: "2. Open the browser console",
          text: "In a second terminal, from the repository root. Requires Node.js 20.19+ or 22.12+. Visit http://127.0.0.1:5173. The development proxy connects to your local server automatically.",
          code: "cd fluxdb-studio\nnpm ci\nnpm run dev",
          language: "Terminal",
        },
        {
          title: "3. Create a database",
          text: "Use Create database in the console, or call the API. Database names accept 1–64 ASCII letters, numbers, underscores, and hyphens. Remove the Authorization header only when token authentication is disabled.",
          code: curl("POST", root),
          language: "curl · Bash",
        },
        {
          title: "4. Write your first point",
          text: "A point is identified by measurement + complete tag set + timestamp. The API validates the entire batch before writing. The response reports the number of supplied points accepted.",
          code: curl("POST", `${root}/points`, { points: [point] }),
          language: "curl · Bash",
        },
        {
          title: "5. Query your data",
          code: curl("POST", `${root}/query`, {
            query: "SELECT * FROM cpu ORDER BY time DESC LIMIT 100",
          }),
          language: "curl · Bash",
        },
        {
          title: "Using Windows PowerShell",
          text: "The curl examples use Bash line continuation. In PowerShell, prefer Invoke-RestMethod or the JavaScript/Python clients below.",
          code: `$headers = @{ Authorization = "Bearer $env:FLUXDB_TOKEN" }\n$body = @{ query = 'SELECT * FROM cpu LIMIT 100' } | ConvertTo-Json\nInvoke-RestMethod -Method Post -Uri '${root}/query' -Headers $headers -ContentType 'application/json' -Body $body`,
          language: "PowerShell",
        },
      ],
    },
    {
      id: "concepts",
      title: "Data model & types",
      intro:
        "Understand the identity, precision, and update behavior of your time-series data before integrating an application.",
      blocks: [
        {
          title: "Databases, measurements, and series",
          text: "A database is an isolated namespace with its own WAL, memtable, SSTables, and retention policy. A measurement groups related observations (cpu, temperature). A series is a measurement plus its entire set of string tags. Tags describe dimensions; fields contain observed values.",
        },
        {
          title: "Point format",
          code: JSON.stringify(point, null, 2),
          language: "JSON",
        },
        {
          title: "Timestamp precision",
          text: "The versioned JSON API requires signed 64-bit Unix nanoseconds as a decimal string. JavaScript numbers cannot represent every nanosecond timestamp exactly. Use BigInt(Date.now()) * 1_000_000n, then .toString(). Read and export endpoints preserve the exact string. The chart axis uses milliseconds for display only.",
        },
        {
          title: "Field types",
          text: 'JSON numbers are stored as f64. Strings and booleans keep their types. For exact signed i64 values, send {"integer":"9223372036854775807"}. Null, arrays, nested application objects, and nonfinite numbers are rejected as field values. Empty field maps are rejected. Tags are string-to-string maps.',
        },
        {
          title: "Duplicate points are upserts",
          text: "Writing the same identity merges fields. Previously stored fields remain unless replaced by name. A new value replaces the matching field; repeating the same write is idempotent. Changing any tag creates a different series. Field removal is not a patch operation: delete and rewrite the point if necessary.",
        },
        {
          title: "Ordering and consistency",
          text: "Out-of-order timestamps are supported. Per-database writes and maintenance are serialized. Reads see a coherent state for that database. Each write batch is WAL-durable before it becomes visible. This is not a multi-statement or cross-database transaction system.",
        },
      ],
    },
    {
      id: "crud",
      title: "CRUD operations",
      intro:
        "Create, read, update, and delete points using the versioned JSON API. All ranges below are inclusive unless stated otherwise.",
      blocks: [
        {
          title: "Create points",
          code: curl("POST", `${root}/points`, { points: [point] }),
          language: "curl · Bash",
        },
        {
          title: "Read and paginate",
          text: "GET returns {total, offset, limit, points}. Results are ordered newest first, then by series. limit defaults to 100 and is clamped to 1–1,000. Offset pagination is not a snapshot across separate requests; use Export snapshot for a consistent whole-database view.",
          code: curl("GET", `${root}/points?measurement=cpu&limit=50&offset=0`),
          language: "curl · Bash",
        },
        {
          title: "Read a time range",
          code: curl(
            "GET",
            `${root}/points?measurement=cpu&start=1789142400000000000&end=1789228800000000000&limit=100`,
          ),
          language: "curl · Bash",
        },
        {
          title: "Update fields",
          text: "This replaces usage while preserving healthy and requests. It uses the exact same timestamp and complete tags as the original point.",
          code: curl("POST", `${root}/points`, {
            points: [{ ...point, fields: { usage: 51.2 } }],
          }),
          language: "curl · Bash",
        },
        {
          title: "Delete one exact point",
          text: "Set exact:true so tags must match the entire tag set. This avoids deleting another series that happens to have additional tags.",
          code: curl("DELETE", `${root}/points`, {
            measurement: "cpu",
            tags: point.tags,
            start: point.timestamp,
            end: point.timestamp,
            exact: true,
          }),
          language: "curl · Bash",
        },
        {
          title: "Delete a range",
          text: "Without exact:true, tag filters match a subset of the tags. Omitting tags matches every series in the specified measurement. Deletions are durable and permanent; the response contains {deleted:N}.",
          code: curl("DELETE", `${root}/points`, {
            measurement: "cpu",
            tags: { host: "api-01" },
            start: "0",
            end: "1789142400000000000",
          }),
          language: "curl · Bash",
        },
        {
          title: "Batch limits and retries",
          text: "The HTTP body limit is 2 MiB and batches accept 1–10,000 points. Split larger imports. Validation failures do not partially apply a batch. For ambiguous network or storage failures, read back before retrying; replaying identical identities is safe. The client libraries do not automatically retry destructive operations.",
        },
      ],
    },
    {
      id: "sql",
      title: "Query language",
      intro:
        "FluxDB implements a documented SQL subset for time-series reads. Mutations use the point API. Unsupported SQL is rejected instead of being presented as successful.",
      blocks: [
        {
          title: "Select, sort, and paginate",
          code: "SELECT * FROM cpu ORDER BY time DESC LIMIT 100 OFFSET 0;\nSELECT usage, host FROM cpu WHERE usage > 80 LIMIT 50;",
          language: "SQL · run one statement at a time",
        },
        {
          title: "Time filters",
          text: "Use integer nanoseconds or RFC3339 timestamps. The strict operators > and < exclude the boundary; >= and <= include it. The JSON query response serializes timestamp and i64 cells as decimal strings.",
          code: "SELECT * FROM cpu\nWHERE time >= '2026-09-11T00:00:00Z'\n  AND time < '2026-09-12T00:00:00Z'\nORDER BY time ASC;",
          language: "SQL",
        },
        {
          title: "Boolean and field filters",
          text: "AND, OR, NOT, numeric comparisons, exact integer comparisons, string comparisons, IN, BETWEEN, LIKE, and IS NULL are supported. LIKE uses % for any sequence and _ for one character. Missing fields follow SQL unknown semantics in comparisons.",
          code: "SELECT * FROM cpu\nWHERE (host = 'api-01' OR host = 'api-02')\n  AND healthy = true\n  AND usage BETWEEN 20 AND 80\nLIMIT 100;",
          language: "SQL",
        },
        {
          title: "Aggregates",
          text: "COUNT(*), COUNT(field), SUM, MEAN/AVG, MIN, MAX, FIRST, LAST, STDDEV, VARIANCE, and MEDIAN are supported. STDDEV and VARIANCE are population statistics. FIRST and LAST use timestamp order and the requested field. Empty COUNT returns 0; numeric aggregates over no values return null. Numeric aggregation uses floating point.",
          code: "SELECT COUNT(*), MEAN(usage) AS average_usage, MAX(usage) AS peak_usage FROM cpu;",
          language: "SQL",
        },
        {
          title: "Time windows and tags",
          text: "Quote the interval: time('1m'). Units: ns, us, ms, s, m, h, d. Intervals must be positive. Buckets are aligned to the Unix epoch, including negative timestamps. Group tag columns are included automatically; select aggregate expressions only. Empty buckets are omitted.",
          code: "SELECT MEAN(usage) AS mean_usage, MAX(usage) AS peak_usage\nFROM cpu\nGROUP BY time('1m'), host\nORDER BY time DESC\nLIMIT 100;",
          language: "SQL",
        },
        {
          title: "Response shape",
          code: JSON.stringify(
            {
              columns: ["time", "host", "mean_usage", "peak_usage"],
              rows: [["1789142400000000000", "api-01", 42.8, 51.2]],
              execution_time_ms: 0.42,
            },
            null,
            2,
          ),
          language: "Illustrative JSON response · timing varies",
        },
        {
          title: "Current language boundaries",
          text: "No Flux language, JOIN execution, subqueries, UNION, computed SELECT expressions, HAVING, field aliases, percentile arguments, FILL, SQL INSERT/UPDATE/DELETE, or multi-statement transactions. Aggregate aliases are supported. There is one ORDER BY key. Use the API for all data mutations and database management.",
        },
      ],
    },
    {
      id: "clients",
      title: "JavaScript & Python",
      intro:
        "Use fetch from JavaScript or the included dependency-free Python client. SDK files live under sdk/ in the repository.",
      blocks: [
        {
          title: "JavaScript / Node.js",
          text: "sdk/javascript/fluxdb.mjs exports FluxDBClient. Node.js 20+ includes fetch. Never embed an administrator token in a public application; keep it on your application server.",
          code: `import { FluxDBClient } from './sdk/javascript/fluxdb.mjs';\nconst client = new FluxDBClient('${base}', process.env.FLUXDB_TOKEN);\nawait client.createDatabase('${db}');\nawait client.write('${db}', [{\n  measurement: 'cpu',\n  tags: { host: 'api-01' },\n  timestamp: (BigInt(Date.now()) * 1_000_000n).toString(),\n  fields: { usage: 42.8, healthy: true }\n}]);\nconst data = await client.query('${db}', 'SELECT * FROM cpu LIMIT 10');\nconsole.log(data.columns, data.rows);`,
          language: "JavaScript",
        },
        {
          title: "Python",
          text: "sdk/python/fluxdb.py uses only the standard library. Add sdk/python to PYTHONPATH, or copy the client into your application.",
          code: `import os, time\nfrom fluxdb import FluxDBClient\n\nclient = FluxDBClient('${base}', os.environ.get('FLUXDB_TOKEN'))\nclient.create_database('${db}')\nclient.write('${db}', [{\n    'measurement': 'cpu',\n    'tags': {'host': 'api-01'},\n    'timestamp': str(time.time_ns()),\n    'fields': {'usage': 42.8, 'healthy': True},\n}])\nprint(client.query('${db}', 'SELECT MEAN(usage) FROM cpu'))`,
          language: "Python",
        },
        {
          title: "Plain fetch",
          code: `const response = await fetch('${root}/query', {\n  method: 'POST',\n  headers: {\n    'Content-Type': 'application/json',\n    Authorization: 'Bearer YOUR_TOKEN'\n  },\n  body: JSON.stringify({ query: 'SELECT COUNT(*) FROM cpu' })\n});\nconst result = await response.json();\nif (!response.ok) throw new Error(result.error);\nconsole.log(result);`,
          language: "JavaScript",
        },
        {
          title: "Client methods",
          text: "Both clients provide database listing/creation/deletion, write, read, query, delete, schema, retention, flush, compact, and export. A typed FluxDBError includes status for HTTP failures. Default request timeout is 30 seconds. Database names and query parameters are URL-encoded.",
        },
      ],
    },
    {
      id: "protocol",
      title: "Line protocol",
      intro:
        "Use the text ingestion endpoint for an Influx-style write format. This is a compatibility subset, not a full InfluxDB server implementation.",
      blocks: [
        {
          title: "Write format",
          text: "Each nonempty line contains measurement[,tags] fields [timestamp]. Comments starting with # are ignored. Escape spaces, commas, and equals in identifiers with a backslash. Quote string fields. An i suffix denotes a signed integer; booleans accept true/false and t/f. Unspecified timestamps use server time.",
          code: 'cpu,host=api-01,region=us-east-1 usage=42.8,requests=1200i,healthy=true 1789142400000000123\nlogs,host=api-01 message="hello, world" 1789142400000000123',
          language: "Line protocol",
        },
        {
          title: "Write a file",
          text: "precision accepts ns (default), us/u, ms, or s. Values are checked for timestamp overflow. A successful write returns HTTP 204. This legacy endpoint creates a database when absent; the versioned JSON API requires explicit database creation.",
          code: `curl -X POST '${base}/write?db=${encodeURIComponent(db)}&precision=ns' \\\n  -H 'Authorization: Bearer YOUR_TOKEN' \\\n  -H 'Content-Type: text/plain' \\\n  --data-binary @points.lp`,
          language: "curl · Bash",
        },
        {
          title: "Compatibility aliases",
          text: "POST /api/v2/write accepts db, database, or bucket as a database selector. POST /api/v2/query accepts {query,database} containing SQL, not Flux. Organizations, InfluxDB token scopes, unsigned integer line-protocol fields, tasks, and the full InfluxDB SDK protocol are not implemented.",
        },
      ],
    },
    {
      id: "operations",
      title: "Retention & maintenance",
      intro:
        "Manage the lifetime and disk representation of data independently for each database.",
      blocks: [
        {
          title: "Set retention",
          text: "Retention is persisted per database. Zero means unlimited; the maximum is 315,360,000 seconds. Once per minute, the server deletes points strictly older than now minus the retention interval, then compacts. Reducing retention can permanently remove existing data.",
          code: curl("PUT", `${root}/retention`, { seconds: 604800 }),
          language: "Keep seven days",
        },
        {
          title: "Read retention",
          code: curl("GET", `${root}/retention`),
          language: "curl · Bash",
        },
        {
          title: "Flush a memtable",
          text: "Flush publishes a typed checksummed SSTable. Acknowledged writes are already synchronized to the WAL by the server’s default configuration.",
          code: curl("POST", `${root}/flush`),
          language: "curl · Bash",
        },
        {
          title: "Compact and checkpoint",
          text: "Compaction merges live points into one sorted snapshot and atomically publishes a manifest with the first required WAL segment. Only then are old files reclaimed. It runs automatically at eight SSTables during writes, and can also be triggered explicitly. Full compaction temporarily blocks writes for that database.",
          code: curl("POST", `${root}/compact`),
          language: "curl · Bash",
        },
        {
          title: "Delete a database",
          text: "Permanently removes the namespace and stored data. The console requires its name as confirmation. If the database is in use, wait for requests to finish and retry.",
          code: curl("DELETE", root),
          language: "Destructive operation",
        },
      ],
    },
    {
      id: "backup",
      title: "Backup & restore",
      intro:
        "Export a consistent logical snapshot of a database, or take a filesystem backup while the server is stopped.",
      blocks: [
        {
          title: "Export snapshot",
          text: "The response includes format, database, retention_seconds, and all live points. It is consistent within that one database. The export is built in memory; use filesystem backups for large datasets.",
          code: `${curl("GET", `${root}/export`)} > snapshot.json`,
          language: "curl · Bash",
        },
        {
          title: "Restore into a new database",
          text: "The restore helper requires a new destination database to prevent accidental overwrites. It splits points into bounded batches, verifies the restored live-point count, and applies the saved retention policy last. Applying that policy may cause older restored points to expire.",
          code: `python scripts/restore.py snapshot.json --database restored_metrics --url '${base}'`,
          language: "Terminal",
        },
        {
          title: "Small imports in the console",
          text: "For snapshots with at most 10,000 points and under 2 MiB, choose Write data → Import JSON, review the destination, then Write points. Large snapshots should use the restore helper. The JSON import does not apply snapshot retention metadata automatically.",
        },
        {
          title: "Filesystem backup",
          text: "Stop the server gracefully, copy the complete data directory including manifests and WAL files, then restart. Restore the complete directory into an empty destination. Never copy a subset of SSTables or copy live files while maintenance is running. Keep a tested off-machine backup before upgrades.",
        },
      ],
    },
    {
      id: "reference",
      title: "API reference",
      intro:
        "All versioned endpoints use /api/v1. Except health, endpoints require Authorization: Bearer <token> when FLUXDB_TOKEN is set.",
      blocks: [
        {
          title: "Server endpoints",
          code: "GET    /api/v1/health         Status and version (public)\nGET    /api/v1/databases      Sorted database names\nGET    /api/v1/stats          Database and storage statistics\nGET    /api/v1/telemetry      Request samples and uptime\nGET    /api/v1/openapi.json   OpenAPI 3.1 description\nGET    /metrics              Prometheus storage gauges",
          language: "Endpoint index",
        },
        {
          title: "Database endpoints",
          code: "POST   /api/v1/databases/{name}            Create database → 201\nDELETE /api/v1/databases/{name}            Delete database → 204\nGET    /api/v1/databases/{name}/points     Read / paginate → 200\nPOST   /api/v1/databases/{name}/points     Write / upsert → 200\nDELETE /api/v1/databases/{name}/points     Delete range → 200\nPOST   /api/v1/databases/{name}/query      SQL query → 200\nGET    /api/v1/databases/{name}/schema     Inferred schema → 200\nGET    /api/v1/databases/{name}/export     Snapshot → 200\nGET    /api/v1/databases/{name}/retention  Read policy → 200\nPUT    /api/v1/databases/{name}/retention  Save policy → 200\nPOST   /api/v1/databases/{name}/flush      Flush → 204\nPOST   /api/v1/databases/{name}/compact    Checkpoint → 204",
          language: "Endpoint index",
        },
        {
          title: "Read parameters",
          text: "measurement: optional exact measurement name. start/end: optional inclusive decimal nanosecond timestamps. limit: default 100, range 1–1,000. offset: default 0. URL-encode all parameter values. Point-level tag filtering is available through SQL or the delete body.",
        },
        {
          title: "Error responses",
          text: "400: invalid values or unsupported SQL. 401: missing/invalid token. 404: missing database. 409: database already exists or is busy. 413: request exceeds 2 MiB. 422: malformed JSON shape. 429: concurrency limit reached; retry with backoff. 500: storage/internal error. Error responses contain an error message; validate HTTP status before consuming results. Do not treat legacy /query error fields as successful query results.",
        },
        {
          title: "Schema discovery",
          text: "Schema is inferred from current live points; fields are not declared in advance. Field type lists and tag values describe the data currently present. Empty measurements do not appear. Schema discovery performs a scan and is intended for exploration.",
        },
      ],
    },
    {
      id: "observability",
      title: "Monitoring",
      intro:
        "The overview uses measured server request durations and actual database statistics. It contains no fabricated latency or throughput values.",
      blocks: [
        {
          title: "Latency and errors",
          text: "P95 is the nearest-rank percentile of request duration samples in the selected database and window. The latency chart shows the peak per interval. Errors count status codes ≥400. Monitoring endpoints are excluded from their own samples. No samples means unavailable, displayed as a dash.",
        },
        {
          title: "Activity and scope",
          text: "Activity counts requests per chart interval, not points per second. The in-memory buffer holds the last 2,000 requests across the server and resets on restart. Thus a selected time window may contain only part of a busy interval. Database-scoped telemetry is collected from versioned database paths; legacy writes are server-level samples.",
        },
        {
          title: "Storage gauges",
          text: "Stored points counts deduplicated live identities, excluding deleted points. Memtable size is an in-memory estimate. SSTable count describes persisted tables. Exported Prometheus storage bytes count SSTable file bytes; they are not total filesystem usage, WAL size, process memory, CPU, or network utilization.",
          code: `curl '${base}/metrics' -H 'Authorization: Bearer YOUR_TOKEN'`,
          language: "Prometheus endpoint",
        },
        {
          title: "Demo and benchmarks",
          text: "The demo writes clearly tagged synthetic workload data to a separate demo database. Request durations remain real. The benchmark reports measured client throughput and latency for your machine; these results are not universal performance claims.",
          code: `python scripts/demo.py --url '${base}'\npython scripts/benchmark.py --url '${base}' --points 10000 --batch 500`,
          language: "Terminal",
        },
      ],
    },
    {
      id: "deploy",
      title: "Deployment & security",
      intro:
        "Run the server and browser frontend behind one HTTPS origin, with durable local storage and a configured access token.",
      blocks: [
        {
          title: "Docker Compose",
          text: "Create a root .env file containing a strong FLUXDB_TOKEN. Compose builds the Rust server and web frontend, uses a persistent volume, and publishes the web console on 127.0.0.1:8080. Add your own HTTPS reverse proxy before remote exposure.",
          code: "# .env\nFLUXDB_TOKEN=replace-with-a-long-random-token\n\n# terminal\ndocker compose up --build -d\n# open http://127.0.0.1:8080\n# Connect server URL: http://127.0.0.1:8080",
          language: "Configuration & terminal",
        },
        {
          title: "Environment variables",
          code: "FLUXDB_ADDR=127.0.0.1:8086\nFLUXDB_DATA_DIR=./data\nFLUXDB_TOKEN=<long-random-secret>\nFLUXDB_CORS_ORIGINS=https://console.example.com",
          language: "Server configuration",
        },
        {
          title: "Access model",
          text: "A token grants administrator access to the server. This version does not implement users, roles, organizations, per-database token scopes, or tenant isolation. Do not expose an unauthenticated server to the Internet. Connection tokens entered in Studio stay in memory and are lost on reload.",
        },
        {
          title: "Operational limits",
          text: "One process owns each data directory. Single-node only: no replication, failover, sharding, multi-statement transactions, continuous queries, or distributed consistency. SSTables are loaded into memory; query, schema, export, and compaction work can scale with dataset size. Size deployments conservatively and benchmark your intended workload.",
        },
        {
          title: "Upgrades and compatibility",
          text: "The new typed FLX2 SSTable format preserves field types. The reader accepts the legacy format, but values already discarded by an old flush cannot be reconstructed. Back up the data directory first. No downgrade compatibility is promised after writing new SSTables.",
        },
      ],
    },
    {
      id: "troubleshooting",
      title: "Troubleshooting",
      intro: "Resolve common connection, query, and storage issues.",
      blocks: [
        {
          title: "The console says Offline",
          text: "Confirm the server is running, its health endpoint responds, and your URL uses the right port. The development console uses a proxy at /api. For a remote server, configure its allowed browser origin. An HTTPS console cannot call an HTTP server because browsers block mixed content.",
        },
        {
          title: "Unauthorized",
          text: "Reconnect with the configured FLUXDB_TOKEN. The token is not saved between reloads. Health can succeed even when authenticated API calls fail.",
        },
        {
          title: "No rows after a write",
          text: "Verify the database and measurement, full tag set, timestamp units, and retention policy. Use SELECT * FROM measurement LIMIT 100, or the point API without a time filter. Writes with identical identity update existing points rather than adding new rows.",
        },
        {
          title: "Database already locked",
          text: "Stop the other server or offline CLI using that directory. Locks are released when the owning process exits; deleting .lock files while a process is running is unsafe and unnecessary.",
        },
        {
          title: "Corruption or storage failure",
          text: "Stop writes and preserve the data directory. The engine fails startup on detected checksum/manifest corruption instead of silently skipping data. A partial final WAL record can be repaired during recovery; a checksum mismatch cannot. Restore a verified backup and investigate the underlying disk problem.",
        },
        {
          title: "Query syntax error",
          text: "Run one SELECT at a time, use single quotes for strings and time intervals, and consult the supported SQL subset. For example GROUP BY time('1m'), not time(1m). Use the JSON point API for mutations.",
        },
      ],
    },
  ];
  const filtered = sections.filter((s) =>
    `${s.title} ${s.intro} ${s.blocks.map((b) => b.title + " " + b.text).join(" ")}`
      .toLowerCase()
      .includes(search.toLowerCase()),
  );
  const selected = sections.find((s) => s.id === topic)!;
  return (
    <div className="docs-layout">
      <aside className="docs-nav">
        <div className="docs-brand">
          <BookOpen size={19} />
          <b>Developer guide</b>
          <span>v0.1</span>
        </div>
        <label className="docs-search">
          <Search size={16} />
          <input
            aria-label="Search documentation"
            placeholder="Search documentation…"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
          />
        </label>
        <nav aria-label="Documentation topics">
          {filtered.map((s) => (
            <button
              key={s.id}
              className={topic === s.id ? "selected" : ""}
              onClick={() => setTopic(s.id)}
            >
              {s.title}
              {topic === s.id && <Chevron />}
            </button>
          ))}
          {filtered.length === 0 && <p>No matching topics.</p>}
        </nav>
      </aside>
      <article className="docs-content">
        <div className="docs-kicker">FLUXDB / DOCUMENTATION</div>
        <h2>{selected.title}</h2>
        <p className="docs-intro">{selected.intro}</p>
        <div className="docs-context">
          <span>
            Examples use <b>{db}</b>
          </span>
          <code>{base}</code>
        </div>
        {selected.blocks.map((b, i) => (
          <section key={`${topic}-${i}`} className="docs-section">
            <h3>{b.title}</h3>
            {b.text && <p>{b.text}</p>}
            {b.code && (
              <CodeBlock code={b.code} language={b.language || "Code"} />
            )}
          </section>
        ))}
        <div className="docs-next">
          <span>Explore more</span>
          {sections
            .filter((s) => s.id !== topic)
            .slice(
              sections.findIndex((s) => s.id === topic) % (sections.length - 1),
              (sections.findIndex((s) => s.id === topic) %
                (sections.length - 1)) +
                2,
            )
            .map((s) => (
              <button
                key={s.id}
                onClick={() => {
                  setTopic(s.id);
                  window.scrollTo({ top: 0 });
                }}
              >
                {s.title}
                <ArrowUpRight size={16} />
              </button>
            ))}
        </div>
      </article>
    </div>
  );
}
function Chevron() {
  return <span aria-hidden="true">›</span>;
}
function CodeBlock({ code, language }: { code: string; language: string }) {
  const [copied, setCopied] = useState(false);
  const [error, setError] = useState(false);
  return (
    <div className="docs-code">
      <div>
        <span>{language}</span>
        <button
          aria-label={`Copy ${language} example`}
          onClick={async () => {
            try {
              await navigator.clipboard.writeText(code);
              setCopied(true);
              setError(false);
            } catch {
              setError(true);
            }
          }}
        >
          {copied ? <Check size={14} /> : <Copy size={14} />}
          {copied ? "Copied" : "Copy"}
        </button>
      </div>
      <pre>
        <code>{code}</code>
      </pre>
      {error && (
        <p role="status">
          Copy unavailable. Select the example text to copy it.
        </p>
      )}
    </div>
  );
}
