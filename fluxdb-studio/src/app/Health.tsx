/**
 * Instance health from the server's own request history.
 *
 * The history is a bounded ring of the last 2,000 application requests, held in
 * memory and reset on restart. It measures server processing time only — not
 * network latency, not CPU utilisation, not disk IOPS — and the page says so,
 * because a latency chart with an unstated denominator is worse than none.
 */

import { useMemo, useState } from "react";
import { Activity, Gauge, RefreshCw } from "lucide-react";
import {
  Chart,
  DataTable,
  EmptyState,
  Notice,
  PageHeader,
  Section,
  Spinner,
  StatTile,
  Tabs,
  useLoader,
  type Column,
} from "../components/ui";
import { api } from "../lib/api";
import { bucketTelemetry, latencyOption, requestsOption } from "../lib/charts";
import { count, decimal, milliseconds, uptime } from "../lib/format";

const RANGES = [
  { minutes: 15, label: "Last 15 minutes" },
  { minutes: 60, label: "Last hour" },
  { minutes: 180, label: "Last 3 hours" },
];

export default function Health() {
  const [minutes, setMinutes] = useState(60);
  const [view, setView] = useState<"latency" | "requests">("latency");
  const [nonce, setNonce] = useState(0);

  const telemetry = useLoader((signal) => api.telemetry(signal), [nonce]);

  const samples = telemetry.data?.samples ?? [];
  const buckets = useMemo(
    () => bucketTelemetry(samples, minutes),
    [samples, minutes],
  );

  const inWindow = useMemo(() => {
    const cutoff = Date.now() - minutes * 60_000;
    return samples.filter((sample) => sample.time >= cutoff);
  }, [samples, minutes]);

  const durations = useMemo(
    () =>
      [...inWindow.map((sample) => sample.duration_ms)].sort((a, b) => a - b),
    [inWindow],
  );
  const quantile = (fraction: number) =>
    durations.length === 0
      ? 0
      : durations[
          Math.min(
            durations.length - 1,
            Math.floor(fraction * durations.length),
          )
        ];
  const failures = inWindow.filter((sample) => sample.status >= 400).length;

  const byOperation = useMemo(() => {
    const groups = new Map<
      string,
      {
        operation: string;
        calls: number;
        total: number;
        worst: number;
        failed: number;
      }
    >();
    for (const sample of inWindow) {
      // Path parameters are collapsed so a route shows as one row rather than
      // one row per id.
      const operation = sample.operation
        .replace(/\/(acc|gst|org|bkt|dsh|mon|cnx|inv)_[A-Za-z0-9_-]+/g, "/:id")
        .replace(/\/[0-9a-f]{16,}/g, "/:id");
      const entry = groups.get(operation) ?? {
        operation,
        calls: 0,
        total: 0,
        worst: 0,
        failed: 0,
      };
      entry.calls += 1;
      entry.total += sample.duration_ms;
      entry.worst = Math.max(entry.worst, sample.duration_ms);
      if (sample.status >= 400) entry.failed += 1;
      groups.set(operation, entry);
    }
    return [...groups.values()].sort((a, b) => b.calls - a.calls);
  }, [inWindow]);

  const columns: Column<(typeof byOperation)[number]>[] = [
    {
      key: "operation",
      header: "Operation",
      render: (row) => <span className="mono">{row.operation}</span>,
    },
    {
      key: "calls",
      header: "Calls",
      align: "right",
      render: (row) => <span className="mono">{count(row.calls)}</span>,
    },
    {
      key: "mean",
      header: "Mean",
      align: "right",
      render: (row) => (
        <span className="mono">{milliseconds(row.total / row.calls)}</span>
      ),
    },
    {
      key: "worst",
      header: "Slowest",
      align: "right",
      render: (row) => <span className="mono">{milliseconds(row.worst)}</span>,
    },
    {
      key: "failed",
      header: "Failed",
      align: "right",
      render: (row) =>
        row.failed === 0 ? (
          <span className="cell-muted">0</span>
        ) : (
          <span className="badge badge-danger">{row.failed}</span>
        ),
    },
  ];

  if (telemetry.loading && !telemetry.data) {
    return (
      <>
        <PageHeader title="Instance health" />
        <Section>
          <Spinner label="Reading the request history…" />
        </Section>
      </>
    );
  }

  return (
    <>
      <PageHeader
        title="Instance health"
        description="Measured request latency and errors for this FluxDB instance."
        actions={
          <>
            <label className="source-picker">
              <span className="visually-hidden">Window</span>
              <select
                className="select"
                value={minutes}
                onChange={(event) => setMinutes(Number(event.target.value))}
              >
                {RANGES.map((range) => (
                  <option key={range.minutes} value={range.minutes}>
                    {range.label}
                  </option>
                ))}
              </select>
            </label>
            <button
              type="button"
              className="btn btn-icon"
              aria-label="Refresh"
              onClick={() => setNonce((current) => current + 1)}
            >
              <RefreshCw size={15} aria-hidden />
            </button>
          </>
        }
      />

      <div className="grid grid-4">
        <StatTile
          label="Requests in window"
          value={count(inWindow.length)}
          hint={`${count(samples.length)} of ${count(telemetry.data?.capacity ?? 2000)} in the ring`}
        />
        <StatTile
          label="p50"
          value={milliseconds(quantile(0.5))}
          hint="Server processing time"
        />
        <StatTile
          label="p95"
          value={milliseconds(quantile(0.95))}
          tone={quantile(0.95) > 250 ? "warning" : "default"}
          hint={`slowest ${milliseconds(durations[durations.length - 1] ?? 0)}`}
        />
        <StatTile
          label="Failed"
          value={
            inWindow.length === 0
              ? "—"
              : `${decimal((failures / inWindow.length) * 100)}%`
          }
          tone={failures > 0 ? "danger" : "success"}
          hint={`${failures} responses of 4xx or 5xx`}
        />
      </div>

      <Section
        title={view === "latency" ? "Latency" : "Requests"}
        description={
          view === "latency"
            ? "p50, p95 and the slowest request per bucket."
            : "Successful against failed responses per bucket."
        }
        actions={
          <Tabs
            value={view}
            onChange={setView}
            tabs={[
              { id: "latency" as const, label: "Latency" },
              { id: "requests" as const, label: "Requests" },
            ]}
          />
        }
      >
        {inWindow.length === 0 ? (
          <EmptyState
            icon={<Activity size={20} aria-hidden />}
            title="No requests recorded in this window"
            description="Run a query or open the explorer, then come back. Polling endpoints are deliberately excluded so this chart describes real data work."
          />
        ) : (
          <Chart
            option={
              view === "latency"
                ? latencyOption(buckets)
                : requestsOption(buckets)
            }
            height={300}
          />
        )}
      </Section>

      <Section compact title="By operation" description="Collapsed by route.">
        <DataTable
          columns={columns}
          rows={byOperation.slice(0, 25)}
          rowKey={(row) => row.operation}
          dense
          empty={
            <EmptyState
              icon={<Gauge size={20} aria-hidden />}
              title="Nothing to break down yet"
              description="Operations appear once requests have been recorded in the selected window."
            />
          }
        />
      </Section>

      <Section title="What this does and does not measure">
        <div className="stack">
          <dl className="kv">
            <dt>Scope</dt>
            <dd>
              Time spent inside this server handling a request. It excludes
              network latency between your browser and the server, and it says
              nothing about CPU utilisation or disk IOPS.
            </dd>
            <dt>Retention</dt>
            <dd>
              The last {count(telemetry.data?.capacity ?? 2000)} requests, in
              memory. Restarting the server clears it; this is not long-term
              monitoring.
            </dd>
            <dt>Exclusions</dt>
            <dd>
              Health checks, this telemetry endpoint, stats polling and the
              session endpoint are not recorded, so the console's own refresh
              loop cannot flatter the numbers.
            </dd>
            <dt>Uptime</dt>
            <dd>
              {uptime(telemetry.data?.uptime_seconds ?? 0)} since the process
              started
              {telemetry.data
                ? `, authentication ${telemetry.data.authentication_enabled ? "enabled" : "disabled"} on the token API`
                : ""}
              .
            </dd>
          </dl>
          <Notice tone="info">
            For a Prometheus scrape of storage-level gauges, the server also
            exposes <code>/metrics</code> — database count, total points and
            total bytes.
          </Notice>
        </div>
      </Section>
    </>
  );
}
