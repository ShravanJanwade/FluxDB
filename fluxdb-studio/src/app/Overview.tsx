/** Project landing page: what is stored, what is wrong, and what to do next. */

import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import {
  ArrowRight,
  BarChart3,
  Bell,
  CheckCircle2,
  Database,
  KeyRound,
  Sparkles,
  Terminal,
  AlertTriangle,
} from "lucide-react";
import {
  Chart,
  EmptyState,
  PageHeader,
  RangePicker,
  Section,
  SkeletonRows,
  StatTile,
  UsageBar,
  useAction,
  useLoader,
} from "../components/ui";
import { api } from "../lib/api";
import { chartOption, toSeries, windowOf } from "../lib/charts";
import { bytes, count, decimal, relative } from "../lib/format";
import { DEFAULT_RANGE, resolveRange, type RangeKey } from "../lib/time";
import { useToast } from "../lib/toast";
import type { QueryResult } from "../lib/types";
import { useProject } from "./ProjectContext";
import { SourcePicker } from "./SourcePicker";

export default function Overview() {
  const { detail, reload, client, target } = useProject();
  const toast = useToast();
  const { run, isBusy } = useAction();
  const [range, setRange] = useState<RangeKey>(DEFAULT_RANGE);
  const project = detail.project;

  const totalPoints = detail.buckets.reduce(
    (sum, bucket) => sum + bucket.points,
    0,
  );
  const alerting = detail.monitors.filter(
    (monitor) => monitor.state === "alerting",
  );
  const emptyBucket = detail.buckets.find((bucket) => bucket.points === 0);

  const window = useMemo(() => resolveRange(range), [range]);

  // The overview chart is intentionally generic: it plots whichever numeric
  // measurement the selected bucket has the most of, so a project holding
  // anything at all shows something real.
  const preview = useLoader<{
    result: QueryResult;
    measurement: string;
  } | null>(
    async (signal) => {
      if (!client) return null;
      const schema = await client.schema(signal);
      const measurements = Object.entries(schema.measurements).sort(
        (a, b) => b[1].points - a[1].points,
      );
      if (measurements.length === 0) return null;
      const [measurement, info] = measurements[0];
      const field = Object.entries(info.fields).find(([, types]) =>
        types.some((type) => type === "float" || type === "integer"),
      )?.[0];
      if (!field) return null;
      const result = await client.query(
        `SELECT MEAN(${field}) AS ${field} FROM ${measurement} WHERE $timeFilter GROUP BY time($interval)`,
        { from: window.from, to: window.to },
        signal,
      );
      return { result, measurement: `${measurement}.${field}` };
    },
    [client ? JSON.stringify(client.target) : "none", window.from, window.to],
  );

  const shaped = toSeries(preview.data?.result ?? null);

  useEffect(() => {
    document.title = `${project.name} — FluxDB`;
  }, [project.name]);

  return (
    <>
      <PageHeader
        title={project.name}
        description={
          project.description ||
          "Buckets, dashboards, monitors and API keys for this project."
        }
        actions={
          <>
            <Link className="btn" to={`/app/p/${project.id}/query`}>
              <Terminal size={15} aria-hidden /> Query workspace
            </Link>
            {project.administrable && (
              <Link
                className="btn btn-primary"
                to={`/app/p/${project.id}/keys`}
              >
                <KeyRound size={15} aria-hidden /> Create an API key
              </Link>
            )}
          </>
        }
      />

      {project.demo && (
        <div className="stack" style={{ marginBottom: "var(--space-5)" }}>
          <div className="notice notice-info">
            <Sparkles size={16} aria-hidden />
            <div>
              <strong>This is the shared showcase project</strong>
              It holds a synthetic production fleet — eight hosts, six services,
              and a payment-dependency incident about 90 minutes back.
              Everything here is readable and nothing is writable. Your own
              workspace is in the switcher above.
            </div>
          </div>
        </div>
      )}

      {alerting.length > 0 && (
        <div className="stack" style={{ marginBottom: "var(--space-5)" }}>
          <div className="notice notice-danger">
            <AlertTriangle size={16} aria-hidden />
            <div>
              <strong>
                {alerting.length === 1
                  ? "1 monitor is alerting"
                  : `${alerting.length} monitors are alerting`}
              </strong>
              {alerting
                .slice(0, 3)
                .map(
                  (monitor) =>
                    `${monitor.name} at ${monitor.last_value === null ? "—" : decimal(monitor.last_value)} (threshold ${decimal(monitor.threshold)})`,
                )
                .join(" · ")}
            </div>
            <div className="notice-action">
              <Link className="btn btn-sm" to={`/app/p/${project.id}/monitors`}>
                Open monitors
              </Link>
            </div>
          </div>
        </div>
      )}

      <div className="grid grid-4">
        <StatTile
          label="Points stored"
          value={count(totalPoints)}
          hint={`Across ${detail.buckets.length} bucket${detail.buckets.length === 1 ? "" : "s"}`}
        />
        <StatTile
          label="On disk"
          value={bytes(detail.usage.size_bytes)}
          hint="SSTable files, excluding the memtable"
        />
        <StatTile
          label="Monitors"
          value={String(detail.monitors.length)}
          tone={alerting.length > 0 ? "danger" : "success"}
          hint={
            alerting.length > 0
              ? `${alerting.length} alerting`
              : detail.monitors.length > 0
                ? "All within threshold"
                : "None configured yet"
          }
        />
        <StatTile
          label="Active API keys"
          value={String(detail.keys.length)}
          hint={
            detail.keys.length > 0
              ? `Last used ${relative(Math.max(...detail.keys.map((key) => key.last_used_at ?? 0)))}`
              : "No keys issued"
          }
        />
      </div>

      {emptyBucket && project.writable && (
        <Section
          title="This bucket is empty"
          description="Load the sample fleet dataset to get working dashboards and queries immediately, or send your own data with an API key."
        >
          <div className="row">
            <button
              type="button"
              className="btn btn-primary"
              disabled={isBusy("sample")}
              onClick={() =>
                run("sample", async () => {
                  try {
                    const result = await api.loadSampleData(
                      project.id,
                      emptyBucket.id,
                    );
                    toast.success(
                      `Loaded ${count(result.written)} sample points into ${emptyBucket.name}`,
                    );
                    reload();
                    preview.reload();
                  } catch (error) {
                    toast.failure(error);
                  }
                })
              }
            >
              <Database size={15} aria-hidden /> Load sample data into{" "}
              {emptyBucket.name}
            </button>
            <Link className="btn" to={`/app/p/${project.id}/keys`}>
              <KeyRound size={15} aria-hidden /> Send my own data
            </Link>
            <span className="hint">
              The sample is explicitly synthetic and labelled as such wherever
              it appears.
            </span>
          </div>
        </Section>
      )}

      <Section
        title="Stored data"
        description={
          preview.data
            ? `Mean ${preview.data.measurement} over the selected range, from the busiest measurement in this source.`
            : "A chart appears here once the selected source holds numeric data."
        }
        actions={
          <>
            <SourcePicker />
            <RangePicker value={range} onChange={setRange} />
          </>
        }
      >
        {preview.loading && !preview.data ? (
          <SkeletonRows rows={5} />
        ) : preview.error ? (
          <EmptyState
            icon={<AlertTriangle size={20} aria-hidden />}
            title="That source could not be read"
            description={preview.error}
            action={
              <button type="button" className="btn" onClick={preview.reload}>
                Try again
              </button>
            }
          />
        ) : shaped.series.length === 0 ? (
          <EmptyState
            icon={<BarChart3 size={20} aria-hidden />}
            title="Nothing stored in this range"
            description={
              target
                ? `No numeric measurements in ${target.label} for the selected window. Widen the range, or write some points.`
                : "Select a data source."
            }
            action={
              <Link className="btn" to={`/app/p/${project.id}/explorer`}>
                Open the data explorer <ArrowRight size={15} aria-hidden />
              </Link>
            }
          />
        ) : (
          <Chart
            option={chartOption(shaped, "area", "", {
              legend: false,
              window: windowOf(preview.data?.result),
            })}
            height={260}
          />
        )}
      </Section>

      <div className="grid grid-2">
        <Section
          title="Buckets"
          description="Each bucket is a separate time-series database."
          actions={
            <Link className="btn btn-sm" to={`/app/p/${project.id}/buckets`}>
              Manage
            </Link>
          }
        >
          {detail.buckets.length === 0 ? (
            <EmptyState
              icon={<Database size={20} aria-hidden />}
              title="No buckets yet"
              description="Create one to start storing points."
              action={
                <Link
                  className="btn btn-primary"
                  to={`/app/p/${project.id}/buckets`}
                >
                  Create a bucket
                </Link>
              }
            />
          ) : (
            <div className="stack">
              {detail.buckets.map((bucket) => (
                <div key={bucket.id} className="row">
                  <Database size={15} aria-hidden className="cell-muted" />
                  <strong>{bucket.name}</strong>
                  <span className="cell-muted">
                    {count(bucket.points)} points
                  </span>
                  <span className="row-end cell-muted mono">
                    {bytes(bucket.size_bytes)}
                  </span>
                </div>
              ))}
              <UsageBar
                label="Points against the hosted limit"
                used={detail.usage.points}
                limit={detail.usage.points_limit}
                format={count}
              />
            </div>
          )}
        </Section>

        <Section
          title="Recent alerts"
          description="State changes recorded by this project's monitors."
          actions={
            <Link className="btn btn-sm" to={`/app/p/${project.id}/monitors`}>
              <Bell size={14} aria-hidden /> Monitors
            </Link>
          }
        >
          {detail.alerts.length === 0 ? (
            <EmptyState
              icon={<CheckCircle2 size={20} aria-hidden />}
              title="No alerts recorded"
              description={
                detail.monitors.length === 0
                  ? "Add a monitor and FluxDB will evaluate it every minute against a real query."
                  : "Every monitor has stayed within its threshold since it was created."
              }
            />
          ) : (
            <div className="timeline">
              {detail.alerts.slice(0, 8).map((alert) => (
                <div key={alert.id} className="timeline-item">
                  <span className="timeline-icon">
                    {alert.state === "alerting" ? (
                      <AlertTriangle size={13} aria-hidden />
                    ) : (
                      <CheckCircle2 size={13} aria-hidden />
                    )}
                  </span>
                  <span className="timeline-body">
                    <strong>{alert.monitor_name}</strong>
                    <small>{alert.message}</small>
                  </span>
                  <span className="timeline-time">{relative(alert.at)}</span>
                </div>
              ))}
            </div>
          )}
        </Section>
      </div>
    </>
  );
}
