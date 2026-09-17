/**
 * Threshold monitors and the alert feed.
 *
 * The server evaluates each enabled monitor once a minute and records only the
 * transitions, so a monitor that stays in breach produces one entry rather than
 * one per sweep. A monitor's query is validated by running it once when the
 * monitor is created, so it cannot be saved in a state where it silently never
 * evaluates.
 */

import { useState } from "react";
import {
  Bell,
  BellOff,
  CheckCircle2,
  Plus,
  Trash2,
  AlertTriangle,
} from "lucide-react";
import {
  ConfirmDialog,
  DataTable,
  EmptyState,
  Modal,
  Notice,
  PageHeader,
  Section,
  useAction,
  type Column,
} from "../components/ui";
import { api, type MonitorInput } from "../lib/api";
import { decimal, relative, timestamp } from "../lib/format";
import { useToast } from "../lib/toast";
import type { Comparison, Monitor, Severity } from "../lib/types";
import { useProject } from "./ProjectContext";

const SEVERITY_TONE: Record<Severity, string> = {
  critical: "badge-danger",
  warning: "badge-warning",
  info: "badge-info",
};

export default function Monitors() {
  const { detail, reload } = useProject();
  const toast = useToast();
  const { run, isBusy } = useAction();
  const [creating, setCreating] = useState(false);
  const [deleting, setDeleting] = useState<Monitor | null>(null);

  const alerting = detail.monitors.filter(
    (monitor) => monitor.state === "alerting",
  );

  const columns: Column<Monitor>[] = [
    {
      key: "state",
      header: "State",
      width: "120px",
      render: (monitor) => (
        <span className="monitor-state">
          <span className={`dot dot-${monitor.state}`} aria-hidden />
          {monitor.state === "alerting"
            ? "Alerting"
            : monitor.state === "ok"
              ? "OK"
              : "Not evaluated"}
        </span>
      ),
    },
    {
      key: "name",
      header: "Monitor",
      render: (monitor) => (
        <div>
          <strong>{monitor.name}</strong>
          <div
            className="cell-muted mono"
            style={{ fontSize: "var(--text-xs)" }}
          >
            {monitor.query.length > 74
              ? `${monitor.query.slice(0, 74)}…`
              : monitor.query}
          </div>
        </div>
      ),
    },
    {
      key: "threshold",
      header: "Condition",
      render: (monitor) => (
        <span className="mono">
          {monitor.comparison === "above" ? ">" : "<"}{" "}
          {decimal(monitor.threshold)}
        </span>
      ),
    },
    {
      key: "value",
      header: "Last value",
      align: "right",
      render: (monitor) =>
        monitor.last_value === null ? (
          <span className="cell-muted">—</span>
        ) : (
          <span className="mono">{decimal(monitor.last_value)}</span>
        ),
    },
    {
      key: "severity",
      header: "Severity",
      secondary: true,
      render: (monitor) => (
        <span className={`badge ${SEVERITY_TONE[monitor.severity]}`}>
          {monitor.severity}
        </span>
      ),
    },
    {
      key: "checked",
      header: "Checked",
      secondary: true,
      render: (monitor) => (
        <span
          className="cell-muted"
          title={
            monitor.last_checked_at
              ? timestamp(monitor.last_checked_at)
              : undefined
          }
        >
          {monitor.last_checked_at
            ? relative(monitor.last_checked_at)
            : "never"}
        </span>
      ),
    },
    {
      key: "actions",
      header: "",
      align: "right",
      width: "150px",
      render: (monitor) =>
        detail.project.writable ? (
          <div className="cell-actions">
            <button
              type="button"
              className="btn btn-sm"
              disabled={isBusy(`toggle-${monitor.id}`)}
              onClick={() =>
                run(`toggle-${monitor.id}`, async () => {
                  try {
                    await api.updateMonitor(detail.project.id, monitor.id, {
                      enabled: !monitor.enabled,
                    });
                    toast.success(
                      monitor.enabled ? "Monitor paused" : "Monitor enabled",
                    );
                    reload();
                  } catch (error) {
                    toast.failure(error);
                  }
                })
              }
            >
              {monitor.enabled ? (
                <>
                  <BellOff size={13} aria-hidden /> Pause
                </>
              ) : (
                <>
                  <Bell size={13} aria-hidden /> Enable
                </>
              )}
            </button>
            <button
              type="button"
              className="btn btn-sm btn-ghost"
              aria-label={`Delete ${monitor.name}`}
              onClick={() => setDeleting(monitor)}
            >
              <Trash2 size={14} aria-hidden />
            </button>
          </div>
        ) : null,
    },
  ];

  return (
    <>
      <PageHeader
        title="Monitors & alerts"
        description="A monitor runs one query every minute and compares the first numeric cell against a threshold."
        actions={
          detail.project.writable && (
            <button
              type="button"
              className="btn btn-primary"
              onClick={() => setCreating(true)}
              disabled={detail.buckets.length === 0}
              title={
                detail.buckets.length === 0
                  ? "Create a bucket before adding a monitor."
                  : undefined
              }
            >
              <Plus size={15} aria-hidden /> New monitor
            </button>
          )
        }
      />

      {alerting.length > 0 && (
        <div style={{ marginBottom: "var(--space-5)" }}>
          <Notice tone="danger" title={`${alerting.length} alerting`}>
            {alerting
              .map(
                (monitor) =>
                  `${monitor.name}: ${monitor.last_value === null ? "—" : decimal(monitor.last_value)} ${
                    monitor.comparison === "above" ? "above" : "below"
                  } ${decimal(monitor.threshold)}`,
              )
              .join(" · ")}
          </Notice>
        </div>
      )}

      <Section compact>
        <DataTable
          columns={columns}
          rows={detail.monitors}
          rowKey={(monitor) => monitor.id}
          empty={
            <EmptyState
              icon={<Bell size={20} aria-hidden />}
              title="No monitors in this project"
              description="A monitor turns stored points into something that notices a problem. For example: MAX(latency_p99) above 750, evaluated every minute."
              action={
                detail.project.writable &&
                detail.buckets.length > 0 && (
                  <button
                    type="button"
                    className="btn btn-primary"
                    onClick={() => setCreating(true)}
                  >
                    Add your first monitor
                  </button>
                )
              }
            />
          }
        />
      </Section>

      {detail.monitors.some((monitor) => monitor.last_error) && (
        <Section title="Evaluation problems">
          <div className="stack">
            {detail.monitors
              .filter((monitor) => monitor.last_error)
              .map((monitor) => (
                <Notice key={monitor.id} tone="warning" title={monitor.name}>
                  {monitor.last_error}
                </Notice>
              ))}
          </div>
        </Section>
      )}

      <Section
        title="Alert history"
        description="State transitions only. A monitor that stays in breach appears once, not once per minute."
      >
        {detail.alerts.length === 0 ? (
          <EmptyState
            icon={<CheckCircle2 size={20} aria-hidden />}
            title="Nothing recorded"
            description="Alerts appear here the first time a monitor crosses or returns inside its threshold."
          />
        ) : (
          <div className="timeline">
            {detail.alerts.map((alert) => (
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
                <span className="timeline-time" title={timestamp(alert.at)}>
                  {relative(alert.at)}
                </span>
              </div>
            ))}
          </div>
        )}
      </Section>

      <Section title="How evaluation works">
        <dl className="kv">
          <dt>Interval</dt>
          <dd>
            Every 60 seconds, in one background sweep shared by all monitors on
            the instance.
          </dd>
          <dt>Value</dt>
          <dd>
            The first numeric cell of the first row. A query returning no rows
            leaves the monitor in <strong>not evaluated</strong> rather than
            treating absence as zero.
          </dd>
          <dt>Time bounds</dt>
          <dd>
            A monitor query runs without a range unless you write one into the
            SQL, so it sees the whole bucket. Add an explicit{" "}
            <code>time &gt;=</code> predicate to narrow it.
          </dd>
          <dt>Delivery</dt>
          <dd>
            Alerts are recorded and shown here. This deployment has no mail or
            webhook delivery — that would be the next thing to build.
          </dd>
        </dl>
      </Section>

      {creating && (
        <MonitorDialog
          buckets={detail.buckets}
          onClose={() => setCreating(false)}
          onSave={async (input) => {
            await api.createMonitor(detail.project.id, input);
            setCreating(false);
            reload();
            toast.success("Monitor created and queued for evaluation");
          }}
        />
      )}

      {deleting && (
        <ConfirmDialog
          title={`Delete ${deleting.name}?`}
          confirmLabel="Delete monitor"
          busy={isBusy("delete-monitor")}
          description="The monitor and its recorded alerts are removed. No stored points are affected."
          onClose={() => setDeleting(null)}
          onConfirm={() =>
            run("delete-monitor", async () => {
              try {
                await api.deleteMonitor(detail.project.id, deleting.id);
                setDeleting(null);
                reload();
                toast.success("Monitor deleted");
              } catch (error) {
                toast.failure(error);
              }
            })
          }
        />
      )}
    </>
  );
}

const TEMPLATES: {
  label: string;
  query: string;
  comparison: Comparison;
  threshold: number;
}[] = [
  {
    label: "Tail latency budget",
    query: "SELECT MAX(latency_p99) AS p99 FROM http_requests",
    comparison: "above",
    threshold: 750,
  },
  {
    label: "Error rate SLO",
    query: "SELECT MEAN(error_rate) AS error_rate FROM http_requests",
    comparison: "above",
    threshold: 1,
  },
  {
    label: "CPU saturation",
    query: "SELECT MAX(usage) AS cpu FROM cpu",
    comparison: "above",
    threshold: 90,
  },
  {
    label: "Ingestion stopped",
    query: "SELECT COUNT(usage) AS points FROM cpu",
    comparison: "below",
    threshold: 1,
  },
];

function MonitorDialog({
  buckets,
  onClose,
  onSave,
}: {
  buckets: { id: string; name: string }[];
  onClose: () => void;
  onSave: (input: MonitorInput) => Promise<void>;
}) {
  const [name, setName] = useState("");
  const [bucketId, setBucketId] = useState(buckets[0]?.id ?? "");
  const [query, setQuery] = useState(TEMPLATES[0].query);
  const [comparison, setComparison] = useState<Comparison>("above");
  const [threshold, setThreshold] = useState("750");
  const [severity, setSeverity] = useState<Severity>("critical");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const numericThreshold = Number(threshold);
  const valid =
    name.trim().length > 0 &&
    query.trim().length > 0 &&
    Number.isFinite(numericThreshold) &&
    bucketId.length > 0;

  return (
    <Modal
      title="New monitor"
      description="The query is run once now to check it produces a number. If it does not, the monitor is not created."
      onClose={onClose}
      width={600}
      footer={
        <>
          <button
            type="button"
            className="btn"
            onClick={onClose}
            disabled={busy}
          >
            Cancel
          </button>
          <button
            type="button"
            className="btn btn-primary"
            disabled={busy || !valid}
            onClick={async () => {
              setBusy(true);
              setError(null);
              try {
                await onSave({
                  name: name.trim(),
                  bucket_id: bucketId,
                  query: query.trim(),
                  comparison,
                  threshold: numericThreshold,
                  severity,
                });
              } catch (cause) {
                setError(
                  cause instanceof Error ? cause.message : "That did not work",
                );
              } finally {
                setBusy(false);
              }
            }}
          >
            Create monitor
          </button>
        </>
      }
    >
      <div className="stack">
        {error && <div className="form-error">{error}</div>}
        <div>
          <span className="label" style={{ display: "block", marginBottom: 6 }}>
            Start from a template
          </span>
          <div className="chip-list">
            {TEMPLATES.map((template) => (
              <button
                key={template.label}
                type="button"
                className="chip"
                onClick={() => {
                  setName(template.label);
                  setQuery(template.query);
                  setComparison(template.comparison);
                  setThreshold(String(template.threshold));
                }}
              >
                {template.label}
              </button>
            ))}
          </div>
        </div>
        <label className="field">
          <span className="label">Name</span>
          <input
            className="input"
            value={name}
            onChange={(event) => setName(event.target.value)}
            maxLength={64}
            placeholder="Tail latency budget"
          />
        </label>
        <label className="field">
          <span className="label">Bucket</span>
          <select
            className="select"
            value={bucketId}
            onChange={(event) => setBucketId(event.target.value)}
          >
            {buckets.map((bucket) => (
              <option key={bucket.id} value={bucket.id}>
                {bucket.name}
              </option>
            ))}
          </select>
        </label>
        <label className="field">
          <span className="label">Query</span>
          <div className="editor">
            <textarea
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              rows={3}
              spellCheck={false}
              aria-label="Monitor query"
            />
            <div className="editor-foot">
              <span>Must return at least one numeric column</span>
            </div>
          </div>
        </label>
        <div className="row">
          <label className="field" style={{ flex: 1 }}>
            <span className="label">Alert when the value is</span>
            <select
              className="select"
              value={comparison}
              onChange={(event) =>
                setComparison(event.target.value as Comparison)
              }
            >
              <option value="above">Above the threshold</option>
              <option value="below">Below the threshold</option>
            </select>
          </label>
          <label className="field" style={{ flex: 1 }}>
            <span className="label">Threshold</span>
            <input
              className="input"
              type="number"
              step="any"
              value={threshold}
              onChange={(event) => setThreshold(event.target.value)}
              aria-invalid={!Number.isFinite(numericThreshold)}
            />
          </label>
          <label className="field" style={{ flex: 1 }}>
            <span className="label">Severity</span>
            <select
              className="select"
              value={severity}
              onChange={(event) => setSeverity(event.target.value as Severity)}
            >
              <option value="critical">Critical</option>
              <option value="warning">Warning</option>
              <option value="info">Info</option>
            </select>
          </label>
        </div>
      </div>
    </Modal>
  );
}
