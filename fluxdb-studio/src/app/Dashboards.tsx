/**
 * Saved dashboards.
 *
 * A panel stores its SQL with the macros unexpanded, so the same panel serves
 * every range the viewer picks. Each panel runs its own query; they are issued
 * together and rendered as they land, so one slow panel does not block the rest.
 */

import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import {
  BarChart3,
  LayoutDashboard,
  Pencil,
  Plus,
  RefreshCw,
  Trash2,
} from "lucide-react";
import {
  Chart,
  ConfirmDialog,
  DataTable,
  EmptyState,
  Modal,
  Notice,
  PageHeader,
  RangePicker,
  Section,
  SkeletonRows,
  Spinner,
  useAction,
  useLoader,
} from "../components/ui";
import { api, dataClient, type PanelInput } from "../lib/api";
import {
  chartOption,
  sparklineOption,
  statValue,
  toSeries,
  windowOf,
} from "../lib/charts";
import { cellText, decimal } from "../lib/format";
import { DEFAULT_RANGE, resolveRange, type RangeKey } from "../lib/time";
import { useToast } from "../lib/toast";
import type { Dashboard, Panel, PanelKind, QueryResult } from "../lib/types";
import { useProject } from "./ProjectContext";

export default function Dashboards() {
  const { detail, reload } = useProject();
  const toast = useToast();
  const { run, isBusy } = useAction();
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [range, setRange] = useState<RangeKey>(DEFAULT_RANGE);
  const [revision, setRevision] = useState(0);
  const [creating, setCreating] = useState(false);
  const [editingPanel, setEditingPanel] = useState<Panel | "new" | null>(null);
  const [deleting, setDeleting] = useState<Dashboard | null>(null);

  const dashboards = useLoader(
    () => api.dashboards(detail.project.id),
    [detail.project.id, revision],
  );

  const list = dashboards.data?.dashboards ?? [];
  const dashboard = useMemo(
    () => list.find((entry) => entry.id === selectedId) ?? list[0] ?? null,
    [list, selectedId],
  );

  useEffect(() => {
    if (dashboard && dashboard.id !== selectedId) setSelectedId(dashboard.id);
  }, [dashboard, selectedId]);

  const window = useMemo(() => resolveRange(range), [range, revision]);

  async function savePanels(next: PanelInput[]) {
    if (!dashboard) return;
    await api.saveDashboard(
      detail.project.id,
      dashboard.id,
      dashboard.name,
      next,
    );
    setRevision((current) => current + 1);
  }

  const asInput = (panel: Panel): PanelInput => ({
    title: panel.title,
    kind: panel.kind,
    bucket_id: panel.bucket_id,
    query: panel.query,
    unit: panel.unit,
    span: panel.span,
  });

  if (dashboards.loading && !dashboards.data) {
    return (
      <>
        <PageHeader title="Dashboards" />
        <Section>
          <Spinner label="Loading dashboards…" />
        </Section>
      </>
    );
  }

  return (
    <>
      <PageHeader
        title="Dashboards"
        description="Panels are stored queries. Pick a range and every panel re-resolves against it."
        actions={
          <>
            {list.length > 1 && (
              <label className="source-picker">
                <span className="visually-hidden">Dashboard</span>
                <select
                  className="select"
                  value={dashboard?.id ?? ""}
                  onChange={(event) => setSelectedId(event.target.value)}
                >
                  {list.map((entry) => (
                    <option key={entry.id} value={entry.id}>
                      {entry.name}
                    </option>
                  ))}
                </select>
              </label>
            )}
            <RangePicker value={range} onChange={setRange} />
            <button
              type="button"
              className="btn btn-icon"
              aria-label="Refresh panels"
              onClick={() => setRevision((current) => current + 1)}
            >
              <RefreshCw size={15} aria-hidden />
            </button>
            {detail.project.writable && (
              <>
                {dashboard && (
                  <button
                    type="button"
                    className="btn"
                    onClick={() => setEditingPanel("new")}
                  >
                    <Plus size={15} aria-hidden /> Add panel
                  </button>
                )}
                <button
                  type="button"
                  className="btn btn-primary"
                  onClick={() => setCreating(true)}
                >
                  <LayoutDashboard size={15} aria-hidden /> New dashboard
                </button>
              </>
            )}
          </>
        }
      />

      {!dashboard ? (
        <Section>
          <EmptyState
            icon={<BarChart3 size={20} aria-hidden />}
            title="No dashboards yet"
            description={
              detail.project.writable
                ? "Build one from the query workspace — run a query, then save it as a panel — or create an empty dashboard here."
                : "This project has no dashboards, and your role does not allow creating them."
            }
            action={
              detail.project.writable && (
                <>
                  <Link
                    className="btn"
                    to={`/app/p/${detail.project.id}/query`}
                  >
                    Open the query workspace
                  </Link>
                  <button
                    type="button"
                    className="btn btn-primary"
                    onClick={() => setCreating(true)}
                  >
                    New dashboard
                  </button>
                </>
              )
            }
          />
        </Section>
      ) : (
        <>
          <div className="row" style={{ marginBottom: "var(--space-4)" }}>
            <h2 style={{ fontSize: "var(--text-md)" }}>{dashboard.name}</h2>
            <span className="cell-muted">
              {dashboard.panels.length} panel
              {dashboard.panels.length === 1 ? "" : "s"}
            </span>
            {detail.project.writable && (
              <button
                type="button"
                className="btn btn-sm btn-ghost row-end"
                onClick={() => setDeleting(dashboard)}
              >
                <Trash2 size={14} aria-hidden /> Delete dashboard
              </button>
            )}
          </div>

          {dashboard.panels.length === 0 ? (
            <Section>
              <EmptyState
                icon={<Plus size={20} aria-hidden />}
                title="This dashboard is empty"
                description="Add a panel with its own SQL, or save one from the query workspace."
                action={
                  detail.project.writable && (
                    <button
                      type="button"
                      className="btn btn-primary"
                      onClick={() => setEditingPanel("new")}
                    >
                      Add a panel
                    </button>
                  )
                }
              />
            </Section>
          ) : (
            <div className="panel-grid">
              {dashboard.panels.map((panel) => (
                <PanelCard
                  key={panel.id}
                  panel={panel}
                  projectId={detail.project.id}
                  window={window}
                  editable={detail.project.writable}
                  onEdit={() => setEditingPanel(panel)}
                  onRemove={() =>
                    run(`remove-${panel.id}`, async () => {
                      try {
                        await savePanels(
                          dashboard.panels
                            .filter((entry) => entry.id !== panel.id)
                            .map(asInput),
                        );
                        toast.success("Panel removed");
                      } catch (error) {
                        toast.failure(error);
                      }
                    })
                  }
                />
              ))}
            </div>
          )}
        </>
      )}

      {creating && (
        <NameDialog
          title="New dashboard"
          label="Dashboard name"
          initial=""
          confirmLabel="Create dashboard"
          onClose={() => setCreating(false)}
          onSubmit={async (name) => {
            const created = await api.createDashboard(
              detail.project.id,
              name,
              [],
            );
            setSelectedId(created.id);
            setRevision((current) => current + 1);
            toast.success(`${name} created`);
          }}
        />
      )}

      {editingPanel && dashboard && (
        <PanelDialog
          panel={editingPanel === "new" ? null : editingPanel}
          buckets={detail.buckets}
          onClose={() => setEditingPanel(null)}
          onSave={async (input) => {
            const next =
              editingPanel === "new"
                ? [...dashboard.panels.map(asInput), input]
                : dashboard.panels.map((entry) =>
                    entry.id === editingPanel.id ? input : asInput(entry),
                  );
            await savePanels(next);
            setEditingPanel(null);
            toast.success(
              editingPanel === "new" ? "Panel added" : "Panel updated",
            );
          }}
        />
      )}

      {deleting && (
        <ConfirmDialog
          title={`Delete ${deleting.name}?`}
          confirmLabel="Delete dashboard"
          busy={isBusy("delete-dashboard")}
          description="The dashboard and its panels are removed. No stored points are affected."
          onClose={() => setDeleting(null)}
          onConfirm={() =>
            run("delete-dashboard", async () => {
              try {
                await api.deleteDashboard(detail.project.id, deleting.id);
                setDeleting(null);
                setSelectedId(null);
                setRevision((current) => current + 1);
                reload();
                toast.success("Dashboard deleted");
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

function PanelCard({
  panel,
  projectId,
  window,
  editable,
  onEdit,
  onRemove,
}: {
  panel: Panel;
  projectId: string;
  window: { from: string; to: string };
  editable: boolean;
  onEdit: () => void;
  onRemove: () => void;
}) {
  const query = useLoader<QueryResult>(
    (signal) =>
      dataClient({
        kind: "cloud",
        projectId,
        bucketId: panel.bucket_id,
        label: panel.title,
      }).query(panel.query, { from: window.from, to: window.to }, signal),
    [panel.id, panel.query, panel.bucket_id, window.from, window.to],
  );

  const shaped = toSeries(query.data ?? null);

  return (
    <article className="panel" style={{ gridColumn: `span ${panel.span}` }}>
      <div className="panel-head">
        <div>
          <h2>{panel.title}</h2>
          {query.data?.window && (
            <p>
              bucket {query.data.window.interval}
              {panel.unit ? ` · ${panel.unit}` : ""}
            </p>
          )}
        </div>
        {editable && (
          <div className="panel-actions">
            <button
              type="button"
              className="btn btn-sm btn-ghost"
              onClick={onEdit}
              aria-label={`Edit ${panel.title}`}
            >
              <Pencil size={14} aria-hidden />
            </button>
            <button
              type="button"
              className="btn btn-sm btn-ghost"
              onClick={onRemove}
              aria-label={`Remove ${panel.title}`}
            >
              <Trash2 size={14} aria-hidden />
            </button>
          </div>
        )}
      </div>
      <div className="panel-body">
        {query.loading && !query.data ? (
          <SkeletonRows rows={panel.kind === "stat" ? 2 : 4} />
        ) : query.error ? (
          <Notice tone="danger">{query.error}</Notice>
        ) : panel.kind === "stat" ? (
          <StatPanel result={query.data} unit={panel.unit} shaped={shaped} />
        ) : panel.kind === "table" ? (
          <TablePanel result={query.data} />
        ) : (
          <>
            <Chart
              option={
                shaped.series.length > 0 || shaped.bars.length > 0
                  ? chartOption(shaped, panel.kind, panel.unit, {
                      compact: true,
                      window: windowOf(query.data),
                    })
                  : null
              }
              height={panel.span <= 4 ? 190 : 240}
            />
            {shaped.hidden > 0 && (
              <p className="hint" style={{ marginTop: "var(--space-2)" }}>
                {shaped.hidden} more not shown — the palette has eight slots and
                a recycled colour would be ambiguous.
              </p>
            )}
          </>
        )}
      </div>
    </article>
  );
}

function StatPanel({
  result,
  unit,
  shaped,
}: {
  result: QueryResult | null;
  unit: string;
  shaped: ReturnType<typeof toSeries>;
}) {
  const value = statValue(result);
  const trend = shaped.series[0]?.points ?? [];
  return (
    <div className="stat" style={{ border: 0, padding: 0, boxShadow: "none" }}>
      <strong className="stat-value" style={{ fontSize: "var(--text-3xl)" }}>
        {value === null ? "—" : decimal(value)}
        {unit && value !== null && <em>{unit}</em>}
      </strong>
      {trend.length > 2 && (
        <div className="stat-spark">
          <Chart option={sparklineOption(trend)} height={42} />
        </div>
      )}
      {value === null && (
        <span className="stat-hint">No rows in this range</span>
      )}
    </div>
  );
}

function TablePanel({ result }: { result: QueryResult | null }) {
  if (!result || result.rows.length === 0) {
    return (
      <div className="chart-empty" style={{ height: 160 }}>
        No rows in this range
      </div>
    );
  }
  return (
    <DataTable
      dense
      columns={result.columns.map((column, index) => ({
        key: `${column}-${index}`,
        header: column,
        align: index === 0 ? "left" : "right",
        render: (row: (string | number | boolean | null)[]) => (
          <span className="mono">{cellText(row[index], column)}</span>
        ),
      }))}
      rows={result.rows.slice(0, 12)}
      rowKey={(row) => row.map(String).join("|")}
    />
  );
}

function NameDialog({
  title,
  label,
  initial,
  confirmLabel,
  onClose,
  onSubmit,
}: {
  title: string;
  label: string;
  initial: string;
  confirmLabel: string;
  onClose: () => void;
  onSubmit: (value: string) => Promise<void>;
}) {
  const [value, setValue] = useState(initial);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  return (
    <Modal
      title={title}
      onClose={onClose}
      width={430}
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
            disabled={busy || value.trim().length === 0}
            onClick={async () => {
              setBusy(true);
              setError(null);
              try {
                await onSubmit(value.trim());
                onClose();
              } catch (cause) {
                setError(
                  cause instanceof Error ? cause.message : "That did not work",
                );
              } finally {
                setBusy(false);
              }
            }}
          >
            {confirmLabel}
          </button>
        </>
      }
    >
      <div className="stack">
        {error && <div className="form-error">{error}</div>}
        <label className="field">
          <span className="label">{label}</span>
          <input
            className="input"
            value={value}
            onChange={(event) => setValue(event.target.value)}
            maxLength={64}
            placeholder="Fleet health"
          />
        </label>
      </div>
    </Modal>
  );
}

function PanelDialog({
  panel,
  buckets,
  onClose,
  onSave,
}: {
  panel: Panel | null;
  buckets: { id: string; name: string }[];
  onClose: () => void;
  onSave: (input: PanelInput) => Promise<void>;
}) {
  const [title, setTitle] = useState(panel?.title ?? "");
  const [kind, setKind] = useState<PanelKind>(panel?.kind ?? "line");
  const [bucketId, setBucketId] = useState(
    panel?.bucket_id ?? buckets[0]?.id ?? "",
  );
  const [query, setQuery] = useState(
    panel?.query ??
      "SELECT MEAN(usage) AS cpu FROM cpu WHERE $timeFilter GROUP BY time($interval), host",
  );
  const [unit, setUnit] = useState(panel?.unit ?? "");
  const [span, setSpan] = useState(panel?.span ?? 6);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  return (
    <Modal
      title={panel ? "Edit panel" : "Add panel"}
      description="Use $timeFilter and $interval so the panel follows the dashboard's range."
      onClose={onClose}
      width={620}
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
            disabled={busy || !bucketId || query.trim().length === 0}
            onClick={async () => {
              setBusy(true);
              setError(null);
              try {
                await onSave({
                  title: title.trim() || "Untitled panel",
                  kind,
                  bucket_id: bucketId,
                  query: query.trim(),
                  unit,
                  span,
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
            {panel ? "Save panel" : "Add panel"}
          </button>
        </>
      }
    >
      <div className="stack">
        {error && <div className="form-error">{error}</div>}
        <label className="field">
          <span className="label">Title</span>
          <input
            className="input"
            value={title}
            onChange={(event) => setTitle(event.target.value)}
            maxLength={64}
            placeholder="CPU by host"
          />
        </label>
        <div className="row">
          <label className="field" style={{ flex: 1 }}>
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
          <label className="field" style={{ flex: 1 }}>
            <span className="label">Type</span>
            <select
              className="select"
              value={kind}
              onChange={(event) => setKind(event.target.value as PanelKind)}
            >
              <option value="line">Line</option>
              <option value="area">Area</option>
              <option value="bar">Bar</option>
              <option value="stat">Single number</option>
              <option value="table">Table</option>
            </select>
          </label>
        </div>
        <div className="row">
          <label className="field" style={{ flex: 1 }}>
            <span className="label">Unit</span>
            <input
              className="input"
              value={unit}
              onChange={(event) => setUnit(event.target.value)}
              maxLength={12}
              placeholder="ms"
            />
          </label>
          <label className="field" style={{ flex: 1 }}>
            <span className="label">Width</span>
            <select
              className="select"
              value={span}
              onChange={(event) => setSpan(Number(event.target.value))}
            >
              <option value={3}>Quarter</option>
              <option value={4}>Third</option>
              <option value={6}>Half</option>
              <option value={8}>Two thirds</option>
              <option value={12}>Full width</option>
            </select>
          </label>
        </div>
        <label className="field">
          <span className="label">Query</span>
          <div className="editor">
            <textarea
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              rows={5}
              spellCheck={false}
              aria-label="Panel query"
            />
            <div className="editor-foot">
              <span>$timeFilter · $interval · $from · $to</span>
            </div>
          </div>
        </label>
      </div>
    </Modal>
  );
}
