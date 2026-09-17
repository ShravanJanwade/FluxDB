/**
 * The SQL workspace.
 *
 * Results are shown as a table and as a chart from the same response: the
 * column classification in `toSeries` decides whether a result is a time
 * series, a set of categories, or neither, so no chart configuration is asked
 * for.
 */

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  BarChart3,
  Clock,
  Download,
  History,
  Play,
  Plus,
  Table2,
  Trash2,
} from "lucide-react";
import {
  Chart,
  DataTable,
  EmptyState,
  Modal,
  Notice,
  PageHeader,
  RangePicker,
  Section,
  Tabs,
  useLoader,
  type Column,
} from "../components/ui";
import { api } from "../lib/api";
import { chartOption, toSeries, windowOf } from "../lib/charts";
import { cellText, count, milliseconds } from "../lib/format";
import { DEFAULT_RANGE, resolveRange, type RangeKey } from "../lib/time";
import { useToast } from "../lib/toast";
import type { PanelKind, QueryResult } from "../lib/types";
import { useProject } from "./ProjectContext";
import { SourcePicker } from "./SourcePicker";

type Row = (string | number | boolean | null)[];

const STARTER =
  "SELECT * FROM cpu WHERE $timeFilter ORDER BY time DESC LIMIT 100";

function historyKey(projectId: string) {
  return `fluxdb.history.${projectId}`;
}

function loadHistory(projectId: string): string[] {
  try {
    const parsed = JSON.parse(
      localStorage.getItem(historyKey(projectId)) ?? "[]",
    );
    return Array.isArray(parsed)
      ? parsed.filter((entry): entry is string => typeof entry === "string")
      : [];
  } catch {
    return [];
  }
}

export default function QueryWorkspace() {
  const { detail, client, target } = useProject();
  const toast = useToast();
  const [sql, setSql] = useState(STARTER);
  const [range, setRange] = useState<RangeKey>(DEFAULT_RANGE);
  const [result, setResult] = useState<QueryResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [running, setRunning] = useState(false);
  const [view, setView] = useState<"table" | "chart">("table");
  const [history, setHistory] = useState<string[]>(() =>
    loadHistory(detail.project.id),
  );
  const [saving, setSaving] = useState(false);
  const editor = useRef<HTMLTextAreaElement>(null);

  const examples = useLoader(
    () => api.examples(detail.project.id),
    [detail.project.id],
  );

  const remember = useCallback(
    (query: string) => {
      setHistory((current) => {
        const next = [
          query,
          ...current.filter((entry) => entry !== query),
        ].slice(0, 25);
        try {
          localStorage.setItem(
            historyKey(detail.project.id),
            JSON.stringify(next),
          );
        } catch {
          // History is a convenience; failing to persist it is not an error.
        }
        return next;
      });
    },
    [detail.project.id],
  );

  const run = useCallback(
    async (query: string = sql) => {
      if (!client || !query.trim()) return;
      setRunning(true);
      setError(null);
      try {
        const window = resolveRange(range);
        const outcome = await client.query(query, {
          from: window.from,
          to: window.to,
        });
        setResult(outcome);
        remember(query.trim());
        // A result with a time column is almost always worth seeing as a chart.
        const shaped = toSeries(outcome);
        setView(
          shaped.series.length > 0 || shaped.bars.length > 0
            ? "chart"
            : "table",
        );
      } catch (cause) {
        setResult(null);
        setError(cause instanceof Error ? cause.message : "The query failed");
      } finally {
        setRunning(false);
      }
    },
    [client, sql, range, remember],
  );

  // Ctrl/Cmd+Enter runs, the way every SQL console does.
  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      if ((event.ctrlKey || event.metaKey) && event.key === "Enter") {
        event.preventDefault();
        void run();
      }
    };
    const node = editor.current;
    node?.addEventListener("keydown", onKey);
    return () => node?.removeEventListener("keydown", onKey);
  }, [run]);

  const shaped = useMemo(() => toSeries(result), [result]);

  const columns = useMemo<Column<Row>[]>(
    () =>
      (result?.columns ?? []).map((column, index) => ({
        key: `${column}-${index}`,
        header: column,
        align: index === 0 ? "left" : "right",
        render: (row) => (
          <span className={column === "time" ? "mono" : "mono"}>
            {cellText(row[index], column)}
          </span>
        ),
      })),
    [result],
  );

  function exportCsv() {
    if (!result) return;
    const escape = (value: unknown) => {
      const text = value === null || value === undefined ? "" : String(value);
      return /[",\n]/.test(text) ? `"${text.replaceAll('"', '""')}"` : text;
    };
    const csv = [
      result.columns.map(escape).join(","),
      ...result.rows.map((row) => row.map(escape).join(",")),
    ].join("\n");
    const blob = new Blob([csv], { type: "text/csv;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = "fluxdb-query.csv";
    anchor.click();
    URL.revokeObjectURL(url);
    toast.success(`Exported ${count(result.rows.length)} rows`);
  }

  return (
    <>
      <PageHeader
        title="Query workspace"
        description="A documented SQL subset over stored points. Unsupported syntax returns an error that names what it cannot do."
        actions={
          <>
            <SourcePicker />
            <RangePicker value={range} onChange={setRange} />
          </>
        }
      />

      <Section
        title="SQL"
        description={
          <>
            <code>$timeFilter</code> becomes the selected range,{" "}
            <code>$interval</code> a bucket width chosen for the chart, and{" "}
            <code>$from</code>/<code>$to</code> the raw nanosecond bounds.
          </>
        }
        actions={
          <>
            <button
              type="button"
              className="btn btn-primary"
              onClick={() => run()}
              disabled={running || !client}
            >
              <Play size={15} aria-hidden /> {running ? "Running…" : "Run"}
            </button>
            <span className="hint">
              {navigator.platform.includes("Mac") ? "⌘" : "Ctrl"} + Enter
            </span>
          </>
        }
      >
        <div className="stack">
          <div className="editor">
            <textarea
              ref={editor}
              value={sql}
              onChange={(event) => setSql(event.target.value)}
              spellCheck={false}
              rows={6}
              aria-label="SQL query"
              placeholder="SELECT MEAN(usage) AS cpu FROM cpu WHERE $timeFilter GROUP BY time($interval), host"
            />
            <div className="editor-foot">
              <span>{target ? target.label : "no source"}</span>
              <span>{sql.length} characters</span>
              {result?.window && (
                <span className="row-end">bucket {result.window.interval}</span>
              )}
            </div>
          </div>

          {examples.data && (
            <div>
              <span
                className="label"
                style={{ display: "block", marginBottom: 6 }}
              >
                Worked examples for the sample dataset
              </span>
              <div className="chip-list">
                {examples.data.examples.map((example) => (
                  <button
                    key={example.title}
                    type="button"
                    className="chip"
                    title={example.query}
                    onClick={() => {
                      setSql(example.query);
                      void run(example.query);
                    }}
                  >
                    {example.title}
                  </button>
                ))}
              </div>
            </div>
          )}
        </div>
      </Section>

      {error && (
        <div style={{ marginTop: "var(--space-5)" }}>
          <Notice tone="danger" title="The query was rejected">
            {error}
          </Notice>
        </div>
      )}

      <Section
        compact
        title="Results"
        description={
          result
            ? `${count(result.rows.length)} rows · ${milliseconds(result.execution_time_ms)}`
            : undefined
        }
        actions={
          result && (
            <>
              <Tabs
                value={view}
                onChange={setView}
                tabs={[
                  { id: "table" as const, label: "Table" },
                  { id: "chart" as const, label: "Chart" },
                ]}
              />
              <button type="button" className="btn btn-sm" onClick={exportCsv}>
                <Download size={14} aria-hidden /> CSV
              </button>
              {detail.project.writable && (
                <button
                  type="button"
                  className="btn btn-sm"
                  onClick={() => setSaving(true)}
                  disabled={
                    shaped.series.length === 0 && shaped.bars.length === 0
                  }
                >
                  <Plus size={14} aria-hidden /> Save as panel
                </button>
              )}
            </>
          )
        }
      >
        {!result ? (
          <EmptyState
            icon={<Table2 size={20} aria-hidden />}
            title="No results yet"
            description="Run a query, or pick one of the worked examples above."
          />
        ) : result.rows.length === 0 ? (
          <EmptyState
            icon={<Clock size={20} aria-hidden />}
            title="The query ran and matched nothing"
            description="Widen the time range, or check the measurement and tag names in the explorer."
          />
        ) : view === "chart" ? (
          <div style={{ padding: "var(--space-5)" }}>
            {shaped.series.length === 0 && shaped.bars.length === 0 ? (
              <EmptyState
                icon={<BarChart3 size={20} aria-hidden />}
                title="This result has nothing to plot"
                description="A chart needs at least one numeric column. Add an aggregate, or read it as a table."
              />
            ) : (
              <Chart
                option={chartOption(
                  shaped,
                  shaped.hasTime ? "line" : "bar",
                  "",
                  { window: windowOf(result) },
                )}
                height={330}
              />
            )}
          </div>
        ) : (
          <DataTable
            columns={columns}
            rows={result.rows}
            rowKey={(row) => row.map(String).join("|")}
            dense
          />
        )}
      </Section>

      <div className="grid grid-2">
        <Section
          title="History"
          description="Queries you have run in this project, newest first."
          actions={
            history.length > 0 && (
              <button
                type="button"
                className="btn btn-sm btn-ghost"
                onClick={() => {
                  setHistory([]);
                  try {
                    localStorage.removeItem(historyKey(detail.project.id));
                  } catch {
                    // Nothing to do if storage is unavailable.
                  }
                }}
              >
                <Trash2 size={14} aria-hidden /> Clear
              </button>
            )
          }
        >
          {history.length === 0 ? (
            <EmptyState
              icon={<History size={18} aria-hidden />}
              title="No history yet"
              description="Successful queries are remembered in this browser."
            />
          ) : (
            <div className="stack">
              {history.slice(0, 10).map((entry, index) => (
                <button
                  key={`${entry}-${index}`}
                  type="button"
                  className="chip"
                  style={{
                    justifyContent: "flex-start",
                    fontFamily: "var(--font-mono)",
                    textAlign: "left",
                    whiteSpace: "normal",
                  }}
                  onClick={() => setSql(entry)}
                >
                  {entry.length > 140 ? `${entry.slice(0, 140)}…` : entry}
                </button>
              ))}
            </div>
          )}
        </Section>

        <Section
          title="Supported SQL"
          description="Anything outside this subset returns an error rather than silently different results."
        >
          <dl className="kv">
            <dt>Projection</dt>
            <dd>
              <code>SELECT *</code>, named fields, and{" "}
              <code>COUNT/SUM/MEAN/MIN/MAX/FIRST/LAST</code> with{" "}
              <code>AS</code> aliases
            </dd>
            <dt>Predicates</dt>
            <dd>
              <code>AND</code>, <code>OR</code>, <code>NOT</code>,{" "}
              <code>IN</code>, <code>BETWEEN</code>, <code>LIKE</code>,{" "}
              <code>IS NULL</code>, comparisons on fields and tags
            </dd>
            <dt>Time</dt>
            <dd>
              <code>time &gt;= …</code> with nanosecond literals or RFC 3339
              strings. There is no <code>now()</code> — use{" "}
              <code>$timeFilter</code>
            </dd>
            <dt>Grouping</dt>
            <dd>
              <code>GROUP BY time('5m')</code>, optionally with tag columns, and{" "}
              <code>HAVING</code>
            </dd>
            <dt>Shaping</dt>
            <dd>
              <code>DISTINCT</code>, <code>ORDER BY</code>, <code>LIMIT</code>,{" "}
              <code>OFFSET</code>
            </dd>
          </dl>
          <div style={{ marginTop: "var(--space-4)" }}>
            <Notice tone="info">
              Integer results and timestamps come back as decimal strings, so no
              precision is lost in JSON. The table above renders them as
              written.
            </Notice>
          </div>
        </Section>
      </div>

      {saving && result && (
        <SavePanelDialog
          query={sql}
          suggestedKind={shaped.hasTime ? "line" : "bar"}
          onClose={() => setSaving(false)}
          onSaved={(name) => {
            setSaving(false);
            toast.success(`Added to ${name}`);
          }}
        />
      )}
    </>
  );
}

/** Adds the current query to a dashboard as a new panel. */
function SavePanelDialog({
  query,
  suggestedKind,
  onClose,
  onSaved,
}: {
  query: string;
  suggestedKind: PanelKind;
  onClose: () => void;
  onSaved: (dashboardName: string) => void;
}) {
  const { detail, target } = useProject();
  const dashboards = useLoader(
    () => api.dashboards(detail.project.id),
    [detail.project.id],
  );
  const [title, setTitle] = useState("");
  const [kind, setKind] = useState<PanelKind>(suggestedKind);
  const [unit, setUnit] = useState("");
  const [dashboardId, setDashboardId] = useState<string>("");
  const [newName, setNewName] = useState("Saved queries");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const bucketId = target?.kind === "cloud" ? target.bucketId : null;

  useEffect(() => {
    if (dashboards.data?.dashboards.length && !dashboardId) {
      setDashboardId(dashboards.data.dashboards[0].id);
    }
  }, [dashboards.data, dashboardId]);

  async function submit() {
    if (!bucketId) {
      setError(
        "Panels are stored against a project bucket. Select a bucket as the data source before saving.",
      );
      return;
    }
    setBusy(true);
    setError(null);
    try {
      const panel = {
        title: title.trim() || "Untitled panel",
        kind,
        bucket_id: bucketId,
        query,
        unit,
        span: 6,
      };
      if (dashboardId) {
        const existing = dashboards.data!.dashboards.find(
          (dashboard) => dashboard.id === dashboardId,
        )!;
        await api.saveDashboard(detail.project.id, existing.id, existing.name, [
          ...existing.panels.map((item) => ({
            title: item.title,
            kind: item.kind,
            bucket_id: item.bucket_id,
            query: item.query,
            unit: item.unit,
            span: item.span,
          })),
          panel,
        ]);
        onSaved(existing.name);
      } else {
        await api.createDashboard(detail.project.id, newName, [panel]);
        onSaved(newName);
      }
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "That did not work");
    } finally {
      setBusy(false);
    }
  }

  return (
    <Modal
      title="Save as a dashboard panel"
      description="The query is stored with its macros intact, so the panel follows whichever range the dashboard is showing."
      onClose={onClose}
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
            onClick={submit}
            disabled={busy}
          >
            Save panel
          </button>
        </>
      }
    >
      <div className="stack">
        {error && <div className="form-error">{error}</div>}
        <label className="field">
          <span className="label">Panel title</span>
          <input
            className="input"
            value={title}
            onChange={(event) => setTitle(event.target.value)}
            placeholder="p99 latency by service"
            maxLength={64}
          />
        </label>
        <div className="row">
          <label className="field" style={{ flex: 1 }}>
            <span className="label">Chart type</span>
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
          <label className="field" style={{ flex: 1 }}>
            <span className="label">Unit</span>
            <input
              className="input"
              value={unit}
              onChange={(event) => setUnit(event.target.value)}
              placeholder="ms, %, req/s"
              maxLength={12}
            />
          </label>
        </div>
        <label className="field">
          <span className="label">Dashboard</span>
          <select
            className="select"
            value={dashboardId}
            onChange={(event) => setDashboardId(event.target.value)}
          >
            {(dashboards.data?.dashboards ?? []).map((dashboard) => (
              <option key={dashboard.id} value={dashboard.id}>
                {dashboard.name}
              </option>
            ))}
            <option value="">Create a new dashboard…</option>
          </select>
        </label>
        {!dashboardId && (
          <label className="field">
            <span className="label">New dashboard name</span>
            <input
              className="input"
              value={newName}
              onChange={(event) => setNewName(event.target.value)}
              maxLength={64}
            />
          </label>
        )}
      </div>
    </Modal>
  );
}
