/**
 * Data explorer: what is in a bucket, and the ability to change it.
 *
 * Schema discovery on the left, points on the right, and a chart for whichever
 * numeric field is selected. Editing a point is an upsert of the same
 * measurement, tag set and timestamp — that is stated in the dialog, because it
 * is the one piece of the data model that surprises people.
 */

import { useEffect, useMemo, useState } from "react";
import {
  ChevronDown,
  Database,
  Pencil,
  Plus,
  RefreshCw,
  Search,
  Table2,
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
  useAction,
  useDebounced,
  useLoader,
  type Column,
} from "../components/ui";
import { chartOption, toSeries, windowOf } from "../lib/charts";
import { count, fieldText, nanosToLocal } from "../lib/format";
import { nowNanos, resolveRange, type RangeKey } from "../lib/time";
import { useToast } from "../lib/toast";
import type { Point, Schema } from "../lib/types";
import { PENDING_WRITE_KEY, takePending } from "./AssistantPanel";
import { useProject } from "./ProjectContext";
import { SourcePicker, useCanWrite } from "./SourcePicker";

const PAGE_SIZE = 50;

export default function Explorer() {
  const { client, target } = useProject();
  const canWrite = useCanWrite();
  const toast = useToast();
  const { run, isBusy } = useAction();

  const [measurement, setMeasurement] = useState<string>("");
  const [range, setRange] = useState<RangeKey>("24h");
  const [page, setPage] = useState(0);
  const [search, setSearch] = useState("");
  const [field, setField] = useState("");
  const [editing, setEditing] = useState<Point | null>(null);
  // A write the assistant prepared, if it sent us here to review it.
  const [proposed] = useState(() => takePending(PENDING_WRITE_KEY));
  const [creating, setCreating] = useState(proposed !== null);
  const [deleting, setDeleting] = useState<Point | null>(null);
  const [revision, setRevision] = useState(0);
  const debouncedSearch = useDebounced(search);

  const window = useMemo(() => resolveRange(range), [range, revision]);
  const sourceKey = target ? JSON.stringify(target) : "none";

  const schema = useLoader<Schema>(
    (signal) =>
      client ? client.schema(signal) : Promise.resolve({ measurements: {} }),
    [sourceKey, revision],
  );

  const measurements = useMemo(
    () =>
      Object.entries(schema.data?.measurements ?? {}).sort(
        (a, b) => b[1].points - a[1].points,
      ),
    [schema.data],
  );

  // Keep the selection valid as the source changes underneath it.
  useEffect(() => {
    if (measurements.length === 0) {
      setMeasurement("");
      return;
    }
    if (!measurements.some(([name]) => name === measurement)) {
      setMeasurement(measurements[0][0]);
      setPage(0);
    }
  }, [measurements, measurement]);

  const selected = measurements.find(([name]) => name === measurement)?.[1];

  useEffect(() => {
    if (!selected) {
      setField("");
      return;
    }
    const numeric = Object.entries(selected.fields).find(([, types]) =>
      types.some((type) => type === "float" || type === "integer"),
    )?.[0];
    if (!field || !selected.fields[field]) setField(numeric ?? "");
  }, [selected, field]);

  const points = useLoader(
    (signal) =>
      client && measurement
        ? client.points(
            {
              measurement,
              start: window.from,
              end: window.to,
              limit: PAGE_SIZE,
              offset: page * PAGE_SIZE,
            },
            signal,
          )
        : Promise.resolve({
            total: 0,
            offset: 0,
            limit: PAGE_SIZE,
            points: [],
          }),
    [sourceKey, measurement, window.from, window.to, page, revision],
  );

  const series = useLoader(
    (signal) =>
      client && measurement && field
        ? client.query(
            `SELECT MEAN(${field}) AS ${field} FROM ${measurement} WHERE $timeFilter GROUP BY time($interval)`,
            { from: window.from, to: window.to },
            signal,
          )
        : Promise.resolve(null),
    [sourceKey, measurement, field, window.from, window.to, revision],
  );

  const visible = useMemo(() => {
    const rows = points.data?.points ?? [];
    const needle = debouncedSearch.trim().toLowerCase();
    if (!needle) return rows;
    return rows.filter((point) =>
      `${point.measurement} ${Object.entries(point.tags)
        .map(([key, value]) => `${key}=${value}`)
        .join(" ")} ${Object.entries(point.fields)
        .map(([key, value]) => `${key}=${fieldText(value)}`)
        .join(" ")}`
        .toLowerCase()
        .includes(needle),
    );
  }, [points.data, debouncedSearch]);

  const tagKeys = useMemo(() => {
    const keys = new Set<string>();
    for (const point of points.data?.points ?? []) {
      for (const key of Object.keys(point.tags)) keys.add(key);
    }
    return [...keys].sort();
  }, [points.data]);

  const fieldKeys = useMemo(() => {
    const keys = new Set<string>();
    for (const point of points.data?.points ?? []) {
      for (const key of Object.keys(point.fields)) keys.add(key);
    }
    return [...keys].sort();
  }, [points.data]);

  const columns: Column<Point>[] = [
    {
      key: "time",
      header: "Time",
      width: "190px",
      render: (point) => (
        <span className="mono" title={point.timestamp}>
          {nanosToLocal(point.timestamp)}
        </span>
      ),
    },
    ...tagKeys.map<Column<Point>>((key) => ({
      key: `tag-${key}`,
      header: key,
      secondary: tagKeys.length > 2,
      render: (point) => (
        <span className={point.tags[key] ? undefined : "cell-muted"}>
          {point.tags[key] ?? "—"}
        </span>
      ),
    })),
    ...fieldKeys.map<Column<Point>>((key) => ({
      key: `field-${key}`,
      header: key,
      align: "right",
      render: (point) => (
        <span className="mono">{fieldText(point.fields[key])}</span>
      ),
    })),
    ...(canWrite
      ? [
          {
            key: "actions",
            header: "",
            align: "right" as const,
            width: "84px",
            render: (point: Point) => (
              <div className="cell-actions">
                <button
                  type="button"
                  className="btn btn-sm btn-ghost"
                  aria-label="Edit point"
                  onClick={() => setEditing(point)}
                >
                  <Pencil size={14} aria-hidden />
                </button>
                <button
                  type="button"
                  className="btn btn-sm btn-ghost"
                  aria-label="Delete point"
                  onClick={() => setDeleting(point)}
                >
                  <Trash2 size={14} aria-hidden />
                </button>
              </div>
            ),
          },
        ]
      : []),
  ];

  const shaped = toSeries(series.data);
  const totalPages = Math.ceil((points.data?.total ?? 0) / PAGE_SIZE);
  const refreshAll = () => setRevision((current) => current + 1);

  if (!client) {
    return (
      <>
        <PageHeader title="Data explorer" />
        <Section>
          <EmptyState
            icon={<Database size={20} aria-hidden />}
            title="No data source"
            description="Create a bucket in this project, or connect a FluxDB server you run yourself."
          />
        </Section>
      </>
    );
  }

  return (
    <>
      <PageHeader
        title="Data explorer"
        description="Discover what a bucket holds, read the points, and change them."
        actions={
          <>
            <SourcePicker />
            <RangePicker
              value={range}
              onChange={(value) => {
                setRange(value);
                setPage(0);
              }}
            />
            <button
              type="button"
              className="btn btn-icon"
              onClick={refreshAll}
              aria-label="Refresh"
            >
              <RefreshCw size={15} aria-hidden />
            </button>
            {canWrite && (
              <button
                type="button"
                className="btn btn-primary"
                onClick={() => setCreating(true)}
              >
                <Plus size={15} aria-hidden /> Write points
              </button>
            )}
          </>
        }
      />

      <div
        className="grid"
        style={{ gridTemplateColumns: "minmax(0, 280px) minmax(0, 1fr)" }}
      >
        <Section
          title="Schema"
          description={`${measurements.length} measurement${measurements.length === 1 ? "" : "s"}`}
        >
          {schema.loading && !schema.data ? (
            <SkeletonRows rows={4} />
          ) : schema.error ? (
            <Notice tone="danger">{schema.error}</Notice>
          ) : measurements.length === 0 ? (
            <EmptyState
              icon={<Table2 size={18} aria-hidden />}
              title="Nothing stored yet"
              description="Write a point, or load sample data from the project overview."
            />
          ) : (
            <div className="schema-tree">
              {measurements.map(([name, info]) => (
                <details
                  key={name}
                  className="schema-node"
                  open={name === measurement}
                >
                  <summary
                    onClick={(event) => {
                      event.preventDefault();
                      setMeasurement(name);
                      setPage(0);
                    }}
                  >
                    <ChevronDown size={14} aria-hidden />
                    {name}
                    <span className="schema-node-meta">
                      {count(info.points)}
                    </span>
                  </summary>
                  <div className="schema-detail">
                    <div className="schema-group">
                      <h5>Fields</h5>
                      <div className="chip-list">
                        {Object.entries(info.fields).map(([key, types]) => (
                          <button
                            key={key}
                            type="button"
                            className="token"
                            onClick={() => {
                              setMeasurement(name);
                              setField(key);
                            }}
                            title={`Chart ${key}`}
                          >
                            {key} <em>{types.join("/")}</em>
                          </button>
                        ))}
                      </div>
                    </div>
                    {Object.keys(info.tags).length > 0 && (
                      <div className="schema-group">
                        <h5>Tags</h5>
                        <div className="chip-list">
                          {Object.entries(info.tags).map(([key, values]) => (
                            <span
                              key={key}
                              className="token"
                              title={values.join(", ")}
                            >
                              {key} <em>{values.length}</em>
                            </span>
                          ))}
                        </div>
                      </div>
                    )}
                  </div>
                </details>
              ))}
            </div>
          )}
        </Section>

        <div>
          {measurement && (
            <Section
              title={field ? `${measurement}.${field}` : measurement}
              description={
                field
                  ? "Mean per bucket over the selected range."
                  : "This measurement has no numeric field to chart."
              }
              actions={
                selected && (
                  <label className="source-picker">
                    <span className="visually-hidden">Field to chart</span>
                    <select
                      className="select"
                      value={field}
                      onChange={(event) => setField(event.target.value)}
                    >
                      <option value="">No chart</option>
                      {Object.entries(selected.fields)
                        .filter(([, types]) =>
                          types.some(
                            (type) => type === "float" || type === "integer",
                          ),
                        )
                        .map(([key]) => (
                          <option key={key} value={key}>
                            {key}
                          </option>
                        ))}
                    </select>
                  </label>
                )
              }
            >
              {series.loading && !series.data ? (
                <SkeletonRows rows={4} />
              ) : (
                <Chart
                  option={
                    shaped.series.length > 0
                      ? chartOption(shaped, "area", "", {
                          legend: false,
                          window: windowOf(series.data),
                        })
                      : null
                  }
                  height={220}
                  empty="No numeric values in this range"
                />
              )}
            </Section>
          )}

          <Section
            compact
            title="Points"
            description={
              points.data
                ? `${count(points.data.total)} in range · newest first`
                : undefined
            }
            actions={
              <label className="source-picker">
                <span className="visually-hidden">Filter rows</span>
                <span className="row" style={{ gap: 6 }}>
                  <Search size={14} aria-hidden className="cell-muted" />
                  <input
                    className="input"
                    style={{ minWidth: 180 }}
                    placeholder="Filter this page"
                    value={search}
                    onChange={(event) => setSearch(event.target.value)}
                  />
                </span>
              </label>
            }
          >
            {points.loading && !points.data ? (
              <div style={{ padding: "var(--space-5)" }}>
                <SkeletonRows rows={6} />
              </div>
            ) : points.error ? (
              <div style={{ padding: "var(--space-5)" }}>
                <Notice tone="danger">{points.error}</Notice>
              </div>
            ) : (
              <>
                <DataTable
                  columns={columns}
                  rows={visible}
                  rowKey={(point) =>
                    `${point.measurement}|${point.timestamp}|${JSON.stringify(point.tags)}`
                  }
                  dense
                  empty={
                    <EmptyState
                      icon={<Table2 size={20} aria-hidden />}
                      title={
                        debouncedSearch
                          ? "No rows on this page match the filter"
                          : "No points in this range"
                      }
                      description={
                        debouncedSearch
                          ? "The filter applies to the rows currently loaded, not to the whole bucket."
                          : "Widen the time range, or write some points."
                      }
                    />
                  }
                />
                {totalPages > 1 && (
                  <div className="pager">
                    <span>
                      Page {page + 1} of {totalPages}
                    </span>
                    <div className="pager-buttons">
                      <button
                        type="button"
                        className="btn btn-sm"
                        disabled={page === 0}
                        onClick={() => setPage((current) => current - 1)}
                      >
                        Previous
                      </button>
                      <button
                        type="button"
                        className="btn btn-sm"
                        disabled={page + 1 >= totalPages}
                        onClick={() => setPage((current) => current + 1)}
                      >
                        Next
                      </button>
                    </div>
                  </div>
                )}
              </>
            )}
          </Section>
        </div>
      </div>

      {(creating || editing) && (
        <PointDialog
          point={editing}
          measurement={measurement}
          initial={editing ? null : proposed}
          onClose={() => {
            setCreating(false);
            setEditing(null);
          }}
          onSaved={(written) => {
            setCreating(false);
            setEditing(null);
            toast.success(
              written === 1 ? "Point written" : `${written} points written`,
            );
            refreshAll();
          }}
        />
      )}

      {deleting && (
        <ConfirmDialog
          title="Delete this point?"
          confirmLabel="Delete point"
          busy={isBusy("delete-point")}
          description={
            <>
              <p>
                <code>{deleting.measurement}</code> at{" "}
                <code>{nanosToLocal(deleting.timestamp)}</code>
                {Object.keys(deleting.tags).length > 0 && (
                  <>
                    {" "}
                    with{" "}
                    <code>
                      {Object.entries(deleting.tags)
                        .map(([key, value]) => `${key}=${value}`)
                        .join(", ")}
                    </code>
                  </>
                )}
                .
              </p>
              <p style={{ marginTop: "var(--space-3)" }}>
                Deleting a point removes all of its fields. A tombstone is
                written immediately; compaction reclaims the space later.
              </p>
            </>
          }
          onClose={() => setDeleting(null)}
          onConfirm={() =>
            run("delete-point", async () => {
              try {
                const result = await client.deletePoints({
                  measurement: deleting.measurement,
                  tags: deleting.tags,
                  start: deleting.timestamp,
                  end: deleting.timestamp,
                  exact: true,
                });
                toast.success(
                  result.deleted === 1
                    ? "Point deleted"
                    : `${result.deleted} points deleted`,
                );
                setDeleting(null);
                refreshAll();
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

/**
 * Write or edit points as JSON. A raw editor rather than a form, because the
 * shape is the API's shape: showing it is how someone learns to script against
 * it.
 */
function PointDialog({
  point,
  measurement,
  initial,
  onClose,
  onSaved,
}: {
  point: Point | null;
  measurement: string;
  /** A payload prepared elsewhere, shown as-is for review. */
  initial?: string | null;
  onClose: () => void;
  onSaved: (written: number) => void;
}) {
  const { client } = useProject();
  const [text, setText] = useState(
    () =>
      initial ??
      JSON.stringify(
        {
          points: [
            point ?? {
              measurement: measurement || "cpu",
              tags: { host: "api-01" },
              timestamp: nowNanos(),
              fields: { usage: 42.8, cores: { integer: "8" } },
            },
          ],
        },
        null,
        2,
      ),
  );
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function submit() {
    setBusy(true);
    setError(null);
    try {
      const parsed = JSON.parse(text) as { points?: Point[] };
      if (!Array.isArray(parsed.points) || parsed.points.length === 0) {
        throw new Error('Supply {"points": [ … ]} with at least one point');
      }
      const result = await client!.writePoints(parsed.points);
      onSaved(result.written);
    } catch (cause) {
      setError(
        cause instanceof SyntaxError
          ? `That is not valid JSON: ${cause.message}`
          : cause instanceof Error
            ? cause.message
            : "That did not work",
      );
    } finally {
      setBusy(false);
    }
  }

  return (
    <Modal
      title={point ? "Edit point" : "Write points"}
      description={
        point
          ? "Saving rewrites the same measurement, tag set and timestamp. Supplied fields replace existing values; changing a tag or the timestamp creates a different point instead."
          : 'A batch of 1 to 10,000 points. Timestamps are decimal nanosecond strings; exact 64-bit integers use {"integer": "…"}.'
      }
      onClose={onClose}
      width={640}
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
            {busy ? "Writing…" : point ? "Save point" : "Write points"}
          </button>
        </>
      }
    >
      <div className="stack">
        {error && <div className="form-error">{error}</div>}
        <div className="editor">
          <textarea
            value={text}
            onChange={(event) => setText(event.target.value)}
            spellCheck={false}
            rows={14}
            aria-label="Points JSON"
          />
          <div className="editor-foot">
            <span>JSON</span>
            <span>
              Float fields are plain JSON numbers · booleans and strings are
              accepted
            </span>
          </div>
        </div>
      </div>
    </Modal>
  );
}
