/** Bucket lifecycle: create, retention, maintenance, snapshot export, delete. */

import { useState } from "react";
import { Link } from "react-router-dom";
import {
  Database,
  Download,
  HardDriveDownload,
  Layers,
  Plus,
  Sparkles,
  Trash2,
  Waves,
} from "lucide-react";
import {
  ConfirmDialog,
  DataTable,
  EmptyState,
  Menu,
  Modal,
  Notice,
  PageHeader,
  Section,
  UsageBar,
  useAction,
  type Column,
} from "../components/ui";
import { api, dataClient } from "../lib/api";
import { bytes, count, duration, relative, timestamp } from "../lib/format";
import { RETENTIONS } from "../lib/time";
import { useToast } from "../lib/toast";
import type { Bucket } from "../lib/types";
import { useProject } from "./ProjectContext";

export default function Buckets() {
  const { detail, reload } = useProject();
  const toast = useToast();
  const { run, isBusy } = useAction();
  const [creating, setCreating] = useState(false);
  const [editing, setEditing] = useState<Bucket | null>(null);
  const [deleting, setDeleting] = useState<Bucket | null>(null);

  const project = detail.project;
  const atLimit = detail.usage.buckets >= detail.usage.buckets_limit;

  async function download(bucket: Bucket) {
    const client = dataClient({
      kind: "cloud",
      projectId: project.id,
      bucketId: bucket.id,
      label: bucket.name,
    });
    const snapshot = await client.exportSnapshot();
    const blob = new Blob([JSON.stringify(snapshot, null, 2)], {
      type: "application/json",
    });
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = `${bucket.name}-snapshot.json`;
    anchor.click();
    URL.revokeObjectURL(url);
  }

  const columns: Column<Bucket>[] = [
    {
      key: "name",
      header: "Bucket",
      render: (bucket) => (
        <span className="row" style={{ gap: "var(--space-2)" }}>
          <Database size={15} aria-hidden className="cell-muted" />
          <strong>{bucket.name}</strong>
        </span>
      ),
    },
    {
      key: "points",
      header: "Points",
      align: "right",
      render: (bucket) => <span className="mono">{count(bucket.points)}</span>,
    },
    {
      key: "size",
      header: "On disk",
      align: "right",
      render: (bucket) => (
        <span className="mono">{bytes(bucket.size_bytes)}</span>
      ),
    },
    {
      key: "memtable",
      header: "In memtable",
      align: "right",
      secondary: true,
      render: (bucket) => (
        <span className="mono cell-muted">{bytes(bucket.memtable_bytes)}</span>
      ),
    },
    {
      key: "sstables",
      header: "SSTables",
      align: "right",
      secondary: true,
      render: (bucket) => <span className="mono">{bucket.sstables}</span>,
    },
    {
      key: "retention",
      header: "Retention",
      render: (bucket) => (
        <span
          className={bucket.retention_seconds === 0 ? "cell-muted" : undefined}
        >
          {duration(bucket.retention_seconds)}
        </span>
      ),
    },
    {
      key: "created",
      header: "Created",
      secondary: true,
      render: (bucket) => (
        <span className="cell-muted" title={timestamp(bucket.created_at)}>
          {relative(bucket.created_at)}
        </span>
      ),
    },
    {
      key: "actions",
      header: "",
      align: "right",
      width: "56px",
      render: (bucket) => (
        <div className="cell-actions">
          <Menu
            label={`Actions for ${bucket.name}`}
            align="end"
            trigger={<span>⋯</span>}
          >
            {(close) => (
              <>
                <Link
                  className="menu-item"
                  to={`/app/p/${project.id}/explorer`}
                  onClick={close}
                >
                  <Layers size={15} aria-hidden /> Open in explorer
                </Link>
                <button
                  type="button"
                  className="menu-item"
                  onClick={() =>
                    run(`export-${bucket.id}`, async () => {
                      close();
                      try {
                        await download(bucket);
                        toast.success(`Exported ${bucket.name}`);
                      } catch (error) {
                        toast.failure(error);
                      }
                    })
                  }
                >
                  <Download size={15} aria-hidden /> Export snapshot
                </button>
                {project.writable && (
                  <>
                    <div className="menu-separator" />
                    <button
                      type="button"
                      className="menu-item"
                      onClick={() => {
                        close();
                        setEditing(bucket);
                      }}
                    >
                      <HardDriveDownload size={15} aria-hidden /> Retention
                      policy
                    </button>
                    <button
                      type="button"
                      className="menu-item"
                      onClick={() =>
                        run(`flush-${bucket.id}`, async () => {
                          close();
                          try {
                            await dataClient({
                              kind: "cloud",
                              projectId: project.id,
                              bucketId: bucket.id,
                              label: bucket.name,
                            }).flush();
                            toast.success("Memtable flushed to an SSTable");
                            reload();
                          } catch (error) {
                            toast.failure(error);
                          }
                        })
                      }
                    >
                      <Waves size={15} aria-hidden /> Flush memtable
                    </button>
                    <button
                      type="button"
                      className="menu-item"
                      onClick={() =>
                        run(`compact-${bucket.id}`, async () => {
                          close();
                          try {
                            await dataClient({
                              kind: "cloud",
                              projectId: project.id,
                              bucketId: bucket.id,
                              label: bucket.name,
                            }).compact();
                            toast.success(
                              "Compacted: obsolete versions and tombstones dropped",
                            );
                            reload();
                          } catch (error) {
                            toast.failure(error);
                          }
                        })
                      }
                    >
                      <Layers size={15} aria-hidden /> Compact now
                    </button>
                  </>
                )}
                {project.administrable && (
                  <>
                    <div className="menu-separator" />
                    <button
                      type="button"
                      className="menu-item"
                      onClick={() => {
                        close();
                        setDeleting(bucket);
                      }}
                    >
                      <Trash2 size={15} aria-hidden /> Delete bucket
                    </button>
                  </>
                )}
              </>
            )}
          </Menu>
        </div>
      ),
    },
  ];

  return (
    <>
      <PageHeader
        title="Buckets"
        description="A bucket is one time-series database: its own write-ahead log, memtable, SSTables and retention policy."
        actions={
          project.writable && (
            <button
              type="button"
              className="btn btn-primary"
              onClick={() => setCreating(true)}
              disabled={atLimit}
              title={
                atLimit
                  ? `This project already has the maximum of ${detail.usage.buckets_limit} buckets on the hosted plan.`
                  : undefined
              }
            >
              <Plus size={15} aria-hidden /> New bucket
            </button>
          )
        }
      />

      {project.demo && (
        <div style={{ marginBottom: "var(--space-5)" }}>
          <Notice tone="info">
            The showcase project is read-only. Switch to your own workspace to
            create buckets and write data.
          </Notice>
        </div>
      )}

      <Section compact>
        <DataTable
          columns={columns}
          rows={detail.buckets}
          rowKey={(bucket) => bucket.id}
          empty={
            <EmptyState
              icon={<Database size={20} aria-hidden />}
              title="No buckets in this project"
              description="Create a bucket to start writing points. You can also load a labelled sample dataset to see the console working straight away."
              action={
                project.writable && (
                  <button
                    type="button"
                    className="btn btn-primary"
                    onClick={() => setCreating(true)}
                  >
                    <Plus size={15} aria-hidden /> Create a bucket
                  </button>
                )
              }
            />
          }
        />
        {detail.buckets.length > 0 && (
          <div className="pager">
            <span>
              {detail.buckets.length} of {detail.usage.buckets_limit} buckets ·{" "}
              {count(detail.usage.points)} points stored
            </span>
            <div style={{ minWidth: 220 }}>
              <UsageBar
                label="Point quota"
                used={detail.usage.points}
                limit={detail.usage.points_limit}
                format={count}
              />
            </div>
          </div>
        )}
      </Section>

      <Section
        title="Retention and maintenance"
        description="Retention is measured against wall-clock time and enforced approximately every 60 seconds."
      >
        <div className="stack">
          <p className="hint">
            A retention policy of <strong>forever</strong> keeps everything.
            Deleting or expiring a point removes all of its fields and writes a
            tombstone; <strong>compact</strong> is what reclaims the space.
            Applying a short policy to a bucket holding older data will expire
            that data on the next sweep — that is not reversible.
          </p>
          <p className="hint">
            <strong>On disk</strong> counts SSTable files only. Points that have
            not been flushed yet are reported separately as{" "}
            <strong>in memtable</strong>, which is why a freshly written bucket
            can show zero bytes on disk.
          </p>
        </div>
      </Section>

      {creating && (
        <CreateBucketDialog
          projectId={project.id}
          onClose={() => setCreating(false)}
          onCreated={(name) => {
            setCreating(false);
            reload();
            toast.success(`Bucket ${name} created`);
          }}
        />
      )}

      {editing && (
        <RetentionDialog
          projectId={project.id}
          bucket={editing}
          onClose={() => setEditing(null)}
          onSaved={() => {
            setEditing(null);
            reload();
            toast.success("Retention policy updated");
          }}
        />
      )}

      {deleting && (
        <ConfirmDialog
          title={`Delete ${deleting.name}?`}
          confirmLabel="Delete bucket and data"
          confirmText={deleting.name}
          busy={isBusy(`delete-${deleting.id}`)}
          description={
            <>
              <p>
                This deletes the bucket, every point in it, and any monitors
                pointing at it. It holds{" "}
                <strong>{count(deleting.points)} points</strong> and{" "}
                <strong>{bytes(deleting.size_bytes)}</strong> on disk.
              </p>
              <p style={{ marginTop: "var(--space-3)" }}>
                There is no undo. Export a snapshot first if you might want it
                back.
              </p>
            </>
          }
          onClose={() => setDeleting(null)}
          onConfirm={() =>
            run(`delete-${deleting.id}`, async () => {
              try {
                await api.deleteBucket(project.id, deleting.id);
                toast.success(`${deleting.name} deleted`);
                setDeleting(null);
                reload();
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

function CreateBucketDialog({
  projectId,
  onClose,
  onCreated,
}: {
  projectId: string;
  onClose: () => void;
  onCreated: (name: string) => void;
}) {
  const [name, setName] = useState("");
  const [retention, setRetention] = useState(0);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [sample, setSample] = useState(false);
  const toast = useToast();

  const valid = /^[A-Za-z0-9][A-Za-z0-9_-]{0,47}$/.test(name);

  async function submit(event: React.FormEvent) {
    event.preventDefault();
    setBusy(true);
    setError(null);
    try {
      const bucket = await api.createBucket(projectId, name, retention);
      if (sample) {
        const loaded = await api.loadSampleData(projectId, bucket.id);
        toast.success(`Loaded ${count(loaded.written)} sample points`);
      }
      onCreated(bucket.name);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "That did not work");
    } finally {
      setBusy(false);
    }
  }

  return (
    <Modal
      title="New bucket"
      description="Names travel into the engine's database names, so they are limited to letters, digits, underscores and hyphens."
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
            type="submit"
            form="create-bucket"
            className="btn btn-primary"
            disabled={busy || !valid}
          >
            Create bucket
          </button>
        </>
      }
    >
      <form id="create-bucket" className="stack" onSubmit={submit}>
        {error && <div className="form-error">{error}</div>}
        <label className="field">
          <span className="label">Name</span>
          <input
            className="input"
            value={name}
            onChange={(event) => setName(event.target.value)}
            placeholder="production"
            maxLength={48}
            required
            spellCheck={false}
            aria-invalid={name.length > 0 && !valid}
          />
          <span className="hint">
            {name.length > 0 && !valid
              ? "Start with a letter or digit, then letters, digits, underscores or hyphens — up to 48 characters."
              : "For example production, staging, or iot-fleet."}
          </span>
        </label>
        <label className="field">
          <span className="label">Retention</span>
          <select
            className="select"
            value={retention}
            onChange={(event) => setRetention(Number(event.target.value))}
          >
            {RETENTIONS.map((option) => (
              <option key={option.seconds} value={option.seconds}>
                {option.label}
              </option>
            ))}
          </select>
          <span className="hint">
            Points older than this are expired by a background sweep. Can be
            changed later.
          </span>
        </label>
        <label className="row" style={{ gap: "var(--space-3)" }}>
          <input
            type="checkbox"
            checked={sample}
            onChange={(event) => setSample(event.target.checked)}
          />
          <span>
            <strong style={{ display: "block" }}>
              <Sparkles size={13} aria-hidden /> Fill it with sample data
            </strong>
            <span className="hint">
              Around 360 synthetic points across cpu, mem and http_requests, so
              the explorer and dashboards have something to show.
            </span>
          </span>
        </label>
      </form>
    </Modal>
  );
}

function RetentionDialog({
  projectId,
  bucket,
  onClose,
  onSaved,
}: {
  projectId: string;
  bucket: Bucket;
  onClose: () => void;
  onSaved: () => void;
}) {
  const [seconds, setSeconds] = useState(bucket.retention_seconds);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const shortening =
    seconds > 0 &&
    (bucket.retention_seconds === 0 || seconds < bucket.retention_seconds);

  return (
    <Modal
      title={`Retention for ${bucket.name}`}
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
            className={`btn ${shortening ? "btn-danger" : "btn-primary"}`}
            disabled={busy}
            onClick={async () => {
              setBusy(true);
              setError(null);
              try {
                await api.setRetention(projectId, bucket.id, seconds);
                onSaved();
              } catch (cause) {
                setError(
                  cause instanceof Error ? cause.message : "That did not work",
                );
              } finally {
                setBusy(false);
              }
            }}
          >
            {shortening ? "Apply and expire older data" : "Save policy"}
          </button>
        </>
      }
    >
      <div className="stack">
        {error && <div className="form-error">{error}</div>}
        <label className="field">
          <span className="label">Keep points for</span>
          <select
            className="select"
            value={seconds}
            onChange={(event) => setSeconds(Number(event.target.value))}
          >
            {RETENTIONS.map((option) => (
              <option key={option.seconds} value={option.seconds}>
                {option.label}
              </option>
            ))}
          </select>
        </label>
        {shortening && (
          <Notice tone="warning" title="This will delete data">
            Points older than {duration(seconds)} will be expired on the next
            sweep, within about a minute. Export a snapshot first if you need
            them.
          </Notice>
        )}
      </div>
    </Modal>
  );
}
