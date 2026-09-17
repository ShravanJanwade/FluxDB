/**
 * Project API keys, and the snippets that use them.
 *
 * A key's secret exists exactly once, in the response that created it: only its
 * SHA-256 is stored. The dialog therefore makes copying it the primary action
 * and says plainly that it cannot be shown again.
 */

import { useMemo, useState } from "react";
import {
  AlertTriangle,
  KeyRound,
  Plus,
  ShieldCheck,
  Trash2,
} from "lucide-react";
import {
  CodeBlock,
  ConfirmDialog,
  CopyButton,
  DataTable,
  EmptyState,
  Modal,
  Notice,
  PageHeader,
  Section,
  Tabs,
  useAction,
  type Column,
} from "../components/ui";
import { api } from "../lib/api";
import { relative, timestamp } from "../lib/format";
import { useToast } from "../lib/toast";
import type { ApiKey, Scope } from "../lib/types";
import { useProject } from "./ProjectContext";

export default function ApiKeys() {
  const { detail, reload } = useProject();
  const toast = useToast();
  const { run, isBusy } = useAction();
  const [creating, setCreating] = useState(false);
  const [issued, setIssued] = useState<{ token: string; key: ApiKey } | null>(
    null,
  );
  const [revoking, setRevoking] = useState<ApiKey | null>(null);

  if (!detail.project.administrable) {
    return (
      <>
        <PageHeader title="API keys" />
        <Section>
          <EmptyState
            icon={<ShieldCheck size={20} aria-hidden />}
            title="Your role does not manage API keys"
            description="Only workspace admins and owners can issue or revoke keys. Ask an admin for one, or for a role change."
          />
        </Section>
      </>
    );
  }

  const columns: Column<ApiKey>[] = [
    {
      key: "name",
      header: "Key",
      render: (key) => (
        <div>
          <strong>{key.name}</strong>
          <div
            className="cell-muted mono"
            style={{ fontSize: "var(--text-xs)" }}
          >
            {key.masked_token}
          </div>
        </div>
      ),
    },
    {
      key: "scopes",
      header: "Scopes",
      render: (key) => (
        <span className="row" style={{ gap: 6 }}>
          {key.scopes.map((scope) => (
            <span key={scope} className="badge">
              {scope}
            </span>
          ))}
        </span>
      ),
    },
    {
      key: "used",
      header: "Last used",
      render: (key) =>
        key.last_used_at ? (
          <span title={timestamp(key.last_used_at)}>
            {relative(key.last_used_at)}
          </span>
        ) : (
          <span className="cell-muted">never</span>
        ),
    },
    {
      key: "created",
      header: "Created",
      secondary: true,
      render: (key) => (
        <span className="cell-muted" title={timestamp(key.created_at)}>
          {relative(key.created_at)}
        </span>
      ),
    },
    {
      key: "actions",
      header: "",
      align: "right",
      width: "110px",
      render: (key) => (
        <div className="cell-actions">
          <button
            type="button"
            className="btn btn-sm btn-ghost"
            onClick={() => setRevoking(key)}
          >
            <Trash2 size={14} aria-hidden /> Revoke
          </button>
        </div>
      ),
    },
  ];

  return (
    <>
      <PageHeader
        title="API keys"
        description="Scoped tokens that let an agent, a script or an SDK write to and read from this project's buckets."
        actions={
          <button
            type="button"
            className="btn btn-primary"
            onClick={() => setCreating(true)}
          >
            <Plus size={15} aria-hidden /> New key
          </button>
        }
      />

      <Section compact>
        <DataTable
          columns={columns}
          rows={detail.keys}
          rowKey={(key) => key.id}
          empty={
            <EmptyState
              icon={<KeyRound size={20} aria-hidden />}
              title="No API keys yet"
              description="Issue a key to send data from outside the console — from a metrics agent, a cron job, or your application."
              action={
                <button
                  type="button"
                  className="btn btn-primary"
                  onClick={() => setCreating(true)}
                >
                  Create your first key
                </button>
              }
            />
          }
        />
      </Section>

      <IngestionGuide
        buckets={detail.buckets.map((bucket) => bucket.name)}
        hasKey={detail.keys.length > 0}
      />

      <Section title="How keys are stored">
        <dl className="kv">
          <dt>Format</dt>
          <dd>
            <code>fdbk_&#123;id&#125;_&#123;secret&#125;</code>. The id is
            public and appears in this table; the secret is 256 random bits.
          </dd>
          <dt>At rest</dt>
          <dd>
            Only the SHA-256 of the secret is stored. A key cannot be recovered
            from the control-plane database, and it cannot be shown again after
            it is issued.
          </dd>
          <dt>Why not Argon2</dt>
          <dd>
            A password-stretching KDF would add tens of milliseconds to every
            ingest request and buys nothing against a secret with that much
            entropy. Account passwords do use Argon2id.
          </dd>
          <dt>Revocation</dt>
          <dd>
            Immediate, and on the next request. A revoked key is kept marked
            rather than deleted, so its audit trail and last-used time survive.
          </dd>
        </dl>
      </Section>

      {creating && (
        <CreateKeyDialog
          onClose={() => setCreating(false)}
          onIssued={(result) => {
            setCreating(false);
            setIssued(result);
            reload();
          }}
        />
      )}

      {issued && (
        <Modal
          title="Copy your key now"
          onClose={() => setIssued(null)}
          width={560}
          footer={
            <button
              type="button"
              className="btn btn-primary"
              onClick={() => setIssued(null)}
            >
              I have copied it
            </button>
          }
        >
          <div className="secret-reveal">
            <div className="row" style={{ gap: "var(--space-2)" }}>
              <AlertTriangle size={16} aria-hidden />
              <strong>This is the only time this secret is shown.</strong>
            </div>
            <div className="secret-value">
              <span>{issued.token}</span>
              <span className="row-end">
                <CopyButton value={issued.token} label="Copy" />
              </span>
            </div>
            <p className="hint">
              Store it in your secret manager or your agent's configuration. If
              you lose it, revoke the key and issue another — there is no way to
              read it back.
            </p>
          </div>
          <div style={{ marginTop: "var(--space-5)" }}>
            <CodeBlock
              language="bash"
              code={`curl -X POST '${location.origin}/api/ingest/v1/write?bucket=${detail.buckets[0]?.name ?? "production"}&precision=s' \\
  -H 'Authorization: Bearer ${issued.token}' \\
  --data-binary 'cpu,host=api-01 usage=42.8 ${Math.floor(Date.now() / 1000)}'`}
            />
          </div>
        </Modal>
      )}

      {revoking && (
        <ConfirmDialog
          title={`Revoke ${revoking.name}?`}
          confirmLabel="Revoke key"
          busy={isBusy("revoke")}
          description={
            <>
              <p>
                Anything using this key stops working immediately. Stored data
                is not affected.
              </p>
              <p style={{ marginTop: "var(--space-3)" }}>
                Key id <code>{revoking.id}</code>
                {revoking.last_used_at
                  ? `, last used ${relative(revoking.last_used_at)}.`
                  : ", never used."}
              </p>
            </>
          }
          onClose={() => setRevoking(null)}
          onConfirm={() =>
            run("revoke", async () => {
              try {
                await api.revokeKey(detail.project.id, revoking.id);
                toast.success(`${revoking.name} revoked`);
                setRevoking(null);
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

function CreateKeyDialog({
  onClose,
  onIssued,
}: {
  onClose: () => void;
  onIssued: (result: { token: string; key: ApiKey }) => void;
}) {
  const { detail } = useProject();
  const [name, setName] = useState("");
  const [scopes, setScopes] = useState<Scope[]>(["read", "write"]);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  function toggle(scope: Scope) {
    setScopes((current) =>
      current.includes(scope)
        ? current.filter((entry) => entry !== scope)
        : [...current, scope],
    );
  }

  return (
    <Modal
      title="New API key"
      description="Name it after the thing that will use it, so the audit trail and the last-used column stay meaningful."
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
            disabled={busy || name.trim().length === 0 || scopes.length === 0}
            onClick={async () => {
              setBusy(true);
              setError(null);
              try {
                const result = await api.createKey(
                  detail.project.id,
                  name.trim(),
                  scopes,
                );
                onIssued({ token: result.token, key: result.key });
              } catch (cause) {
                setError(
                  cause instanceof Error ? cause.message : "That did not work",
                );
              } finally {
                setBusy(false);
              }
            }}
          >
            Create key
          </button>
        </>
      }
    >
      <div className="stack">
        {error && <div className="form-error">{error}</div>}
        <label className="field">
          <span className="label">Name</span>
          <input
            className="input"
            value={name}
            onChange={(event) => setName(event.target.value)}
            maxLength={64}
            placeholder="telegraf on api-01"
          />
        </label>
        <fieldset
          className="field"
          style={{ border: 0, padding: 0, margin: 0 }}
        >
          <span className="label">Scopes</span>
          <label className="row" style={{ gap: "var(--space-3)" }}>
            <input
              type="checkbox"
              checked={scopes.includes("write")}
              onChange={() => toggle("write")}
            />
            <span>
              <strong style={{ display: "block" }}>write</strong>
              <span className="hint">
                Send points over line protocol or JSON into this project's
                buckets.
              </span>
            </span>
          </label>
          <label className="row" style={{ gap: "var(--space-3)" }}>
            <input
              type="checkbox"
              checked={scopes.includes("read")}
              onChange={() => toggle("read")}
            />
            <span>
              <strong style={{ display: "block" }}>read</strong>
              <span className="hint">
                Run SQL against this project's buckets, for a Grafana-style
                reader or a report job.
              </span>
            </span>
          </label>
        </fieldset>
        <Notice tone="info">
          A key is scoped to this project only. It cannot reach another project,
          another workspace, or the console's own administration endpoints.
        </Notice>
      </div>
    </Modal>
  );
}

function IngestionGuide({
  buckets,
  hasKey,
}: {
  buckets: string[];
  hasKey: boolean;
}) {
  const [tab, setTab] = useState<"line" | "json" | "query" | "python">("line");
  const bucket = buckets[0] ?? "production";
  const origin = useMemo(() => location.origin, []);
  const seconds = Math.floor(Date.now() / 1000);

  const snippets: Record<typeof tab, { language: string; code: string }> = {
    line: {
      language: "bash",
      code: `# InfluxDB line protocol — what most metrics agents already emit.
curl -X POST '${origin}/api/ingest/v1/write?bucket=${bucket}&precision=s' \\
  -H 'Authorization: Bearer fdbk_YOUR_KEY' \\
  --data-binary '
cpu,host=api-01,region=us-east-1 usage=42.8,cores=8i ${seconds}
cpu,host=api-02,region=us-east-1 usage=37.1,cores=8i ${seconds}
http_requests,service=checkout requests=2412i,errors=3i,latency_p99=184.2 ${seconds}'`,
    },
    json: {
      language: "bash",
      code: `# JSON batch. Timestamps are decimal nanosecond strings; exact
# 64-bit integers use {"integer": "…"} so JSON cannot round them.
curl -X POST '${origin}/api/ingest/v1/points?bucket=${bucket}' \\
  -H 'Authorization: Bearer fdbk_YOUR_KEY' \\
  -H 'Content-Type: application/json' \\
  -d '{"points":[{
        "measurement":"cpu",
        "tags":{"host":"api-01","region":"us-east-1"},
        "timestamp":"${seconds}000000000",
        "fields":{"usage":42.8,"cores":{"integer":"8"},"healthy":true}
      }]}'`,
    },
    query: {
      language: "bash",
      code: `# Read back with SQL. $timeFilter and $interval are expanded by the
# server from the from/to/interval fields.
curl -X POST '${origin}/api/ingest/v1/query' \\
  -H 'Authorization: Bearer fdbk_YOUR_KEY' \\
  -H 'Content-Type: application/json' \\
  -d '{"bucket":"${bucket}",
       "interval":"5m",
       "query":"SELECT MEAN(usage) AS cpu FROM cpu WHERE $timeFilter GROUP BY time($interval), host"}'

# Check what a key can reach:
curl '${origin}/api/ingest/v1/whoami' -H 'Authorization: Bearer fdbk_YOUR_KEY'`,
    },
    python: {
      language: "python",
      code: `import os, time, urllib.request, json

KEY = os.environ["FLUXDB_KEY"]          # fdbk_…
URL = "${origin}/api/ingest/v1/points?bucket=${bucket}"

payload = {"points": [{
    "measurement": "cpu",
    "tags": {"host": "api-01"},
    "timestamp": f"{time.time_ns()}",
    "fields": {"usage": 42.8, "cores": {"integer": "8"}},
}]}

request = urllib.request.Request(
    URL,
    data=json.dumps(payload).encode(),
    headers={"Authorization": f"Bearer {KEY}", "Content-Type": "application/json"},
)
with urllib.request.urlopen(request) as response:
    print(json.load(response))`,
    },
  };

  return (
    <Section
      title="Sending data"
      description={
        hasKey
          ? "Replace fdbk_YOUR_KEY with the key you copied."
          : "Create a key above, then use it in place of fdbk_YOUR_KEY."
      }
      actions={
        <Tabs
          value={tab}
          onChange={setTab}
          tabs={[
            { id: "line" as const, label: "Line protocol" },
            { id: "json" as const, label: "JSON" },
            { id: "query" as const, label: "Query" },
            { id: "python" as const, label: "Python" },
          ]}
        />
      }
    >
      <div className="stack">
        <CodeBlock
          language={snippets[tab].language}
          code={snippets[tab].code}
        />
        <Notice tone="success">
          Points written with a key appear immediately in the data explorer and
          in any dashboard panel that queries that bucket.
        </Notice>
      </div>
    </Section>
  );
}
