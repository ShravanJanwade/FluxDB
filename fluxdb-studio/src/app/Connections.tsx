/**
 * Connecting to a FluxDB server the visitor runs themselves.
 *
 * The browser talks to that server directly over `/api/v1`. Its token is held
 * in memory for the tab and is never sent to this origin — a hosted console has
 * no business holding the keys to someone else's database. The cost of that
 * choice is that the token must be re-entered after a reload, and the UI says
 * so rather than hiding it.
 */

import { useState } from "react";
import { Link } from "react-router-dom";
import {
  CheckCircle2,
  Database,
  Laptop,
  Plug,
  Plus,
  Server,
  Trash2,
  Unplug,
} from "lucide-react";
import {
  CodeBlock,
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
import { api, createDirectDatabase } from "../lib/api";
import { useDirect } from "../lib/direct";
import { relative, timestamp } from "../lib/format";
import { useToast } from "../lib/toast";
import type { Connection } from "../lib/types";
import { useProject } from "./ProjectContext";

export default function Connections() {
  const { detail, reload } = useProject();
  const { server, disconnect, refreshDatabases } = useDirect();
  const toast = useToast();
  const { run, isBusy } = useAction();
  const [connecting, setConnecting] = useState(false);
  const [registering, setRegistering] = useState(false);
  const [removing, setRemoving] = useState<Connection | null>(null);
  const [creatingDb, setCreatingDb] = useState(false);

  const columns: Column<Connection>[] = [
    {
      key: "name",
      header: "Server",
      render: (connection) => (
        <div>
          <strong>{connection.name}</strong>
          <div
            className="cell-muted mono"
            style={{ fontSize: "var(--text-xs)" }}
          >
            {connection.url}
          </div>
        </div>
      ),
    },
    {
      key: "mode",
      header: "Mode",
      render: (connection) => (
        <span className="badge">
          {connection.mode === "browser" ? "Browser-direct" : "Proxied"}
        </span>
      ),
    },
    {
      key: "created",
      header: "Added",
      secondary: true,
      render: (connection) => (
        <span className="cell-muted" title={timestamp(connection.created_at)}>
          {relative(connection.created_at)}
        </span>
      ),
    },
    {
      key: "actions",
      header: "",
      align: "right",
      width: "150px",
      render: (connection) => (
        <div className="cell-actions">
          <button
            type="button"
            className="btn btn-sm"
            onClick={() => setConnecting(true)}
          >
            <Plug size={13} aria-hidden /> Connect
          </button>
          {detail.project.writable && (
            <button
              type="button"
              className="btn btn-sm btn-ghost"
              aria-label={`Remove ${connection.name}`}
              onClick={() => setRemoving(connection)}
            >
              <Trash2 size={14} aria-hidden />
            </button>
          )}
        </div>
      ),
    },
  ];

  return (
    <>
      <PageHeader
        title="Your own servers"
        description="Point this console at a FluxDB you run. The same explorer, query workspace and dashboards work against it."
        actions={
          <>
            {detail.project.writable && (
              <button
                type="button"
                className="btn"
                onClick={() => setRegistering(true)}
              >
                <Plus size={15} aria-hidden /> Save an address
              </button>
            )}
            <button
              type="button"
              className="btn btn-primary"
              onClick={() => setConnecting(true)}
            >
              <Plug size={15} aria-hidden /> Connect a server
            </button>
          </>
        }
      />

      {server ? (
        <Section
          title="Connected"
          description="This browser tab is talking to your server directly."
          actions={
            <>
              <button
                type="button"
                className="btn btn-sm"
                onClick={() =>
                  run("refresh", async () => {
                    try {
                      await refreshDatabases();
                      toast.success("Database list refreshed");
                    } catch (error) {
                      toast.failure(error);
                    }
                  })
                }
              >
                Refresh
              </button>
              <button
                type="button"
                className="btn btn-sm"
                onClick={() => {
                  disconnect();
                  toast.notify(
                    "info",
                    "Disconnected. The token was discarded.",
                  );
                }}
              >
                <Unplug size={14} aria-hidden /> Disconnect
              </button>
            </>
          }
        >
          <div className="stack">
            <dl className="kv">
              <dt>Address</dt>
              <dd className="mono">{server.url}</dd>
              <dt>Engine version</dt>
              <dd className="mono">{server.version}</dd>
              <dt>Token</dt>
              <dd>
                {server.authenticated ? (
                  <span className="badge badge-success">
                    <CheckCircle2 size={11} aria-hidden /> Accepted
                  </span>
                ) : (
                  <span className="badge badge-warning">
                    Rejected — data endpoints will refuse requests
                  </span>
                )}
              </dd>
              <dt>Databases</dt>
              <dd>
                {server.databases.length === 0
                  ? "None yet"
                  : server.databases.join(", ")}
              </dd>
              <dt>Connected</dt>
              <dd>{relative(server.connectedAt)}</dd>
            </dl>
            <div className="row">
              <Link className="btn" to={`/app/p/${detail.project.id}/explorer`}>
                <Database size={15} aria-hidden /> Open in the explorer
              </Link>
              <button
                type="button"
                className="btn"
                onClick={() => setCreatingDb(true)}
              >
                <Plus size={15} aria-hidden /> Create a database there
              </button>
            </div>
            <Notice tone="info">
              Your server's databases now appear in the data source picker
              alongside this project's buckets. Nothing about your server, and
              in particular not its token, is stored here — reloading this page
              ends the connection.
            </Notice>
          </div>
        </Section>
      ) : (
        <Section>
          <EmptyState
            icon={<Server size={20} aria-hidden />}
            title="No server connected in this tab"
            description="Run FluxDB locally, then connect to it. Everything the console can do against a hosted bucket it can do against your own instance."
            action={
              <button
                type="button"
                className="btn btn-primary"
                onClick={() => setConnecting(true)}
              >
                <Plug size={15} aria-hidden /> Connect a server
              </button>
            }
          />
        </Section>
      )}

      <Section
        title="Saved addresses"
        description="Addresses this project has recorded. Tokens are never saved — you supply one when you connect."
        compact
      >
        <DataTable
          columns={columns}
          rows={detail.connections}
          rowKey={(connection) => connection.id}
          empty={
            <EmptyState
              icon={<Laptop size={20} aria-hidden />}
              title="No saved addresses"
              description="Saving an address is a convenience for your team; it does not grant anyone access."
            />
          }
        />
      </Section>

      <Section title="Running FluxDB yourself">
        <div className="stack">
          <p className="hint">
            The fastest path is the repository's one-command launcher, which
            builds the engine, installs the console's dependencies and waits for
            both to come up.
          </p>
          <CodeBlock
            language="bash"
            code={`git clone https://github.com/ShravanJanwade/FluxDB.git
cd FluxDB
node start-all.js
# API on http://127.0.0.1:8086, console on http://127.0.0.1:5173`}
          />
          <p className="hint">
            To let <em>this</em> page reach a server on your machine, the server
            has to allow this origin and you have to give it a token:
          </p>
          <CodeBlock
            language="bash"
            code={`export FLUXDB_TOKEN="$(python -c 'import secrets; print(secrets.token_hex(32))')"
export FLUXDB_CORS_ORIGINS="${location.origin}"
cd fluxdb && cargo run --release -p fluxdb-server --bin fluxdb`}
          />
          <Notice tone="warning" title="Two things browsers do here">
            A page served over HTTPS may ask your permission before it can reach
            a server on your local network — allow it when prompted. And a
            server bound to a non-loopback address refuses to start without a
            token of at least 32 characters, on purpose.
          </Notice>
        </div>
      </Section>

      <Section title="Browser-direct or proxied">
        <dl className="kv">
          <dt>Browser-direct</dt>
          <dd>
            The default, and the right choice for a server on your own machine.
            Requests go from your browser to your server; this host is not
            involved and never sees the token.
          </dd>
          <dt>Proxied</dt>
          <dd>
            For a server on the public internet that your browser cannot reach
            directly. This host forwards the request, with the token supplied
            per request and never stored. Only FluxDB's own <code>/api/v1</code>{" "}
            paths are forwarded, and targets that resolve inside a private
            network are refused.
          </dd>
        </dl>
      </Section>

      {connecting && (
        <ConnectDialog
          onClose={() => setConnecting(false)}
          onConnected={(url) => {
            setConnecting(false);
            toast.success(`Connected to ${url}`);
          }}
        />
      )}

      {registering && (
        <RegisterDialog
          projectId={detail.project.id}
          onClose={() => setRegistering(false)}
          onSaved={() => {
            setRegistering(false);
            reload();
            toast.success("Address saved");
          }}
        />
      )}

      {creatingDb && server && (
        <CreateDatabaseDialog
          onClose={() => setCreatingDb(false)}
          onCreated={async (name) => {
            setCreatingDb(false);
            await refreshDatabases();
            toast.success(`Database ${name} created on your server`);
          }}
        />
      )}

      {removing && (
        <ConfirmDialog
          title={`Remove ${removing.name}?`}
          confirmLabel="Remove address"
          busy={isBusy("remove")}
          description="This removes the saved address from the project. Your server and its data are untouched."
          onClose={() => setRemoving(null)}
          onConfirm={() =>
            run("remove", async () => {
              try {
                await api.deleteConnection(detail.project.id, removing.id);
                setRemoving(null);
                reload();
                toast.success("Address removed");
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

function ConnectDialog({
  onClose,
  onConnected,
}: {
  onClose: () => void;
  onConnected: (url: string) => void;
}) {
  const { connect, recent } = useDirect();
  const { detail } = useProject();
  const [url, setUrl] = useState(recent[0] ?? "http://127.0.0.1:8086");
  const [token, setToken] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  return (
    <Modal
      title="Connect to your FluxDB server"
      description="The token stays in this browser tab. It is not sent to this host and not stored anywhere."
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
            disabled={busy || url.trim().length === 0}
            onClick={async () => {
              setBusy(true);
              setError(null);
              try {
                const connected = await connect(url, token);
                onConnected(connected.url);
              } catch (cause) {
                setError(
                  cause instanceof Error ? cause.message : "Could not connect",
                );
              } finally {
                setBusy(false);
              }
            }}
          >
            {busy ? "Connecting…" : "Connect"}
          </button>
        </>
      }
    >
      <div className="stack">
        {error && <div className="form-error">{error}</div>}
        <label className="field">
          <span className="label">Server address</span>
          <input
            className="input"
            value={url}
            onChange={(event) => setUrl(event.target.value)}
            placeholder="http://127.0.0.1:8086"
            spellCheck={false}
          />
          <span className="hint">The base address only, without a path.</span>
        </label>
        {(recent.length > 0 || detail.connections.length > 0) && (
          <div className="chip-list">
            {[
              ...new Set([
                ...recent,
                ...detail.connections.map((entry) => entry.url),
              ]),
            ]
              .slice(0, 6)
              .map((entry) => (
                <button
                  key={entry}
                  type="button"
                  className="chip"
                  onClick={() => setUrl(entry)}
                >
                  {entry}
                </button>
              ))}
          </div>
        )}
        <label className="field">
          <span className="label">Administration token</span>
          <input
            className="input"
            type="password"
            value={token}
            onChange={(event) => setToken(event.target.value)}
            placeholder="FLUXDB_TOKEN"
            autoComplete="off"
            spellCheck={false}
          />
          <span className="hint">
            The value of <code>FLUXDB_TOKEN</code> on that server. Leave it
            empty if the server runs without one — a loopback-only server is
            allowed to.
          </span>
        </label>
        <Notice tone="info" title="What happens next">
          The console checks <code>/api/v1/health</code> and lists the server's
          databases. If the token is rejected you will be told that
          specifically, rather than being shown a generic failure.
        </Notice>
      </div>
    </Modal>
  );
}

function RegisterDialog({
  projectId,
  onClose,
  onSaved,
}: {
  projectId: string;
  onClose: () => void;
  onSaved: () => void;
}) {
  const [name, setName] = useState("");
  const [url, setUrl] = useState("http://127.0.0.1:8086");
  const [mode, setMode] = useState<"browser" | "proxy">("browser");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  return (
    <Modal
      title="Save a server address"
      description="Recorded against this project so your team can find it. No credential is stored."
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
            disabled={busy || name.trim().length === 0}
            onClick={async () => {
              setBusy(true);
              setError(null);
              try {
                await api.createConnection(
                  projectId,
                  name.trim(),
                  url.trim(),
                  mode,
                );
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
            Save address
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
            placeholder="Laptop · dev"
          />
        </label>
        <label className="field">
          <span className="label">Address</span>
          <input
            className="input"
            value={url}
            onChange={(event) => setUrl(event.target.value)}
            spellCheck={false}
          />
        </label>
        <label className="field">
          <span className="label">Mode</span>
          <select
            className="select"
            value={mode}
            onChange={(event) =>
              setMode(event.target.value as "browser" | "proxy")
            }
          >
            <option value="browser">
              Browser-direct — your browser reaches it
            </option>
            <option value="proxy">Proxied — this host forwards requests</option>
          </select>
          <span className="hint">
            {mode === "browser"
              ? "Nothing transits this host. Right for anything on your own machine or network."
              : "Requires an https address that resolves to a public host. The token is supplied per request and never stored."}
          </span>
        </label>
        {mode === "proxy" && (
          <Notice tone="warning">
            Proxied addresses are screened: plain HTTP and anything resolving
            into a private range is refused, so this endpoint cannot be used to
            probe the deployment's own network.
          </Notice>
        )}
      </div>
    </Modal>
  );
}

function CreateDatabaseDialog({
  onClose,
  onCreated,
}: {
  onClose: () => void;
  onCreated: (name: string) => void | Promise<void>;
}) {
  const { server } = useDirect();
  const [name, setName] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const valid = /^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$/.test(name);

  return (
    <Modal
      title="Create a database on your server"
      description={`On ${server?.url}. Names may use letters, digits, underscores and hyphens.`}
      onClose={onClose}
      width={440}
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
                await createDirectDatabase(server!.url, server!.token, name);
                await onCreated(name);
              } catch (cause) {
                setError(
                  cause instanceof Error ? cause.message : "That did not work",
                );
              } finally {
                setBusy(false);
              }
            }}
          >
            Create database
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
            spellCheck={false}
            placeholder="observability"
            aria-invalid={name.length > 0 && !valid}
          />
        </label>
      </div>
    </Modal>
  );
}
