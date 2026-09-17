/** Project settings, workspace settings, and account settings. */

import { useState } from "react";
import { useNavigate } from "react-router-dom";
import {
  AlertTriangle,
  Building2,
  FolderKanban,
  Save,
  Trash2,
} from "lucide-react";
import {
  ConfirmDialog,
  Notice,
  PageHeader,
  Section,
  UsageBar,
  useAction,
} from "../components/ui";
import { api } from "../lib/api";
import { bytes, count, relative, timestamp } from "../lib/format";
import { useSession } from "../lib/session";
import { useToast } from "../lib/toast";
import { useProject } from "./ProjectContext";

export function ProjectSettings() {
  const { detail, reload } = useProject();
  const { refresh } = useSession();
  const toast = useToast();
  const navigate = useNavigate();
  const { run, isBusy } = useAction();
  const [name, setName] = useState(detail.project.name);
  const [description, setDescription] = useState(detail.project.description);
  const [orgName, setOrgName] = useState(detail.org.name);
  const [deletingProject, setDeletingProject] = useState(false);
  const [deletingOrg, setDeletingOrg] = useState(false);

  const canAdminister = detail.project.administrable;
  const isOwner = detail.project.role === "owner";
  const dirty =
    name !== detail.project.name || description !== detail.project.description;

  return (
    <>
      <PageHeader
        title="Settings"
        description={`Project and workspace configuration for ${detail.project.name}.`}
      />

      <Section
        title="Project"
        description="Shown in the switcher and on the overview."
        actions={
          <button
            type="button"
            className="btn btn-primary"
            disabled={!canAdminister || !dirty || isBusy("project")}
            onClick={() =>
              run("project", async () => {
                try {
                  await api.updateProject(detail.project.id, {
                    name,
                    description,
                  });
                  await refresh();
                  reload();
                  toast.success("Project updated");
                } catch (error) {
                  toast.failure(error);
                }
              })
            }
          >
            <Save size={15} aria-hidden /> Save changes
          </button>
        }
      >
        <div className="stack" style={{ maxWidth: 560 }}>
          {!canAdminister && (
            <Notice tone="info">
              {detail.project.demo
                ? "The shared showcase project cannot be changed."
                : "Only workspace admins and owners can change project settings."}
            </Notice>
          )}
          <label className="field">
            <span className="label">Name</span>
            <input
              className="input"
              value={name}
              onChange={(event) => setName(event.target.value)}
              maxLength={64}
              disabled={!canAdminister}
            />
          </label>
          <label className="field">
            <span className="label">Description</span>
            <textarea
              className="input"
              rows={3}
              value={description}
              onChange={(event) => setDescription(event.target.value)}
              maxLength={280}
              disabled={!canAdminister}
            />
            <span className="hint">Up to 280 characters.</span>
          </label>
          <dl className="kv">
            <dt>Project id</dt>
            <dd className="mono">{detail.project.id}</dd>
            <dt>Created</dt>
            <dd>{timestamp(detail.project.created_at)}</dd>
            <dt>Your role</dt>
            <dd>
              <span className="badge badge-accent">{detail.project.role}</span>
            </dd>
          </dl>
        </div>
      </Section>

      <Section title="Usage" description="Against the hosted plan's limits.">
        <div className="stack" style={{ maxWidth: 560 }}>
          <UsageBar
            label="Points stored"
            used={detail.usage.points}
            limit={detail.usage.points_limit}
            format={count}
          />
          <UsageBar
            label="Buckets"
            used={detail.usage.buckets}
            limit={detail.usage.buckets_limit}
            format={(value) => String(value)}
          />
          <p className="hint">
            {bytes(detail.usage.size_bytes)} in SSTable files. Self-hosting
            removes every one of these limits — the console works identically
            against your own server.
          </p>
        </div>
      </Section>

      <Section
        title="Workspace"
        description={`${detail.org.name} · plan ${detail.org.plan}`}
        actions={
          <button
            type="button"
            className="btn btn-primary"
            disabled={!isOwner || orgName === detail.org.name || isBusy("org")}
            onClick={() =>
              run("org", async () => {
                try {
                  await api.renameOrg(detail.org.id, orgName);
                  await refresh();
                  reload();
                  toast.success("Workspace renamed");
                } catch (error) {
                  toast.failure(error);
                }
              })
            }
          >
            <Building2 size={15} aria-hidden /> Rename workspace
          </button>
        }
      >
        <div className="stack" style={{ maxWidth: 560 }}>
          <label className="field">
            <span className="label">Workspace name</span>
            <input
              className="input"
              value={orgName}
              onChange={(event) => setOrgName(event.target.value)}
              maxLength={64}
              disabled={!isOwner}
            />
            <span className="hint">
              {isOwner
                ? "Only the owner can rename a workspace."
                : "Only the workspace owner can rename it."}
            </span>
          </label>
        </div>
      </Section>

      {(canAdminister || isOwner) && !detail.project.demo && (
        <Section title="Danger zone">
          <div className="stack">
            {canAdminister && (
              <div className="row">
                <div>
                  <strong style={{ display: "block" }}>
                    Delete this project
                  </strong>
                  <span className="hint">
                    Removes every bucket, point, dashboard, monitor and API key
                    in it. A workspace keeps at least one project, so this is
                    refused if it is the last one.
                  </span>
                </div>
                <button
                  type="button"
                  className="btn btn-danger row-end"
                  onClick={() => setDeletingProject(true)}
                >
                  <Trash2 size={15} aria-hidden /> Delete project
                </button>
              </div>
            )}
            {isOwner && (
              <div className="row">
                <div>
                  <strong style={{ display: "block" }}>
                    Delete this workspace
                  </strong>
                  <span className="hint">
                    Removes the workspace, all of its projects and all of their
                    data. Refused if it is the only workspace you own.
                  </span>
                </div>
                <button
                  type="button"
                  className="btn btn-danger row-end"
                  onClick={() => setDeletingOrg(true)}
                >
                  <Trash2 size={15} aria-hidden /> Delete workspace
                </button>
              </div>
            )}
          </div>
        </Section>
      )}

      {deletingProject && (
        <ConfirmDialog
          title={`Delete ${detail.project.name}?`}
          confirmLabel="Delete project and all data"
          confirmText={detail.project.name}
          busy={isBusy("delete-project")}
          description={
            <p>
              This removes {detail.buckets.length} bucket
              {detail.buckets.length === 1 ? "" : "s"} holding{" "}
              <strong>{count(detail.usage.points)} points</strong>, plus this
              project's dashboards, monitors and API keys. There is no undo.
            </p>
          }
          onClose={() => setDeletingProject(false)}
          onConfirm={() =>
            run("delete-project", async () => {
              try {
                await api.deleteProject(detail.project.id);
                toast.success("Project deleted");
                await refresh();
                navigate("/app", { replace: true });
              } catch (error) {
                toast.failure(error);
                setDeletingProject(false);
              }
            })
          }
        />
      )}

      {deletingOrg && (
        <ConfirmDialog
          title={`Delete ${detail.org.name}?`}
          confirmLabel="Delete workspace and everything in it"
          confirmText={detail.org.name}
          busy={isBusy("delete-org")}
          description={
            <p>
              Every project in this workspace, every bucket, every point and
              every member's access goes with it. There is no undo.
            </p>
          }
          onClose={() => setDeletingOrg(false)}
          onConfirm={() =>
            run("delete-org", async () => {
              try {
                await api.deleteOrg(detail.org.id);
                toast.success("Workspace deleted");
                await refresh();
                navigate("/app", { replace: true });
              } catch (error) {
                toast.failure(error);
                setDeletingOrg(false);
              }
            })
          }
        />
      )}
    </>
  );
}

export function AccountSettings() {
  const { session, refresh, signOut } = useSession();
  const navigate = useNavigate();
  const toast = useToast();
  const { run, isBusy } = useAction();
  const [current, setCurrent] = useState("");
  const [next, setNext] = useState("");
  const [deleting, setDeleting] = useState(false);

  const account = session?.account;
  if (!account) return null;
  const guest = account.kind === "guest";

  return (
    <>
      <PageHeader
        title="Account"
        description="Your sign-in details and the workspaces you belong to."
      />

      <Section title="Profile">
        <dl className="kv" style={{ maxWidth: 560 }}>
          <dt>Name</dt>
          <dd>{account.name}</dd>
          <dt>Email</dt>
          <dd className="mono">{account.email}</dd>
          <dt>Account type</dt>
          <dd>
            <span
              className={`badge ${guest ? "badge-warning" : "badge-success"}`}
            >
              {guest ? "Guest" : "Registered"}
            </span>
          </dd>
          <dt>Created</dt>
          <dd>{timestamp(account.created_at)}</dd>
          {account.expires_at && (
            <>
              <dt>Removed</dt>
              <dd>
                {timestamp(account.expires_at)} ({relative(account.expires_at)})
              </dd>
            </>
          )}
        </dl>
        {guest && (
          <div style={{ marginTop: "var(--space-4)" }}>
            <Notice tone="warning" title="This is a temporary account">
              Your guest workspace and everything in it is deleted
              automatically. Create a free account to keep what you build — the
              sign-up form takes an email address and a password, nothing else.
            </Notice>
          </div>
        )}
      </Section>

      {!guest && (
        <Section
          title="Password"
          description={
            account.has_password
              ? "Changing it signs out every other session."
              : "You signed in with GitHub. Setting a password adds email sign-in as well."
          }
          actions={
            <button
              type="button"
              className="btn btn-primary"
              disabled={isBusy("password") || next.length < 10}
              onClick={() =>
                run("password", async () => {
                  try {
                    const result = await api.changePassword(
                      account.has_password ? current : null,
                      next,
                    );
                    setCurrent("");
                    setNext("");
                    await refresh();
                    toast.success(
                      result.sessions_revoked > 0
                        ? `Password updated. ${result.sessions_revoked} other session${result.sessions_revoked === 1 ? "" : "s"} signed out.`
                        : "Password updated",
                    );
                  } catch (error) {
                    toast.failure(error);
                  }
                })
              }
            >
              Update password
            </button>
          }
        >
          <div className="stack" style={{ maxWidth: 420 }}>
            {account.has_password && (
              <label className="field">
                <span className="label">Current password</span>
                <input
                  className="input"
                  type="password"
                  value={current}
                  onChange={(event) => setCurrent(event.target.value)}
                  autoComplete="current-password"
                />
              </label>
            )}
            <label className="field">
              <span className="label">New password</span>
              <input
                className="input"
                type="password"
                value={next}
                onChange={(event) => setNext(event.target.value)}
                autoComplete="new-password"
              />
              <span className="hint">
                At least 10 characters. 16 or more needs nothing else; shorter
                ones must mix three of upper case, lower case, digits and
                symbols.
              </span>
            </label>
          </div>
        </Section>
      )}

      <Section title="Workspaces">
        <div className="stack">
          {(session?.organizations ?? []).map((org) => (
            <div key={org.id} className="row">
              {org.is_demo ? (
                <Building2 size={15} aria-hidden className="cell-muted" />
              ) : (
                <FolderKanban size={15} aria-hidden className="cell-muted" />
              )}
              <strong>{org.name}</strong>
              <span className="badge badge-accent">{org.role}</span>
              <span className="cell-muted row-end">
                {org.projects.length} project
                {org.projects.length === 1 ? "" : "s"}
              </span>
            </div>
          ))}
        </div>
      </Section>

      <Section title="Danger zone">
        <div className="row">
          <div>
            <strong style={{ display: "block" }}>Delete your account</strong>
            <span className="hint">
              Workspaces you own alone are deleted with it. Ones you share must
              have ownership transferred first, so nobody else loses their data
              by accident.
            </span>
          </div>
          <button
            type="button"
            className="btn btn-danger row-end"
            onClick={() => setDeleting(true)}
          >
            <AlertTriangle size={15} aria-hidden /> Delete account
          </button>
        </div>
      </Section>

      {deleting && (
        <ConfirmDialog
          title="Delete your account?"
          confirmLabel="Delete my account"
          confirmText={account.email}
          busy={isBusy("delete-account")}
          description={
            <p>
              This removes your account, your sessions, and every workspace you
              own by yourself, including all stored points. It cannot be undone.
            </p>
          }
          onClose={() => setDeleting(false)}
          onConfirm={() =>
            run("delete-account", async () => {
              try {
                await api.deleteAccount();
                await signOut();
                navigate("/", { replace: true });
              } catch (error) {
                toast.failure(error);
                setDeleting(false);
              }
            })
          }
        />
      )}
    </>
  );
}
