/** Workspace members, roles and pending invitations. */

import { useState } from "react";
import { Mail, ShieldCheck, Trash2, UserPlus, Users } from "lucide-react";
import {
  ConfirmDialog,
  DataTable,
  EmptyState,
  Modal,
  Notice,
  PageHeader,
  Section,
  Spinner,
  useAction,
  useLoader,
  type Column,
} from "../components/ui";
import { api } from "../lib/api";
import { initials, relative, timestamp } from "../lib/format";
import { useSession } from "../lib/session";
import { useToast } from "../lib/toast";
import type { Invite, Member, Role } from "../lib/types";
import { useProject } from "./ProjectContext";

const ROLE_SUMMARY: Record<Role, string> = {
  viewer: "Read dashboards, run queries, export snapshots",
  member: "Also write data and manage buckets, dashboards and monitors",
  admin: "Also manage projects, API keys and members",
  owner: "Also rename or delete the workspace",
};

export default function Members() {
  const { detail } = useProject();
  const { session } = useSession();
  const toast = useToast();
  const { run, isBusy } = useAction();
  const [inviting, setInviting] = useState(false);
  const [removing, setRemoving] = useState<Member | null>(null);
  const orgId = detail.org.id;

  const members = useLoader(() => api.members(orgId), [orgId]);

  if (members.loading && !members.data) {
    return (
      <>
        <PageHeader title="Members" />
        <Section>
          <Spinner label="Loading members…" />
        </Section>
      </>
    );
  }

  if (members.error) {
    return (
      <>
        <PageHeader title="Members" />
        <Section>
          <EmptyState
            icon={<ShieldCheck size={20} aria-hidden />}
            title="Members are not visible to you"
            description={members.error}
          />
        </Section>
      </>
    );
  }

  const data = members.data!;
  const canAdminister = data.role === "admin" || data.role === "owner";

  const columns: Column<Member>[] = [
    {
      key: "person",
      header: "Member",
      render: (member) => (
        <span className="row" style={{ gap: "var(--space-3)" }}>
          <span className="avatar" aria-hidden>
            {initials(member.name || member.email)}
          </span>
          <span>
            <strong style={{ display: "block" }}>
              {member.name || member.email}
              {member.is_you && (
                <span className="badge" style={{ marginLeft: 8 }}>
                  You
                </span>
              )}
            </strong>
            <small className="cell-muted">{member.email}</small>
          </span>
        </span>
      ),
    },
    {
      key: "role",
      header: "Role",
      width: "170px",
      render: (member) =>
        canAdminister && member.role !== "owner" ? (
          <select
            className="select"
            value={member.role}
            disabled={isBusy(`role-${member.account_id}`)}
            onChange={(event) =>
              run(`role-${member.account_id}`, async () => {
                try {
                  await api.updateMember(
                    orgId,
                    member.account_id,
                    event.target.value as Role,
                  );
                  toast.success(
                    `${member.email} is now a ${event.target.value}`,
                  );
                  members.reload();
                } catch (error) {
                  toast.failure(error);
                }
              })
            }
          >
            <option value="viewer">Viewer</option>
            <option value="member">Member</option>
            <option value="admin">Admin</option>
          </select>
        ) : (
          <span className="badge badge-accent">{member.role}</span>
        ),
    },
    {
      key: "joined",
      header: "Joined",
      secondary: true,
      render: (member) => (
        <span className="cell-muted" title={timestamp(member.created_at)}>
          {relative(member.created_at)}
        </span>
      ),
    },
    {
      key: "actions",
      header: "",
      align: "right",
      width: "60px",
      render: (member) =>
        (canAdminister || member.is_you) && member.role !== "owner" ? (
          <div className="cell-actions">
            <button
              type="button"
              className="btn btn-sm btn-ghost"
              aria-label={`Remove ${member.email}`}
              onClick={() => setRemoving(member)}
            >
              <Trash2 size={14} aria-hidden />
            </button>
          </div>
        ) : null,
    },
  ];

  const inviteColumns: Column<Invite>[] = [
    {
      key: "email",
      header: "Invited address",
      render: (invite) => <span className="mono">{invite.email}</span>,
    },
    {
      key: "role",
      header: "Role",
      render: (invite) => <span className="badge">{invite.role}</span>,
    },
    {
      key: "sent",
      header: "Created",
      render: (invite) => (
        <span className="cell-muted">{relative(invite.created_at)}</span>
      ),
    },
  ];

  return (
    <>
      <PageHeader
        title="Members"
        description={`Roles apply across every project in ${detail.org.name}.`}
        actions={
          canAdminister &&
          session?.account.kind !== "guest" && (
            <button
              type="button"
              className="btn btn-primary"
              onClick={() => setInviting(true)}
            >
              <UserPlus size={15} aria-hidden /> Invite someone
            </button>
          )
        }
      />

      {detail.org.id.startsWith("org_fluxdb_demo") && (
        <div style={{ marginBottom: "var(--space-5)" }}>
          <Notice tone="info">
            Membership of the shared showcase workspace is automatic — every
            account is added as a viewer. Your own workspace is in the switcher.
          </Notice>
        </div>
      )}

      <Section compact>
        <DataTable
          columns={columns}
          rows={data.members}
          rowKey={(member) => member.account_id}
          empty={
            <EmptyState
              icon={<Users size={20} aria-hidden />}
              title="No members"
              description="That should not be possible — every workspace has an owner."
            />
          }
        />
      </Section>

      {data.invites.length > 0 && (
        <Section
          compact
          title="Pending invitations"
          description="Applied automatically when the address registers."
        >
          <DataTable
            columns={inviteColumns}
            rows={data.invites}
            rowKey={(invite) => invite.id}
          />
        </Section>
      )}

      <Section title="What each role can do">
        <dl className="kv">
          {(Object.keys(ROLE_SUMMARY) as Role[]).map((role) => (
            <div key={role} style={{ display: "contents" }}>
              <dt>
                <span className="badge badge-accent">{role}</span>
              </dt>
              <dd>{ROLE_SUMMARY[role]}</dd>
            </div>
          ))}
        </dl>
        <div style={{ marginTop: "var(--space-4)" }}>
          <Notice tone="warning" title="No email is sent">
            This deployment has no mail provider. An invitation to an address
            that has no account is stored and applied the first time that
            address signs up — share the sign-up link yourself.
          </Notice>
        </div>
      </Section>

      {inviting && (
        <InviteDialog
          orgId={orgId}
          onClose={() => setInviting(false)}
          onInvited={(message) => {
            setInviting(false);
            members.reload();
            toast.success(message);
          }}
        />
      )}

      {removing && (
        <ConfirmDialog
          title={
            removing.is_you
              ? "Leave this workspace?"
              : `Remove ${removing.email}?`
          }
          confirmLabel={removing.is_you ? "Leave workspace" : "Remove member"}
          busy={isBusy("remove-member")}
          description={
            removing.is_you
              ? "You will lose access to every project in this workspace. An admin can add you back."
              : "They lose access to every project in this workspace immediately. Data they wrote is not affected."
          }
          onClose={() => setRemoving(null)}
          onConfirm={() =>
            run("remove-member", async () => {
              try {
                await api.removeMember(orgId, removing.account_id);
                toast.success(
                  removing.is_you ? "You left the workspace" : "Member removed",
                );
                setRemoving(null);
                if (removing.is_you) {
                  window.location.href = "/app";
                } else {
                  members.reload();
                }
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

function InviteDialog({
  orgId,
  onClose,
  onInvited,
}: {
  orgId: string;
  onClose: () => void;
  onInvited: (message: string) => void;
}) {
  const [email, setEmail] = useState("");
  const [role, setRole] = useState<Role>("member");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  return (
    <Modal
      title="Invite someone"
      description="If they already have a FluxDB account they join immediately. Otherwise the invitation waits for them to register."
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
            disabled={busy || !email.includes("@")}
            onClick={async () => {
              setBusy(true);
              setError(null);
              try {
                const result = await api.addMember(orgId, email.trim(), role);
                onInvited(
                  result.status === "added"
                    ? `${result.email} joined as ${result.role}`
                    : `${result.email} will join as ${result.role} when they register`,
                );
              } catch (cause) {
                setError(
                  cause instanceof Error ? cause.message : "That did not work",
                );
              } finally {
                setBusy(false);
              }
            }}
          >
            <Mail size={15} aria-hidden /> Send invitation
          </button>
        </>
      }
    >
      <div className="stack">
        {error && <div className="form-error">{error}</div>}
        <label className="field">
          <span className="label">Email address</span>
          <input
            className="input"
            type="email"
            value={email}
            onChange={(event) => setEmail(event.target.value)}
            placeholder="colleague@example.com"
          />
        </label>
        <label className="field">
          <span className="label">Role</span>
          <select
            className="select"
            value={role}
            onChange={(event) => setRole(event.target.value as Role)}
          >
            <option value="viewer">Viewer</option>
            <option value="member">Member</option>
            <option value="admin">Admin</option>
          </select>
          <span className="hint">{ROLE_SUMMARY[role]}</span>
        </label>
      </div>
    </Modal>
  );
}
