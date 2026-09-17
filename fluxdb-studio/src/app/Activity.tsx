/** The workspace audit trail. */

import { useMemo, useState } from "react";
import {
  Activity as ActivityIcon,
  Database,
  KeyRound,
  Search,
  Settings,
  ShieldCheck,
  Trash2,
  Upload,
  UserPlus,
} from "lucide-react";
import {
  DataTable,
  EmptyState,
  PageHeader,
  Section,
  Spinner,
  useDebounced,
  useLoader,
  type Column,
} from "../components/ui";
import { api } from "../lib/api";
import { relative, timestamp } from "../lib/format";
import type { AuditEntry } from "../lib/types";
import { useProject } from "./ProjectContext";

/** Icon per action family, so the trail is scannable rather than a wall of text. */
function iconFor(action: string) {
  if (action.startsWith("apikey")) return KeyRound;
  if (action.startsWith("bucket")) return Database;
  if (action.startsWith("member") || action.startsWith("account"))
    return UserPlus;
  if (action.startsWith("data.write")) return Upload;
  if (action.startsWith("data.delete") || action.includes("delete"))
    return Trash2;
  if (action.startsWith("connection")) return ShieldCheck;
  return Settings;
}

const DESCRIPTIONS: Record<string, string> = {
  "bucket.create": "created a bucket",
  "bucket.delete": "deleted a bucket",
  "bucket.retention": "changed a retention policy",
  "bucket.sample": "loaded sample data",
  "apikey.create": "issued an API key",
  "apikey.revoke": "revoked an API key",
  "project.create": "created a project",
  "project.delete": "deleted a project",
  "project.update": "renamed or described a project",
  "member.add": "added a member",
  "member.invite": "invited a member",
  "member.remove": "removed a member",
  "member.role": "changed a member's role",
  "connection.create": "saved a server address",
  "connection.delete": "removed a server address",
  "org.rename": "renamed the workspace",
  "account.password": "changed their password",
  "data.write": "wrote points",
  "data.delete": "deleted points",
};

export default function Activity() {
  const { detail } = useProject();
  const [term, setTerm] = useState("");
  const search = useDebounced(term);
  const orgId = detail.org.id;

  const audit = useLoader(() => api.audit(orgId), [orgId]);

  const rows = useMemo(() => {
    const entries = audit.data?.entries ?? [];
    const needle = search.trim().toLowerCase();
    if (!needle) return entries;
    return entries.filter((entry) =>
      `${entry.actor} ${entry.action} ${entry.target} ${entry.detail}`
        .toLowerCase()
        .includes(needle),
    );
  }, [audit.data, search]);

  const columns: Column<AuditEntry>[] = [
    {
      key: "what",
      header: "Action",
      render: (entry) => {
        const Icon = iconFor(entry.action);
        return (
          <span className="row" style={{ gap: "var(--space-3)" }}>
            <span className="timeline-icon">
              <Icon size={13} aria-hidden />
            </span>
            <span>
              <strong style={{ display: "block" }}>
                {entry.actor} {DESCRIPTIONS[entry.action] ?? entry.action}
              </strong>
              <small className="cell-muted mono">
                {entry.target}
                {entry.detail ? ` · ${entry.detail}` : ""}
              </small>
            </span>
          </span>
        );
      },
    },
    {
      key: "action",
      header: "Event",
      secondary: true,
      width: "170px",
      render: (entry) => (
        <span className="mono cell-muted">{entry.action}</span>
      ),
    },
    {
      key: "at",
      header: "When",
      align: "right",
      width: "150px",
      render: (entry) => (
        <span className="cell-muted" title={timestamp(entry.at)}>
          {relative(entry.at)}
        </span>
      ),
    },
  ];

  if (audit.loading && !audit.data) {
    return (
      <>
        <PageHeader title="Activity" />
        <Section>
          <Spinner label="Loading the audit trail…" />
        </Section>
      </>
    );
  }

  if (audit.error) {
    return (
      <>
        <PageHeader title="Activity" />
        <Section>
          <EmptyState
            icon={<ShieldCheck size={20} aria-hidden />}
            title="The audit trail is not visible to you"
            description={audit.error}
          />
        </Section>
      </>
    );
  }

  return (
    <>
      <PageHeader
        title="Activity"
        description="Privileged actions in this workspace, newest first. Recorded server-side and append-only."
        actions={
          <label className="source-picker">
            <span className="visually-hidden">Filter the trail</span>
            <span className="row" style={{ gap: 6 }}>
              <Search size={14} aria-hidden className="cell-muted" />
              <input
                className="input"
                style={{ minWidth: 200 }}
                placeholder="Filter by person, action or target"
                value={term}
                onChange={(event) => setTerm(event.target.value)}
              />
            </span>
          </label>
        }
      />

      <Section compact>
        <DataTable
          columns={columns}
          rows={rows}
          rowKey={(entry) => entry.id}
          dense
          empty={
            <EmptyState
              icon={<ActivityIcon size={20} aria-hidden />}
              title={
                search
                  ? "Nothing matches that filter"
                  : "No activity recorded yet"
              }
              description={
                search
                  ? "Try a different person, action or target."
                  : "Creating buckets, issuing keys, changing roles and writing data all appear here."
              }
            />
          }
        />
        {rows.length > 0 && (
          <div className="pager">
            <span>
              {rows.length} of {audit.data?.entries.length ?? 0} recorded events
              · the 200 most recent are kept
            </span>
          </div>
        )}
      </Section>
    </>
  );
}
