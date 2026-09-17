/**
 * The AI assistant, for self-hosted connections.
 *
 * The assistant operates on `/api/v1` with an administration token: it inspects
 * real schema, can run bounded read-only SELECTs when the visitor enables that,
 * and prepares reviewed operations that the browser executes. That is the whole
 * trust model of a self-hosted server, so the panel is offered exactly when the
 * visitor has connected one — never against a hosted project, where a caller
 * has no administration token and should not have one.
 *
 * This is a thin adapter: it supplies the connection, a request function scoped
 * to that server, and the navigation the assistant asks for. The panel itself is
 * unchanged.
 */

import { useCallback, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import { Sparkles } from "lucide-react";
import Assistant from "../Assistant";
import { useDirect } from "../lib/direct";
import { useToast } from "../lib/toast";
import { useProject } from "./ProjectContext";

/** Handed to the query workspace and the explorer when the assistant asks for
 *  something to be opened there. */
export const PENDING_QUERY_KEY = "fluxdb.pendingQuery";
export const PENDING_WRITE_KEY = "fluxdb.pendingWrite";

export function AssistantLauncher({ page }: { page: string }) {
  const { server } = useDirect();
  const { detail, target } = useProject();
  const navigate = useNavigate();
  const toast = useToast();
  const [open, setOpen] = useState(false);
  const [busy, setBusy] = useState(false);

  // The assistant talks to the connected server, not to this origin. Paths
  // arrive in `/api/v1/...` form, which is exactly what that server exposes.
  const request = useCallback(
    async <T,>(path: string, options: RequestInit = {}): Promise<T> => {
      if (!server) throw new Error("No server is connected in this tab.");
      const response = await fetch(`${server.url}${path}`, {
        ...options,
        credentials: "omit",
        headers: {
          ...(options.body ? { "content-type": "application/json" } : {}),
          ...(server.token ? { authorization: `Bearer ${server.token}` } : {}),
          ...(options.headers as Record<string, string> | undefined),
        },
      });
      const raw = await response.text();
      const parsed = raw ? JSON.parse(raw) : null;
      if (!response.ok) {
        throw new Error(
          (parsed as { error?: string } | null)?.error ??
            `Request failed with status ${response.status}`,
        );
      }
      return parsed as T;
    },
    [server],
  );

  // The database the assistant reasons about: the connected server's database
  // when that is the selected source, otherwise the first it reported.
  const database = useMemo(() => {
    if (target?.kind === "direct") return target.database;
    return server?.databases[0] ?? "";
  }, [target, server]);

  if (!server) return null;

  return (
    <>
      {!open && (
        <button
          type="button"
          className="btn btn-sm assistant-launcher"
          onClick={() => setOpen(true)}
        >
          <Sparkles size={15} aria-hidden /> Ask AI
        </button>
      )}
      <Assistant
        open={open}
        onClose={() => setOpen(false)}
        database={database}
        page={page}
        serverUrl={server.url}
        token={server.token}
        online={true}
        busy={busy}
        request={request}
        onActionBusy={setBusy}
        onChanged={async (message) => {
          toast.success(message);
        }}
        onOpenQuery={(query) => {
          try {
            sessionStorage.setItem(PENDING_QUERY_KEY, query);
          } catch {
            // A query that cannot be handed over is still visible in the panel.
          }
          setOpen(false);
          navigate(`/app/p/${detail.project.id}/query`);
        }}
        onOpenWrite={(payload) => {
          try {
            sessionStorage.setItem(PENDING_WRITE_KEY, payload);
          } catch {
            // Same: the proposal stays readable in the panel.
          }
          setOpen(false);
          navigate(`/app/p/${detail.project.id}/explorer`);
        }}
      />
    </>
  );
}

/** Read and clear a value the assistant left for another screen. */
export function takePending(key: string): string | null {
  try {
    const value = sessionStorage.getItem(key);
    if (value !== null) sessionStorage.removeItem(key);
    return value;
  } catch {
    return null;
  }
}
