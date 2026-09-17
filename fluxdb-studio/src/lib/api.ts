/**
 * HTTP access to FluxDB.
 *
 * Two transports sit behind one interface:
 *
 *  * the hosted control plane on this origin (`/api/cloud`), authenticated by
 *    the session cookie, where data is addressed as project + bucket;
 *  * a FluxDB server the visitor runs themselves, which the browser talks to
 *    directly over `/api/v1` with a token that is held in memory for the tab
 *    and never sent to this origin.
 *
 * Every screen in the console is written against `DataClient`, so the explorer,
 * the query workspace and the dashboards work unchanged against either.
 */

import type {
  AgentConfig,
  AgentRun,
  AlertEvent,
  ApiKey,
  AuditEntry,
  Bucket,
  Connection,
  Dashboard,
  DataTarget,
  DeploymentConfig,
  Invite,
  Member,
  Monitor,
  OrgDetail,
  Panel,
  Point,
  PointsPage,
  Project,
  ProjectDetail,
  PublicStats,
  QueryResult,
  Role,
  SavedAgent,
  Schema,
  Scope,
  Session,
  Telemetry,
} from "./types";

export class ApiError extends Error {
  status: number;
  code: string;

  constructor(status: number, code: string, message: string) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.code = code;
  }

  /** The caller is not signed in, or the session expired. */
  get unauthenticated() {
    return this.status === 401;
  }

  /** A hosted-plan limit was reached rather than a request being malformed. */
  get quota() {
    return this.status === 402;
  }
}

type RequestOptions = {
  method?: string;
  body?: unknown;
  /** Absolute URL; used for self-hosted servers on another origin. */
  absolute?: string;
  token?: string;
  text?: string;
  signal?: AbortSignal;
  /** The visitor's own AI provider key. Held in tab memory, never stored. */
  geminiKey?: string;
};

async function request<T>(
  path: string,
  options: RequestOptions = {},
): Promise<T> {
  const url = options.absolute ? `${options.absolute}${path}` : path;
  const headers: Record<string, string> = {};
  let body: string | undefined;
  if (options.text !== undefined) {
    headers["content-type"] = "text/plain";
    body = options.text;
  } else if (options.body !== undefined) {
    headers["content-type"] = "application/json";
    body = JSON.stringify(options.body);
  }
  if (options.token) {
    headers.authorization = `Bearer ${options.token}`;
  }
  if (options.geminiKey) {
    headers["x-gemini-api-key"] = options.geminiKey;
  }

  let response: Response;
  try {
    response = await fetch(url, {
      method: options.method ?? "GET",
      headers,
      body,
      // Cookies are needed for this origin. A self-hosted server on another
      // origin must not receive them.
      credentials: options.absolute ? "omit" : "same-origin",
      signal: options.signal,
    });
  } catch (cause) {
    if (cause instanceof DOMException && cause.name === "AbortError")
      throw cause;
    throw new ApiError(
      0,
      "network",
      options.absolute
        ? `${options.absolute} could not be reached from this browser. Check the address, that the server is running, and that it allows this page's origin.`
        : "The network request failed. Check your connection and try again.",
    );
  }

  if (response.status === 204) return undefined as T;

  const raw = await response.text();
  let parsed: unknown = null;
  if (raw) {
    try {
      parsed = JSON.parse(raw);
    } catch {
      parsed = { error: raw.slice(0, 400) };
    }
  }
  if (!response.ok) {
    const payload = (parsed ?? {}) as { error?: string; code?: string };
    throw new ApiError(
      response.status,
      payload.code ?? String(response.status),
      payload.error ?? `Request failed with status ${response.status}`,
    );
  }
  return parsed as T;
}

// ============================================================================
// Control plane
// ============================================================================

const base = "/api/cloud";

export const api = {
  config: () => request<DeploymentConfig>(`${base}/config`),

  /** Measured figures from the running instance, readable without a session. */
  publicStats: () => request<PublicStats>(`${base}/public/stats`),

  session: (signal?: AbortSignal) =>
    request<Session>(`${base}/auth/session`, { signal }),

  signUp: (email: string, password: string, name: string) =>
    request<Session>(`${base}/auth/signup`, {
      method: "POST",
      body: { email, password, name },
    }),

  signIn: (email: string, password: string) =>
    request<Session>(`${base}/auth/login`, {
      method: "POST",
      body: { email, password },
    }),

  signOut: () => request<void>(`${base}/auth/logout`, { method: "POST" }),

  startGuest: () => request<Session>(`${base}/auth/guest`, { method: "POST" }),

  changePassword: (currentPassword: string | null, newPassword: string) =>
    request<{ updated: boolean; sessions_revoked: number }>(
      `${base}/auth/password`,
      {
        method: "POST",
        body: { current_password: currentPassword, new_password: newPassword },
      },
    ),

  deleteAccount: () =>
    request<{ deleted: boolean }>(`${base}/auth/account`, { method: "DELETE" }),

  org: (orgId: string) => request<OrgDetail>(`${base}/orgs/${orgId}`),

  renameOrg: (orgId: string, name: string) =>
    request<{ id: string; name: string }>(`${base}/orgs/${orgId}`, {
      method: "PATCH",
      body: { name },
    }),

  deleteOrg: (orgId: string) =>
    request<void>(`${base}/orgs/${orgId}`, { method: "DELETE" }),

  createProject: (orgId: string, name: string, description: string) =>
    request<Project>(`${base}/orgs/${orgId}/projects`, {
      method: "POST",
      body: { name, description },
    }),

  members: (orgId: string) =>
    request<{ role: Role; members: Member[]; invites: Invite[] }>(
      `${base}/orgs/${orgId}/members`,
    ),

  addMember: (orgId: string, email: string, role: Role) =>
    request<{ status: string; email: string; role: Role; note?: string }>(
      `${base}/orgs/${orgId}/members`,
      { method: "POST", body: { email, role } },
    ),

  updateMember: (orgId: string, accountId: string, role: Role) =>
    request<{ role: Role }>(`${base}/orgs/${orgId}/members/${accountId}`, {
      method: "PATCH",
      body: { role },
    }),

  removeMember: (orgId: string, accountId: string) =>
    request<void>(`${base}/orgs/${orgId}/members/${accountId}`, {
      method: "DELETE",
    }),

  // ---- agent ----------------------------------------------------------

  agentConfig: () => request<AgentConfig>(`${base}/agent/config`),

  agentModels: (geminiKey?: string) =>
    request<{ models: { id: string; name: string }[] }>(
      `${base}/agent/models`,
      { geminiKey },
    ),

  /** One conversational turn. Runs tools server-side and returns the whole run. */
  ask: (
    projectId: string,
    messages: { role: "user" | "assistant"; text: string }[],
    options: { model?: string; page?: string; geminiKey?: string } = {},
  ) =>
    request<{ run: AgentRun; model: string }>(
      `${base}/projects/${projectId}/agent/chat`,
      {
        method: "POST",
        body: {
          messages,
          model: options.model ?? "",
          page: options.page ?? "",
        },
        geminiKey: options.geminiKey,
      },
    ),

  /** The one-click investigation: fans out per bucket, then reconciles. */
  investigate: (projectId: string, geminiKey?: string) =>
    request<{ run: AgentRun; model: string }>(
      `${base}/projects/${projectId}/agent/insights`,
      { method: "POST", geminiKey },
    ),

  savedAgents: (projectId: string) =>
    request<{ agents: SavedAgent[] }>(
      `${base}/projects/${projectId}/agent/saved`,
    ),

  createSavedAgent: (
    projectId: string,
    input: {
      name: string;
      instruction: string;
      interval_minutes: number;
      enabled: boolean;
    },
  ) =>
    request<{ agent: SavedAgent }>(
      `${base}/projects/${projectId}/agent/saved`,
      { method: "POST", body: input },
    ),

  updateSavedAgent: (
    projectId: string,
    agentId: string,
    input: {
      name: string;
      instruction: string;
      interval_minutes: number;
      enabled: boolean;
    },
  ) =>
    request<{ agent: SavedAgent }>(
      `${base}/projects/${projectId}/agent/saved/${agentId}`,
      { method: "PATCH", body: input },
    ),

  deleteSavedAgent: (projectId: string, agentId: string) =>
    request<void>(`${base}/projects/${projectId}/agent/saved/${agentId}`, {
      method: "DELETE",
    }),

  runSavedAgent: (projectId: string, agentId: string, geminiKey?: string) =>
    request<{ run: AgentRun; model: string }>(
      `${base}/projects/${projectId}/agent/saved/${agentId}/run`,
      { method: "POST", geminiKey },
    ),

  agentRuns: (projectId: string) =>
    request<{ runs: AgentRun[] }>(`${base}/projects/${projectId}/agent/runs`),

  audit: (orgId: string) =>
    request<{ entries: AuditEntry[] }>(`${base}/orgs/${orgId}/audit`),

  project: (projectId: string, signal?: AbortSignal) =>
    request<ProjectDetail>(`${base}/projects/${projectId}`, { signal }),

  updateProject: (
    projectId: string,
    patch: { name?: string; description?: string },
  ) =>
    request<Project>(`${base}/projects/${projectId}`, {
      method: "PATCH",
      body: patch,
    }),

  deleteProject: (projectId: string) =>
    request<void>(`${base}/projects/${projectId}`, { method: "DELETE" }),

  createBucket: (projectId: string, name: string, retentionSeconds: number) =>
    request<Bucket>(`${base}/projects/${projectId}/buckets`, {
      method: "POST",
      body: { name, retention_seconds: retentionSeconds },
    }),

  setRetention: (projectId: string, bucketId: string, seconds: number) =>
    request<Bucket>(`${base}/projects/${projectId}/buckets/${bucketId}`, {
      method: "PATCH",
      body: { retention_seconds: seconds },
    }),

  deleteBucket: (projectId: string, bucketId: string) =>
    request<void>(`${base}/projects/${projectId}/buckets/${bucketId}`, {
      method: "DELETE",
    }),

  loadSampleData: (projectId: string, bucketId: string) =>
    request<{ written: number; bucket: Bucket }>(
      `${base}/projects/${projectId}/buckets/${bucketId}/sample`,
      { method: "POST" },
    ),

  keys: (projectId: string) =>
    request<{ keys: ApiKey[] }>(`${base}/projects/${projectId}/keys`),

  createKey: (projectId: string, name: string, scopes: Scope[]) =>
    request<{ key: ApiKey; token: string; note: string }>(
      `${base}/projects/${projectId}/keys`,
      { method: "POST", body: { name, scopes } },
    ),

  revokeKey: (projectId: string, keyId: string) =>
    request<void>(`${base}/projects/${projectId}/keys/${keyId}`, {
      method: "DELETE",
    }),

  connections: (projectId: string) =>
    request<{ connections: Connection[] }>(
      `${base}/projects/${projectId}/connections`,
    ),

  createConnection: (
    projectId: string,
    name: string,
    url: string,
    mode: "browser" | "proxy",
  ) =>
    request<Connection>(`${base}/projects/${projectId}/connections`, {
      method: "POST",
      body: { name, url, mode },
    }),

  deleteConnection: (projectId: string, connectionId: string) =>
    request<void>(`${base}/projects/${projectId}/connections/${connectionId}`, {
      method: "DELETE",
    }),

  dashboards: (projectId: string) =>
    request<{ dashboards: Dashboard[] }>(
      `${base}/projects/${projectId}/dashboards`,
    ),

  createDashboard: (projectId: string, name: string, panels: PanelInput[]) =>
    request<Dashboard>(`${base}/projects/${projectId}/dashboards`, {
      method: "POST",
      body: { name, panels },
    }),

  saveDashboard: (
    projectId: string,
    dashboardId: string,
    name: string,
    panels: PanelInput[],
  ) =>
    request<Dashboard>(
      `${base}/projects/${projectId}/dashboards/${dashboardId}`,
      { method: "PUT", body: { name, panels } },
    ),

  deleteDashboard: (projectId: string, dashboardId: string) =>
    request<void>(`${base}/projects/${projectId}/dashboards/${dashboardId}`, {
      method: "DELETE",
    }),

  monitors: (projectId: string) =>
    request<{ monitors: Monitor[] }>(`${base}/projects/${projectId}/monitors`),

  createMonitor: (projectId: string, monitor: MonitorInput) =>
    request<Monitor>(`${base}/projects/${projectId}/monitors`, {
      method: "POST",
      body: monitor,
    }),

  updateMonitor: (
    projectId: string,
    monitorId: string,
    patch: Partial<
      Pick<Monitor, "enabled" | "threshold" | "comparison" | "severity">
    >,
  ) =>
    request<Monitor>(`${base}/projects/${projectId}/monitors/${monitorId}`, {
      method: "PATCH",
      body: patch,
    }),

  deleteMonitor: (projectId: string, monitorId: string) =>
    request<void>(`${base}/projects/${projectId}/monitors/${monitorId}`, {
      method: "DELETE",
    }),

  alerts: (projectId: string) =>
    request<{ alerts: AlertEvent[] }>(`${base}/projects/${projectId}/alerts`),

  examples: (projectId: string) =>
    request<{ examples: { title: string; query: string }[] }>(
      `${base}/projects/${projectId}/examples`,
    ),

  telemetry: (signal?: AbortSignal) =>
    request<Telemetry>(`${base}/telemetry`, { signal }),
};

export type PanelInput = {
  title: string;
  kind: Panel["kind"];
  bucket_id: string;
  query: string;
  unit: string;
  span: number;
};

export type MonitorInput = {
  name: string;
  bucket_id: string;
  query: string;
  comparison: Monitor["comparison"];
  threshold: number;
  severity: Monitor["severity"];
};

// ============================================================================
// Query macros
// ============================================================================

/** Bucket widths the server accepts, smallest first. */
export const INTERVALS = [
  "10s",
  "30s",
  "1m",
  "2m",
  "5m",
  "10m",
  "15m",
  "30m",
  "1h",
  "3h",
  "6h",
  "12h",
  "1d",
] as const;

function intervalNs(interval: string): number {
  const value = Number.parseInt(interval, 10) || 1;
  const unit = interval.slice(-1);
  const scale =
    unit === "s" ? 1e9 : unit === "m" ? 60e9 : unit === "h" ? 3600e9 : 86400e9;
  return value * scale;
}

/** Choose a bucket width that keeps a line chart readable for the range. */
export function autoInterval(spanNs: number): string {
  const target = Math.max(1, spanNs / 180);
  return INTERVALS.find((candidate) => intervalNs(candidate) >= target) ?? "1d";
}

/**
 * Expand `$timeFilter`, `$interval`, `$from` and `$to`. The hosted API does
 * this server-side; a self-hosted FluxDB knows nothing about the macros, so the
 * same substitution runs here for direct connections. Keeping one
 * implementation of the macro list on each side is deliberate: a saved panel
 * must render identically wherever its data lives.
 */
export function substituteMacros(
  query: string,
  fromNs: string,
  toNs: string,
  interval: string,
): string {
  return query
    .replaceAll("$timeFilter", `time >= ${fromNs} AND time <= ${toNs}`)
    .replaceAll("$interval", `'${interval}'`)
    .replaceAll("$from", fromNs)
    .replaceAll("$to", toNs);
}

// ============================================================================
// Data plane
// ============================================================================

export type ReadOptions = {
  measurement?: string;
  start?: string;
  end?: string;
  limit?: number;
  offset?: number;
};

export type QueryWindow = { from: string; to: string; interval?: string };

export type DeleteOptions = {
  measurement: string;
  tags?: Record<string, string>;
  start: string;
  end: string;
  exact?: boolean;
};

export interface DataClient {
  readonly target: DataTarget;
  points(options: ReadOptions, signal?: AbortSignal): Promise<PointsPage>;
  writePoints(points: Point[]): Promise<{ written: number }>;
  deletePoints(options: DeleteOptions): Promise<{ deleted: number }>;
  query(
    sql: string,
    window?: QueryWindow,
    signal?: AbortSignal,
  ): Promise<QueryResult>;
  schema(signal?: AbortSignal): Promise<Schema>;
  flush(): Promise<void>;
  compact(): Promise<void>;
  exportSnapshot(): Promise<unknown>;
}

function searchParams(options: Record<string, unknown>): string {
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(options)) {
    if (value !== undefined && value !== null && value !== "") {
      params.set(key, String(value));
    }
  }
  const encoded = params.toString();
  return encoded ? `?${encoded}` : "";
}

function cloudClient(
  target: Extract<DataTarget, { kind: "cloud" }>,
): DataClient {
  const root = `${base}/projects/${target.projectId}/buckets/${target.bucketId}`;
  return {
    target,
    points: (options, signal) =>
      request<PointsPage>(`${root}/points${searchParams(options)}`, { signal }),
    writePoints: (points) =>
      request<{ written: number }>(`${root}/points`, {
        method: "POST",
        body: { points },
      }),
    deletePoints: (options) =>
      request<{ deleted: number }>(`${root}/points`, {
        method: "DELETE",
        body: options,
      }),
    query: (sql, window, signal) =>
      request<QueryResult>(`${root}/query`, {
        method: "POST",
        body: { query: sql, ...window },
        signal,
      }),
    schema: (signal) => request<Schema>(`${root}/schema`, { signal }),
    flush: () => request<void>(`${root}/flush`, { method: "POST" }),
    compact: () => request<void>(`${root}/compact`, { method: "POST" }),
    exportSnapshot: () => request<unknown>(`${root}/export`),
  };
}

function directClient(
  target: Extract<DataTarget, { kind: "direct" }>,
): DataClient {
  const root = `/api/v1/databases/${encodeURIComponent(target.database)}`;
  const options = { absolute: target.baseUrl, token: target.token };
  return {
    target,
    points: (read, signal) =>
      request<PointsPage>(`${root}/points${searchParams(read)}`, {
        ...options,
        signal,
      }),
    writePoints: (points) =>
      request<{ written: number }>(`${root}/points`, {
        ...options,
        method: "POST",
        body: { points },
      }),
    deletePoints: (remove) =>
      request<{ deleted: number }>(`${root}/points`, {
        ...options,
        method: "DELETE",
        body: remove,
      }),
    query: async (sql, window, signal) => {
      // A self-hosted server does not implement the macros, so they are
      // expanded here and the resolved window is reported back the same way
      // the hosted API reports it.
      const now = String(Date.now() * 1_000_000);
      const from = window?.from ?? String(Date.now() * 1_000_000 - 6 * 3600e9);
      const to = window?.to ?? now;
      const interval =
        window?.interval ?? autoInterval(Number(to) - Number(from));
      const result = await request<QueryResult>(`${root}/query`, {
        ...options,
        method: "POST",
        body: { query: substituteMacros(sql, from, to, interval) },
        signal,
      });
      return { ...result, window: { from, to, interval } };
    },
    schema: (signal) =>
      request<Schema>(`${root}/schema`, { ...options, signal }),
    flush: () => request<void>(`${root}/flush`, { ...options, method: "POST" }),
    compact: () =>
      request<void>(`${root}/compact`, { ...options, method: "POST" }),
    exportSnapshot: () => request<unknown>(`${root}/export`, options),
  };
}

export function dataClient(target: DataTarget): DataClient {
  return target.kind === "cloud" ? cloudClient(target) : directClient(target);
}

// ============================================================================
// Self-hosted server discovery
// ============================================================================

export type DirectServerInfo = {
  version: string;
  databases: string[];
  authenticated: boolean;
};

/**
 * Probe a FluxDB server the visitor runs themselves. Reported failures
 * distinguish the three things that actually go wrong — unreachable, wrong
 * token, and reachable but not FluxDB — because "connection failed" on its own
 * is not actionable.
 */
export async function probeDirectServer(
  baseUrl: string,
  token: string,
): Promise<DirectServerInfo> {
  const trimmed = baseUrl.replace(/\/+$/, "");
  const health = await request<{ status: string; version: string }>(
    "/api/v1/health",
    { absolute: trimmed },
  );
  if (!health?.version) {
    throw new ApiError(
      0,
      "not_fluxdb",
      `${trimmed} answered, but it does not look like a FluxDB server.`,
    );
  }
  let databases: string[] = [];
  let authenticated = true;
  try {
    databases = await request<string[]>("/api/v1/databases", {
      absolute: trimmed,
      token,
    });
  } catch (error) {
    if (error instanceof ApiError && error.status === 401) {
      authenticated = false;
    } else {
      throw error;
    }
  }
  return { version: health.version, databases, authenticated };
}

/** Create a database on a self-hosted server. */
export function createDirectDatabase(
  baseUrl: string,
  token: string,
  name: string,
) {
  return request<void>(`/api/v1/databases/${encodeURIComponent(name)}`, {
    absolute: baseUrl.replace(/\/+$/, ""),
    token,
    method: "POST",
  });
}
