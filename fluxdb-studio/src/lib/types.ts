/** Shapes returned by the FluxDB control plane and data plane. */

export type Role = "viewer" | "member" | "admin" | "owner";
export type AccountKind = "standard" | "guest";
export type Scope = "read" | "write";
export type MonitorState = "ok" | "alerting" | "unknown";
export type Severity = "info" | "warning" | "critical";
export type Comparison = "above" | "below";
export type PanelKind = "line" | "area" | "bar" | "stat" | "table";
export type ConnectionMode = "browser" | "proxy";

export type Account = {
  id: string;
  email: string;
  name: string;
  avatar_url: string | null;
  kind: AccountKind;
  has_password: boolean;
  created_at: number;
  expires_at: number | null;
};

export type ProjectSummary = {
  id: string;
  name: string;
  slug: string;
  description: string;
  demo: boolean;
  created_at: number;
};

export type OrgSummary = {
  id: string;
  name: string;
  slug: string;
  plan: string;
  role: Role;
  expires_at: number | null;
  is_demo: boolean;
  projects: ProjectSummary[];
};

export type Session = {
  account: Account;
  organizations: OrgSummary[];
};

export type DeploymentConfig = {
  version: string;
  providers: { github: boolean };
  guest_enabled: boolean;
  demo_project_id: string;
  control_plane: string;
  limits: {
    projects_per_org: number;
    buckets_per_project: number;
    points_per_project: number;
    guest_lifetime_hours: number;
  };
};

/** Live figures published for the marketing page. */
export type PublicStats = {
  version: string;
  uptime_seconds: number;
  demo_points: number;
  demo_sstables: number;
  requests_recorded: number;
  query_p50_ms: number | null;
  query_p95_ms: number | null;
  query_p99_ms: number | null;
};

export type Bucket = {
  id: string;
  name: string;
  retention_seconds: number;
  created_at: number;
  points: number;
  /** Bytes in SSTable files. Excludes anything still in the memtable. */
  size_bytes: number;
  sstables: number;
  memtable_bytes: number;
};

export type ApiKey = {
  id: string;
  name: string;
  scopes: Scope[];
  created_at: number;
  last_used_at: number | null;
  revoked: boolean;
  masked_token: string;
};

export type Connection = {
  id: string;
  name: string;
  url: string;
  mode: ConnectionMode;
  created_at: number;
  last_checked_at: number | null;
  last_status: string | null;
};

export type Monitor = {
  id: string;
  name: string;
  bucket_id: string;
  query: string;
  comparison: Comparison;
  threshold: number;
  severity: Severity;
  enabled: boolean;
  state: MonitorState;
  last_value: number | null;
  last_checked_at: number | null;
  last_error: string | null;
  created_at: number;
};

export type AlertEvent = {
  id: string;
  monitor_id: string;
  monitor_name: string;
  state: MonitorState;
  severity: Severity;
  value: number | null;
  message: string;
  at: number;
};

export type Panel = {
  id: string;
  title: string;
  kind: PanelKind;
  bucket_id: string;
  query: string;
  unit: string;
  span: number;
};

export type Dashboard = {
  id: string;
  name: string;
  panels: Panel[];
  created_at: number;
  updated_at: number;
};

export type Project = {
  id: string;
  org_id: string;
  name: string;
  slug: string;
  description: string;
  demo: boolean;
  created_at: number;
  role: Role;
  writable: boolean;
  administrable: boolean;
};

export type ProjectDetail = {
  project: Project;
  org: { id: string; name: string; slug: string; plan: string };
  buckets: Bucket[];
  keys: ApiKey[];
  connections: Connection[];
  monitors: Monitor[];
  alerts: AlertEvent[];
  usage: {
    points: number;
    points_limit: number;
    buckets: number;
    buckets_limit: number;
    size_bytes: number;
  };
};

export type OrgDetail = {
  id: string;
  name: string;
  slug: string;
  plan: string;
  role: Role;
  created_at: number;
  expires_at: number | null;
  is_demo: boolean;
  members: number;
  usage: {
    projects: number;
    projects_limit: number;
    buckets: number;
    points: number;
    points_limit: number;
  };
  projects: Project[];
};

export type Member = {
  account_id: string;
  email: string;
  name: string;
  role: Role;
  created_at: number;
  is_you: boolean;
};

export type Invite = {
  id: string;
  email: string;
  role: Role;
  created_at: number;
};

export type AuditEntry = {
  id: string;
  at: number;
  actor: string;
  action: string;
  target: string;
  detail: string;
  project_id: string | null;
};

/** A field value as the JSON API represents it. Exact 64-bit integers arrive
 *  as `{ integer: "…" }` because a double cannot hold them. */
export type FieldValue = number | string | boolean | { integer: string };

export type Point = {
  measurement: string;
  /** Decimal nanoseconds. */
  timestamp: string;
  tags: Record<string, string>;
  fields: Record<string, FieldValue>;
};

export type PointsPage = {
  total: number;
  offset: number;
  limit: number;
  points: Point[];
};

export type Measurement = {
  points: number;
  fields: Record<string, string[]>;
  tags: Record<string, string[]>;
};

export type Schema = { measurements: Record<string, Measurement> };

export type QueryResult = {
  columns: string[];
  rows: (string | number | boolean | null)[][];
  execution_time_ms: number;
  window?: { from: string; to: string; interval: string };
};

export type TelemetrySample = {
  time: number;
  duration_ms: number;
  status: number;
  operation: string;
  database: string | null;
};

export type Telemetry = {
  uptime_seconds: number;
  samples: TelemetrySample[];
  capacity: number;
  authentication_enabled: boolean;
};

/** Where a data request should go: a cloud bucket, or a FluxDB server the user
 *  runs themselves and the browser talks to directly. */
export type DataTarget =
  | { kind: "cloud"; projectId: string; bucketId: string; label: string }
  | {
      kind: "direct";
      baseUrl: string;
      token: string;
      database: string;
      label: string;
    };
