import Documentation from "./Documentation";
import Assistant from "./Assistant";
import { useCallback, useEffect, useRef, useState } from "react";
import {
  Activity,
  ArrowDownToLine,
  ArrowUpRight,
  Braces,
  Check,
  ChevronRight,
  HelpCircle,
  Code2,
  Database,
  HardDrive,
  Layers,
  LayoutDashboard,
  Play,
  Plus,
  RefreshCw,
  Server,
  Settings2,
  ShieldCheck,
  Terminal,
  Trash2,
  Upload,
  X,
  Zap,
  Sparkles,
} from "lucide-react";
import ReactECharts from "echarts-for-react/lib/core";
import * as echarts from "echarts/core";
import { LineChart, BarChart } from "echarts/charts";
import {
  GridComponent,
  TooltipComponent,
  LegendComponent,
} from "echarts/components";
import { CanvasRenderer } from "echarts/renderers";
echarts.use([
  LineChart,
  BarChart,
  GridComponent,
  TooltipComponent,
  LegendComponent,
  CanvasRenderer,
]);

type Profile = { name: string; url: string };
type Sample = {
  time: number;
  duration_ms: number;
  status: number;
  operation: string;
  database: string | null;
};
type Telemetry = {
  uptime_seconds: number;
  samples: Sample[];
  authentication_enabled: boolean;
};
type Db = {
  name: string;
  memtable_size: number;
  sstables: number;
  total_entries: number;
  retention_seconds: number;
  total_size_bytes: number;
};
type Stats = {
  database_count: number;
  total_entries: number;
  total_size_bytes: number;
  databases: Db[];
};
type Point = {
  measurement: string;
  timestamp: string;
  tags: Record<string, string>;
  fields: Record<string, unknown>;
};
type Schema = Record<
  string,
  {
    points: number;
    fields: Record<string, string[]>;
    tags: Record<string, string[]>;
  }
>;
type Result = {
  columns: string[];
  rows: unknown[][];
  execution_time_ms: number;
};
const tabs = [
  "Overview",
  "Data explorer",
  "Query workspace",
  "Write data",
  "API & connection",
  "Settings",
  "Developer resources",
] as const;
type Tab = (typeof tabs)[number];
const icons = [
  LayoutDashboard,
  Database,
  Terminal,
  Upload,
  Code2,
  Settings2,
  HelpCircle,
];
const format = new Intl.NumberFormat("en", { maximumFractionDigits: 1 });
function bytes(n: number) {
  return n > 1048576
    ? `${format.format(n / 1048576)} MB`
    : n > 1024
      ? `${format.format(n / 1024)} KB`
      : `${n} B`;
}
function display(v: unknown): string {
  return v == null
    ? "—"
    : typeof v === "object"
      ? JSON.stringify(v)
      : String(v);
}
function loadProfiles(): Profile[] {
  try {
    const p = JSON.parse(localStorage.getItem("fluxdb.profiles") || "[]");
    return Array.isArray(p) &&
      p.every((v) => typeof v.name === "string" && typeof v.url === "string")
      ? p
      : [];
  } catch {
    return [];
  }
}
const example = JSON.stringify(
  {
    points: [
      {
        measurement: "cpu",
        tags: { host: "api-01", region: "us-east-1" },
        timestamp: "1789142400000000000",
        fields: { usage: 42.8, healthy: true, requests: { integer: "1200" } },
      },
    ],
  },
  null,
  2,
);

export default function Console() {
  const [assistantOpen, setAssistantOpen] = useState(false);
  const [tab, setTab] = useState<Tab>("Overview");
  const [profiles, setProfiles] = useState<Profile[]>(loadProfiles);
  const [connection, setConnection] = useState<Profile>({
    name: "Local server",
    url: "",
  });
  const [token, setToken] = useState("");
  const [dialog, setDialog] = useState<"connect" | "create" | "delete" | null>(
    null,
  );
  const [draftName, setDraftName] = useState("Local server");
  const [draftUrl, setDraftUrl] = useState("http://127.0.0.1:8086");
  const [draftToken, setDraftToken] = useState("");
  const [newDb, setNewDb] = useState("");
  const [confirm, setConfirm] = useState("");
  const [stats, setStats] = useState<Stats | null>(null);
  const [telemetry, setTelemetry] = useState<Telemetry | null>(null);
  const [online, setOnline] = useState(false);
  const [db, setDb] = useState("");
  const [schema, setSchema] = useState<Schema>({});
  const [measurement, setMeasurement] = useState("");
  const [points, setPoints] = useState<Point[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(0);
  const [query, setQuery] = useState(
    "SELECT * FROM cpu ORDER BY time DESC LIMIT 100",
  );
  const [result, setResult] = useState<Result | null>(null);
  const [history, setHistory] = useState<string[]>([]);
  const [payload, setPayload] = useState(example);
  const [busy, setBusy] = useState(false);
  const [notice, setNotice] = useState<{ text: string; error: boolean } | null>(
    null,
  );
  const [range, setRange] = useState(15);
  const [updated, setUpdated] = useState<Date | null>(null);
  const [deletePoint, setDeletePoint] = useState<Point | null>(null);
  const [revision, setRevision] = useState(0);
  const [retentionDays, setRetentionDays] = useState("0");
  const [retentionConfirmed, setRetentionConfirmed] = useState(false);
  const [plotField, setPlotField] = useState("");
  const generation = useRef(0);
  const modalRef = useRef<HTMLDivElement>(null);
  useEffect(() => {
    window.scrollTo({ top: 0, behavior: "instant" });
  }, [tab]);
  useEffect(() => {
    if (!dialog && !deletePoint) return;
    setNotice(null);
    const previous = document.activeElement as HTMLElement | null;
    const panel = modalRef.current;
    const nodes = Array.from(
      document.querySelectorAll<HTMLElement>(
        ".sidebar,.main-shell,.assistant-panel",
      ),
    );
    nodes.forEach((n) => {
      n.inert = true;
    });
    const focusTarget =
      panel?.querySelector<HTMLElement>("input") ||
      panel?.querySelector<HTMLElement>("button");
    focusTarget?.focus();
    const trap = (event: KeyboardEvent) => {
      if (event.key !== "Tab" || !panel) return;
      const items = Array.from(
        panel.querySelectorAll<HTMLElement>(
          "button:not(:disabled),input:not(:disabled),select:not(:disabled),textarea:not(:disabled),a[href]",
        ),
      );
      if (!items.length) return;
      const first = items[0],
        last = items[items.length - 1];
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first.focus();
      }
    };
    document.addEventListener("keydown", trap);
    return () => {
      nodes.forEach((n) => {
        n.inert = false;
      });
      document.removeEventListener("keydown", trap);
      previous?.focus();
    };
  }, [dialog, deletePoint]);
  const notify = (text: string, error = false) => setNotice({ text, error });
  const request = useCallback(
    async <T,>(
      path: string,
      options: RequestInit = {},
      profile = connection,
      secret = token,
    ): Promise<T> => {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), 30000);
      try {
        const response = await fetch(
          `${profile.url.replace(/\/$/, "")}${path}`,
          {
            ...options,
            signal: controller.signal,
            headers: {
              "Content-Type": "application/json",
              ...(secret ? { Authorization: `Bearer ${secret}` } : {}),
              ...options.headers,
            },
          },
        );
        const text = await response.text();
        let data;
        try {
          data = text ? JSON.parse(text) : null;
        } catch {
          throw new Error("Unexpected server response. Check the server URL.");
        }
        if (
          !response.ok &&
          response.status === 404 &&
          path.startsWith("/api/v1/assistant/")
        )
          throw new Error(
            "Assistant endpoint unavailable. Restart FluxDB with the updated server, then reopen Ask AI.",
          );
        if (!response.ok)
          throw new Error(data?.error || `Request failed (${response.status})`);
        return data as T;
      } catch (error) {
        if (error instanceof DOMException && error.name === "AbortError")
          throw new Error(
            "Request timed out after 30 seconds. Check the server and verify the operation before retrying.",
          );
        if (error instanceof TypeError)
          throw new Error(
            "Cannot reach this server. Check its URL, network connection, and allowed browser origins.",
          );
        throw error;
      } finally {
        clearTimeout(timer);
      }
    },
    [connection, token],
  );
  const refresh = useCallback(async () => {
    const current = generation.current;
    try {
      const [s, t] = await Promise.all([
        request<Stats>("/api/v1/stats"),
        request<Telemetry>("/api/v1/telemetry"),
      ]);
      if (current !== generation.current) return;
      s.databases.sort((a, b) => a.name.localeCompare(b.name));
      setStats(s);
      setTelemetry(t);
      setOnline(true);
      setUpdated(new Date());
      setRevision((r) => r + 1);
      setDb((old) =>
        s.databases.some((d) => d.name === old)
          ? old
          : s.databases[0]?.name || "",
      );
    } catch {
      if (current === generation.current) setOnline(false);
    }
  }, [request]);
  useEffect(() => {
    generation.current++;
    void refresh();
    const timer = setInterval(() => void refresh(), 5000);
    return () => {
      generation.current++;
      clearInterval(timer);
    };
  }, [refresh]);
  useEffect(() => {
    let active = true;
    if (!db || !online) {
      setSchema({});
      setPoints([]);
      setTotal(0);
    }
    if (db && online)
      void Promise.all([
        request<{ measurements: Schema }>(
          `/api/v1/databases/${encodeURIComponent(db)}/schema`,
        ),
        request<{ points: Point[]; total: number }>(
          `/api/v1/databases/${encodeURIComponent(db)}/points?limit=50&offset=${page * 50}${measurement ? `&measurement=${encodeURIComponent(measurement)}` : ""}`,
        ),
      ])
        .then(([s, p]) => {
          if (active) {
            setSchema(s.measurements);
            setPoints(p.points);
            setTotal(p.total);
          }
        })
        .catch((e) => {
          if (active) notify(e.message, true);
        });
    return () => {
      active = false;
    };
  }, [db, online, measurement, page, request, stats?.total_entries, revision]);
  useEffect(() => {
    if (!dialog && !deletePoint) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape" && !busy) {
        setDialog(null);
        setDeletePoint(null);
      }
    };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [dialog, deletePoint, busy]);
  const act = async (fn: () => Promise<void>) => {
    setBusy(true);
    setNotice(null);
    try {
      await fn();
    } catch (e) {
      notify(e instanceof Error ? e.message : "Operation failed", true);
    } finally {
      setBusy(false);
    }
  };
  const changed = async () => {
    setRevision((r) => r + 1);
    await refresh();
  };
  const selectDb = (name: string) => {
    setNotice(null);
    setPoints([]);
    setSchema({});
    setTotal(0);
    setDb(name);
    setMeasurement("");
    setPage(0);
    setResult(null);
  };
  const connect = () =>
    act(async () => {
      const url = new URL(draftUrl);
      if (
        !["http:", "https:"].includes(url.protocol) ||
        url.username ||
        url.password ||
        url.search ||
        url.hash
      )
        throw new Error(
          "Enter an HTTP or HTTPS URL without credentials or query parameters.",
        );
      const profile = {
        name: draftName.trim() || url.host,
        url: draftUrl.replace(/\/$/, ""),
      };
      await request("/api/v1/stats", {}, profile, draftToken);
      generation.current++;
      setStats(null);
      setTelemetry(null);
      selectDb("");
      setHistory([]);
      setConnection(profile);
      setToken(draftToken);
      const next = [...profiles.filter((p) => p.url !== profile.url), profile];
      setProfiles(next);
      localStorage.setItem("fluxdb.profiles", JSON.stringify(next));
      setDialog(null);
      notify(`Connected to ${profile.name}`);
    });
  const selected = stats?.databases.find((d) => d.name === db);
  useEffect(() => {
    setRetentionDays(String((selected?.retention_seconds || 0) / 86400));
    setRetentionConfirmed(false);
  }, [db, selected?.retention_seconds]);
  const samples = (telemetry?.samples || []).filter(
    (s) => s.time >= Date.now() - range * 60000 && (!db || s.database === db),
  );
  const durations = samples.map((s) => s.duration_ms).sort((a, b) => a - b);
  const p95 = durations.length
    ? durations[Math.ceil(durations.length * 0.95) - 1]
    : null;
  const failures = samples.filter((s) => s.status >= 400).length;
  const download = (content: string, name: string) => {
    const url = URL.createObjectURL(
      new Blob([content], { type: "application/json" }),
    );
    const a = document.createElement("a");
    a.href = url;
    a.download = name;
    a.click();
    URL.revokeObjectURL(url);
  };
  const chart = (kind: "latency" | "requests") => {
    const now = Date.now();
    const step = (range * 60000) / 30;
    const bins = Array.from({ length: 30 }, (_, i) => {
      const start = now - (30 - i) * step;
      const bin = samples.filter(
        (s) => s.time >= start && s.time < start + step,
      );
      return [
        start,
        kind === "requests"
          ? bin.length
          : bin.length
            ? Math.max(...bin.map((s) => s.duration_ms))
            : null,
      ];
    });
    return {
      animation: false,
      grid: { left: 46, right: 20, top: 20, bottom: 34 },
      tooltip: { trigger: "axis" },
      xAxis: {
        type: "time",
        axisLine: { lineStyle: { color: "#dce2ed" } },
        axisLabel: { color: "#7c879e", fontSize: 12 },
        splitLine: { show: false },
      },
      yAxis: {
        type: "value",
        min: 0,
        axisLabel: { color: "#7c879e" },
        splitLine: { lineStyle: { color: "#edf1f7", type: "dashed" } },
      },
      series: [
        {
          data: bins,
          type: kind === "requests" ? "bar" : "line",
          connectNulls: false,
          showSymbol: true,
          symbolSize: 5,
          barMaxWidth: 12,
          itemStyle: {
            color: kind === "latency" ? "#285bea" : "#13a98b",
            borderRadius: [3, 3, 0, 0],
          },
          lineStyle: { width: 2 },
          areaStyle:
            kind === "latency"
              ? { color: "#285bea", opacity: 0.07 }
              : undefined,
        },
      ],
    };
  };
  const dbPath = `/api/v1/databases/${encodeURIComponent(db)}`;
  const endpoint = `${connection.url || window.location.origin}/api/v1/databases/${encodeURIComponent(db || "observability")}`;
  return (
    <div className={`app-shell ${assistantOpen ? "assistant-open" : ""}`}>
      <aside className="sidebar">
        <a
          className="brand"
          href="#"
          onClick={(e) => {
            e.preventDefault();
            setTab("Overview");
          }}
        >
          <span className="brand-mark">
            <Zap size={23} fill="currentColor" />
          </span>
          <b>
            flux<span>db</span>
          </b>
          <small>STUDIO</small>
        </a>
        <button
          disabled={busy}
          className="server-switch"
          onClick={() => {
            setDraftToken("");
            setDialog("connect");
          }}
        >
          <span className="server-symbol">
            <Server size={19} />
          </span>
          <span>
            <strong>{connection.name}</strong>
            <small>{online ? "Connected server" : "Connection required"}</small>
          </span>
          <ChevronRight size={15} />
        </button>
        <div className="nav-label">WORKSPACE</div>
        <nav>
          {tabs
            .filter((t) => t !== "Developer resources")
            .map((name, i) => {
              const Icon = icons[i];
              return (
                <button
                  key={name}
                  className={tab === name ? "nav-item active" : "nav-item"}
                  onClick={() => setTab(name)}
                >
                  <Icon size={18} />
                  {name}
                  {name === "Data explorer" && (
                    <span className="nav-count">
                      {stats?.database_count ?? 0}
                    </span>
                  )}
                </button>
              );
            })}
          <button
            className={`nav-item mobile-docs ${tab === "Developer resources" ? "active" : ""}`}
            onClick={() => setTab("Developer resources")}
          >
            <HelpCircle size={18} />
            Developer resources
          </button>
        </nav>
        <div className="nav-label database-label">
          DATABASES{" "}
          <button
            aria-label="Create database"
            onClick={() => setDialog("create")}
            disabled={!online}
          >
            <Plus size={15} />
          </button>
        </div>
        <div className="db-nav">
          {stats?.databases.map((d) => (
            <button
              disabled={busy}
              className={db === d.name ? "selected-db" : ""}
              key={d.name}
              onClick={() => selectDb(d.name)}
            >
              <Database size={15} />
              <span>{d.name}</span>
            </button>
          ))}
          {!stats?.databases.length && <p>No databases yet</p>}
        </div>
        <div className="sidebar-bottom">
          <div>
            <ShieldCheck size={18} />
            <span>
              Single-node instance
              <small>
                {telemetry?.authentication_enabled
                  ? "Token authentication enabled"
                  : "Local development access"}
              </small>
            </span>
          </div>
          <button
            className={
              tab === "Developer resources" ? "nav-item active" : "nav-item"
            }
            onClick={() => setTab("Developer resources")}
          >
            <HelpCircle size={18} />
            Developer resources
            <ArrowUpRight size={14} />
          </button>
          <div className="user">
            <span>FL</span>
            <div>
              FluxDB workspace<small>Server administrator</small>
            </div>
          </div>
        </div>
      </aside>
      <div className="main-shell">
        <header className="topbar">
          <div className="breadcrumb">
            Workspace <ChevronRight size={14} />
            <span>{connection.name}</span>
            <ChevronRight size={14} />
            <strong>{tab}</strong>
          </div>
          <div className="top-actions">
            <button
              className="assistant-trigger"
              aria-expanded={assistantOpen}
              onClick={() => setAssistantOpen(!assistantOpen)}
            >
              <Sparkles size={16} />
              <span>Ask AI</span>
            </button>
            <span className={`status ${online ? "healthy" : "offline"}`}>
              <i />
              {online ? "Server online" : "Offline"}
            </span>
            <button
              className="icon-button"
              aria-label="Refresh server"
              onClick={() => void changed()}
            >
              <RefreshCw size={17} />
            </button>
            <span className="avatar">FL</span>
          </div>
        </header>
        <main>
          {import.meta.env.VITE_RENDER_DEMO === "true" && (
            <div className="render-demo-notice" role="note">
              <strong>Temporary demo</strong>
              <span>Sample data is restored after a reset. Public access supports browsing and SQL; changes require an administrator token. AI uses your own session key. The free host may sleep when idle.</span>
            </div>
          )}
          <div className="page-heading">
            <div>
              <div className="eyebrow">
                {db ? `DATABASE / ${db}` : "YOUR TIME-SERIES WORKSPACE"}
              </div>
              <h1>{tab === "Overview" ? "Database overview" : tab}</h1>
              <p>
                {
                  {
                    Overview:
                      "A live view of your data, performance, and database activity.",
                    "Data explorer":
                      "Browse measurements, inspect points, and explore your schema.",
                    "Query workspace":
                      "Turn time-series data into answers with SQL.",
                    "Write data":
                      "Ingest typed points or update existing series.",
                    "API & connection":
                      "Connect your applications directly to FluxDB.",
                    Settings: "Manage storage and database lifecycle.",
                    "Developer resources":
                      "Guides, API reference, and working examples for building with FluxDB.",
                  }[tab]
                }
              </p>
            </div>
            <div className="heading-actions">
              <button disabled={busy} onClick={() => setDialog("connect")}>
                <Server size={16} />
                Connect server
              </button>
              <button
                className="primary"
                disabled={!online}
                onClick={() => setTab("Write data")}
              >
                <Plus size={17} />
                Write data
              </button>
            </div>
          </div>
          {notice && (
            <div
              role="alert"
              className={`notice ${notice.error ? "error" : ""}`}
            >
              {notice.error ? <HelpCircle size={18} /> : <Check size={18} />}
              <span>{notice.text}</span>
              <button
                aria-label="Dismiss notification"
                onClick={() => setNotice(null)}
              >
                <X size={16} />
              </button>
            </div>
          )}
          {!online && (
            <div className="connection-banner">
              <Server size={23} />
              <div>
                <strong>Connect to your FluxDB server</strong>
                <p>
                  Start the Rust server, then connect with its URL and access
                  token. Live data will appear here.
                </p>
              </div>
              <button disabled={busy} onClick={() => setDialog("connect")}>
                Set up connection <ArrowUpRight size={15} />
              </button>
            </div>
          )}
          <div className="scope-bar">
            <div>
              <span className="scope-label">Database</span>
              <select
                disabled={busy}
                aria-label="Active database"
                value={db}
                onChange={(e) => selectDb(e.target.value)}
              >
                <option value="" disabled>
                  Select a database
                </option>
                {stats?.databases.map((d) => (
                  <option key={d.name}>{d.name}</option>
                ))}
              </select>
              <button
                className="text-button"
                disabled={!online}
                onClick={() => setDialog("create")}
              >
                <Plus size={15} />
                Create database
              </button>
            </div>
            <div>
              <span className="live-label">
                {online ? "Auto-refresh · 5s" : "Awaiting connection"}
              </span>
              {tab === "Overview" && (
                <select
                  aria-label="Monitoring time range"
                  value={range}
                  onChange={(e) => setRange(Number(e.target.value))}
                >
                  <option value={5}>Last 5 minutes</option>
                  <option value={15}>Last 15 minutes</option>
                  <option value={60}>Last hour</option>
                </select>
              )}
            </div>
          </div>
          {tab === "Developer resources" && (
            <Documentation baseUrl={connection.url} database={db} />
          )}
          {tab === "Overview" && (
            <>
              <div className="stat-grid">
                {[
                  {
                    label: "Stored points",
                    value: selected
                      ? format.format(selected.total_entries)
                      : "—",
                    icon: Database,
                    detail: db || "Select a database",
                  },
                  {
                    label: "P95 request latency",
                    value: p95 === null ? "—" : `${p95.toFixed(2)} ms`,
                    icon: Activity,
                    detail: `${samples.length} requests in selected window`,
                  },
                  {
                    label: "Active memtable",
                    value: selected ? bytes(selected.memtable_size) : "—",
                    icon: HardDrive,
                    detail: `${selected?.sstables ?? 0} persisted SSTables`,
                  },
                  {
                    label: "Request errors",
                    value: samples.length
                      ? `${((failures / samples.length) * 100).toFixed(1)}%`
                      : "—",
                    icon: ShieldCheck,
                    detail: `${failures} failed requests in window`,
                  },
                ].map((card) => (
                  <section className="stat-card" key={card.label}>
                    <div>
                      <span>{card.label}</span>
                      <card.icon size={18} />
                    </div>
                    <strong>{card.value}</strong>
                    <p>{card.detail}</p>
                  </section>
                ))}
              </div>
              <div className="chart-grid">
                <section className="panel">
                  <div className="panel-heading">
                    <div>
                      <h2>Request latency</h2>
                      <p>Peak duration per interval · milliseconds</p>
                    </div>
                    <span className="legend blue">Max latency</span>
                  </div>
                  <ReactECharts
                    echarts={echarts}
                    notMerge={true}
                    option={chart("latency")}
                    style={{ height: 235 }}
                  />
                  {!samples.length && (
                    <p className="chart-note">
                      Run a query or write data to collect request latency.
                    </p>
                  )}
                </section>
                <section className="panel">
                  <div className="panel-heading">
                    <div>
                      <h2>Database activity</h2>
                      <p>Requests per interval · selected database</p>
                    </div>
                    <span className="legend green">Requests</span>
                  </div>
                  <ReactECharts
                    echarts={echarts}
                    notMerge={true}
                    option={chart("requests")}
                    style={{ height: 235 }}
                  />
                  {!samples.length && (
                    <p className="chart-note">
                      No database requests recorded in this window.
                    </p>
                  )}
                </section>
              </div>
              <section className="panel">
                <div className="panel-heading">
                  <div>
                    <h2>
                      Measurements{" "}
                      <span className="badge">
                        {Object.keys(schema).length}
                      </span>
                    </h2>
                    <p>Data stored in {db || "your selected database"}</p>
                  </div>
                  <button
                    className="text-button"
                    onClick={() => setTab("Data explorer")}
                  >
                    Explore data
                    <ArrowUpRight size={15} />
                  </button>
                </div>
                <div className="table-wrap">
                  <table>
                    <thead>
                      <tr>
                        <th>Measurement</th>
                        <th>Points</th>
                        <th>Fields</th>
                        <th>Tag keys</th>
                        <th />
                      </tr>
                    </thead>
                    <tbody>
                      {Object.entries(schema).map(([name, s]) => (
                        <tr key={name}>
                          <td>
                            <span className="table-name">
                              <Layers size={16} />
                              {name}
                            </span>
                          </td>
                          <td>{format.format(s.points)}</td>
                          <td>{Object.keys(s.fields).join(", ")}</td>
                          <td>{Object.keys(s.tags).join(", ") || "—"}</td>
                          <td>
                            <button
                              className="text-button"
                              onClick={() => {
                                setMeasurement(name);
                                setPage(0);
                                setTab("Data explorer");
                              }}
                            >
                              Browse
                              <ChevronRight size={14} />
                            </button>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
                {!Object.keys(schema).length && (
                  <Empty
                    title="Your next data point starts here"
                    text="Create a database and write your first measurement to explore it here."
                    action={() => setTab("Write data")}
                    label="Write your first points"
                  />
                )}
              </section>
              <div className="bottom-grid">
                <section className="panel">
                  <div className="panel-heading">
                    <div>
                      <h2>Recent requests</h2>
                      <p>
                        Measured on the server · latest 2,000 requests retained
                        in memory
                      </p>
                    </div>
                    <Activity size={18} />
                  </div>
                  <div className="request-list">
                    {samples
                      .slice(-5)
                      .reverse()
                      .map((s, i) => (
                        <div key={`${s.time}-${i}`}>
                          <span
                            className={`http-status ${s.status >= 400 ? "failed" : ""}`}
                          >
                            {s.status}
                          </span>
                          <code>
                            {s.operation.replace("/api/v1/databases/", "")}
                          </code>
                          <span>{s.duration_ms.toFixed(2)} ms</span>
                          <small>{new Date(s.time).toLocaleTimeString()}</small>
                        </div>
                      ))}
                    {!samples.length && (
                      <p className="muted">
                        Request history appears as you use this database.
                      </p>
                    )}
                  </div>
                </section>
                <section className="quickstart">
                  <Braces size={25} />
                  <h2>From application to insight.</h2>
                  <p>
                    Write points with a simple HTTP request. Query them from any
                    language.
                  </p>
                  <button onClick={() => setTab("API & connection")}>
                    Explore the API
                    <ArrowUpRight size={16} />
                  </button>
                  <code>POST /api/v1/databases/:name/points</code>
                </section>
              </div>
            </>
          )}
          {tab === "Data explorer" && (
            <>
              <section className="panel">
                <div className="panel-heading">
                  <div>
                    <h2>Measurement values</h2>
                    <p>
                      Current page · up to 50 newest points · grouped by series
                    </p>
                  </div>
                  <select
                    aria-label="Chart field"
                    value={plotField}
                    onChange={(e) => setPlotField(e.target.value)}
                  >
                    <option value="">Choose a numeric field</option>
                    {Array.from(
                      new Set(
                        points.flatMap((p) =>
                          Object.entries(p.fields)
                            .filter(([, v]) => typeof v === "number")
                            .map(([k]) => k),
                        ),
                      ),
                    ).map((k) => (
                      <option key={k}>{k}</option>
                    ))}
                  </select>
                </div>
                {plotField ? (
                  <ReactECharts
                    echarts={echarts}
                    notMerge={true}
                    style={{ height: 260 }}
                    option={{
                      animation: false,
                      tooltip: { trigger: "axis" },
                      legend: { type: "scroll", bottom: 0 },
                      grid: { left: 55, right: 25, top: 15, bottom: 60 },
                      xAxis: { type: "time" },
                      yAxis: { type: "value", scale: true },
                      series: Array.from(
                        new Set(
                          points.map(
                            (p) => p.measurement + JSON.stringify(p.tags),
                          ),
                        ),
                      ).map((key) => ({
                        name: key,
                        type: "line",
                        showSymbol: true,
                        connectNulls: false,
                        data: points
                          .filter(
                            (p) =>
                              p.measurement + JSON.stringify(p.tags) === key &&
                              typeof p.fields[plotField] === "number",
                          )
                          .map((p) => [
                            Number(BigInt(p.timestamp) / 1000000n),
                            p.fields[plotField],
                          ])
                          .sort((a, b) => Number(a[0]) - Number(b[0])),
                      })),
                    }}
                  />
                ) : (
                  <Empty
                    title="Explore your time series"
                    text="Choose a numeric field to chart stored values. Table timestamps retain full nanosecond precision."
                  />
                )}
              </section>
              <section className="panel">
                <div className="panel-heading">
                  <div>
                    <h2>
                      Stored points <span className="badge">{total}</span>
                    </h2>
                    <p>Newest first · timestamps in nanoseconds</p>
                  </div>
                  <div className="inline-controls">
                    <select
                      aria-label="Measurement filter"
                      value={measurement}
                      onChange={(e) => {
                        setMeasurement(e.target.value);
                        setPage(0);
                      }}
                    >
                      <option value="">All measurements</option>
                      {Object.keys(schema).map((m) => (
                        <option key={m}>{m}</option>
                      ))}
                    </select>
                    <button
                      disabled={!points.length}
                      onClick={() =>
                        download(
                          JSON.stringify({ points }, null, 2),
                          `${db}-page-${page + 1}.json`,
                        )
                      }
                    >
                      <ArrowDownToLine size={16} />
                      Export page
                    </button>
                  </div>
                </div>
                <div className="table-wrap">
                  <table>
                    <thead>
                      <tr>
                        <th>Timestamp (ns)</th>
                        <th>Measurement</th>
                        <th>Tags</th>
                        <th>Fields</th>
                        <th>Actions</th>
                      </tr>
                    </thead>
                    <tbody>
                      {points.map((p) => (
                        <tr
                          key={`${p.timestamp}-${p.measurement}-${JSON.stringify(p.tags)}`}
                        >
                          <td>
                            <code>{p.timestamp}</code>
                          </td>
                          <td>{p.measurement}</td>
                          <td>
                            {Object.entries(p.tags).map(([k, v]) => (
                              <span className="tag" key={k}>
                                {k}={v}
                              </span>
                            ))}
                          </td>
                          <td>
                            <code className="field-values">
                              {JSON.stringify(p.fields)}
                            </code>
                          </td>
                          <td>
                            <div className="inline-controls">
                              <button
                                className="text-button"
                                onClick={() => {
                                  setPayload(
                                    JSON.stringify({ points: [p] }, null, 2),
                                  );
                                  setTab("Write data");
                                }}
                              >
                                Edit
                              </button>
                              <button
                                className="icon-button danger"
                                aria-label={`Delete ${p.measurement} at ${p.timestamp}`}
                                onClick={() => setDeletePoint(p)}
                              >
                                <Trash2 size={15} />
                              </button>
                            </div>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
                {!points.length && (
                  <Empty
                    title="No points found"
                    text="Write points or choose a different measurement."
                  />
                )}
                <div className="pagination">
                  <span>
                    {total
                      ? `${page * 50 + 1}–${Math.min((page + 1) * 50, total)} of ${total}`
                      : "0 points"}
                  </span>
                  <button
                    disabled={page === 0}
                    onClick={() => setPage((p) => p - 1)}
                  >
                    Previous
                  </button>
                  <button
                    disabled={(page + 1) * 50 >= total}
                    onClick={() => setPage((p) => p + 1)}
                  >
                    Next
                  </button>
                </div>
              </section>
            </>
          )}
          {tab === "Query workspace" && (
            <>
              <section className="panel">
                <div className="panel-heading">
                  <div>
                    <h2>
                      <Terminal size={18} />
                      SQL editor
                    </h2>
                    <p>
                      SELECT, filters, aggregations, GROUP BY time, LIMIT and
                      OFFSET
                    </p>
                  </div>
                  <button
                    className="primary"
                    disabled={busy || !db || !online}
                    onClick={() =>
                      void act(async () => {
                        setResult(null);
                        const r = await request<Result>(`${dbPath}/query`, {
                          method: "POST",
                          body: JSON.stringify({ query }),
                        });
                        setResult(r);
                        setHistory((h) =>
                          [query, ...h.filter((q) => q !== query)].slice(0, 12),
                        );
                        await refresh();
                      })
                    }
                  >
                    <Play size={15} />
                    {busy ? "Running…" : "Run query"}
                  </button>
                </div>
                <textarea
                  className="code-editor"
                  aria-label="SQL query"
                  spellCheck={false}
                  value={query}
                  onChange={(e) => setQuery(e.target.value)}
                />
                <div className="editor-footer">
                  <span>SQL · {db || "No database selected"}</span>
                  <select
                    aria-label="Query examples"
                    defaultValue=""
                    onChange={(e) => {
                      if (e.target.value) setQuery(e.target.value);
                    }}
                  >
                    <option value="">Choose an example</option>
                    <option
                      value={`SELECT * FROM ${measurement || "cpu"} ORDER BY time DESC LIMIT 100`}
                    >
                      Latest points
                    </option>
                    <option
                      value={`SELECT COUNT(*) FROM ${measurement || "cpu"}`}
                    >
                      Count all points
                    </option>
                    <option
                      value={`SELECT MEAN(usage) FROM ${measurement || "cpu"} GROUP BY time('1m')`}
                    >
                      One-minute averages
                    </option>
                  </select>
                </div>
              </section>
              <section className="panel result-panel">
                <div className="panel-heading">
                  <div>
                    <h2>Query results</h2>
                    <p>
                      {result
                        ? `${result.rows.length} rows · ${result.execution_time_ms.toFixed(2)} ms execution`
                        : "Run a query to see its result"}
                    </p>
                  </div>
                  <button
                    disabled={!result}
                    onClick={() =>
                      result &&
                      download(
                        JSON.stringify(result, null, 2),
                        `${db}-query.json`,
                      )
                    }
                  >
                    <ArrowDownToLine size={16} />
                    Export
                  </button>
                </div>
                {result ? (
                  <div className="table-wrap">
                    <table>
                      <thead>
                        <tr>
                          {result.columns.map((c, i) => (
                            <th key={i}>{c}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {result.rows.map((r, i) => (
                          <tr key={i}>
                            {r.map((v, j) => (
                              <td key={j}>
                                <code>{display(v)}</code>
                              </td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                    {result.rows.length === 0 && (
                      <Empty
                        title="Query completed · no matching rows"
                        text="Try a different filter or write data to this measurement."
                      />
                    )}
                  </div>
                ) : (
                  <Empty
                    title="Ready when you are"
                    text="Choose a database, write a SELECT statement, and run your query."
                  />
                )}
              </section>
              {history.length > 0 && (
                <section className="panel">
                  <div className="panel-heading">
                    <h2>Session query history</h2>
                  </div>
                  {history.map((q) => (
                    <button
                      className="history-item"
                      key={q}
                      onClick={() => setQuery(q)}
                    >
                      <code>{q}</code>
                      <ChevronRight size={15} />
                    </button>
                  ))}
                </section>
              )}
            </>
          )}
          {tab === "Write data" && (
            <div className="write-grid">
              <section className="panel">
                <div className="panel-heading">
                  <div>
                    <h2>Write points</h2>
                    <p>JSON batch · up to 10,000 points per request</p>
                  </div>
                  <label className="file-button">
                    <Upload size={16} />
                    Import JSON
                    <input
                      type="file"
                      accept=".json,application/json"
                      onChange={(e) => {
                        const file = e.target.files?.[0];
                        if (file) {
                          if (file.size > 2 * 1024 * 1024) {
                            notify("Files must be smaller than 2 MiB", true);
                            return;
                          }
                          void file
                            .text()
                            .then(setPayload)
                            .catch(() => notify("Could not read file", true));
                        }
                      }}
                    />
                  </label>
                </div>
                <textarea
                  aria-label="JSON points"
                  className="code-editor payload"
                  value={payload}
                  onChange={(e) => setPayload(e.target.value)}
                  spellCheck={false}
                />
                <div className="editor-footer">
                  <span>
                    Destination: <b>{db || "Select a database"}</b>
                  </span>
                  <button
                    className="primary"
                    disabled={!db || !online || busy}
                    onClick={() =>
                      void act(async () => {
                        const body = JSON.parse(payload);
                        const r = await request<{ written: number }>(
                          `${dbPath}/points`,
                          { method: "POST", body: JSON.stringify(body) },
                        );
                        notify(`${r.written} points written to ${db}`);
                        await changed();
                      })
                    }
                  >
                    <Upload size={16} />
                    {busy ? "Writing…" : "Write points"}
                  </button>
                </div>
              </section>
              <section className="panel guide">
                <span className="guide-icon">
                  <Layers size={23} />
                </span>
                <h2>A point, explained</h2>
                <dl>
                  <dt>Measurement</dt>
                  <dd>
                    The name of the time series, such as cpu or temperature.
                  </dd>
                  <dt>Tags</dt>
                  <dd>
                    Dimensions used to identify a series: host, region, sensor.
                  </dd>
                  <dt>Timestamp</dt>
                  <dd>
                    Unix nanoseconds as a decimal string to preserve precision.
                  </dd>
                  <dt>Fields</dt>
                  <dd>
                    Floats, strings, booleans, or{" "}
                    <code>{'{"integer":"42"}'}</code> for exact 64-bit integers.
                  </dd>
                </dl>
                <div className="info-box">
                  The same measurement, tags, and timestamp merges fields. New
                  values replace matching field names.
                </div>
                <button
                  onClick={() =>
                    setPayload(
                      example.replace(
                        "1789142400000000000",
                        (BigInt(Date.now()) * 1000000n).toString(),
                      ),
                    )
                  }
                >
                  Load example with current time
                </button>
              </section>
            </div>
          )}
          {tab === "API & connection" && (
            <div className="write-grid">
              <section className="panel">
                <div className="panel-heading">
                  <div>
                    <h2>HTTP API</h2>
                    <p>Use a bearer token when authentication is enabled.</p>
                  </div>
                  <Code2 size={21} />
                </div>
                <div className="api-content">
                  <label className="scope-label">BASE ENDPOINT</label>
                  <code className="endpoint">{endpoint}</code>
                  {[
                    {
                      title: "Write or update points",
                      method: "POST",
                      path: "/points",
                      body: JSON.stringify(JSON.parse(example)),
                    },
                    {
                      title: "Read points",
                      method: "GET",
                      path: "/points?measurement=cpu&limit=100",
                    },
                    {
                      title: "Query",
                      method: "POST",
                      path: "/query",
                      body: '{"query":"SELECT MEAN(usage) FROM cpu"}',
                    },
                    {
                      title: "Delete an inclusive time range",
                      method: "DELETE",
                      path: "/points",
                      body: '{"measurement":"cpu","tags":{"host":"api-01"},"start":"0","end":"1000000000"}',
                    },
                  ].map((e) => (
                    <div key={e.title}>
                      <h3>{e.title}</h3>
                      <pre>{`curl -X ${e.method} '${endpoint}${e.path}' \\\n  -H 'Authorization: Bearer YOUR_TOKEN'${e.body ? ` \\\n  -H 'Content-Type: application/json' \\\n  -d '${e.body}'` : ""}`}</pre>
                    </div>
                  ))}
                </div>
              </section>
              <div>
                <section className="panel guide">
                  <Server size={25} />
                  <h2>Saved servers</h2>
                  <p className="muted">
                    URLs are saved on this browser. Tokens stay in memory for
                    this session.
                  </p>
                  {profiles.map((p) => (
                    <button
                      className="profile"
                      key={p.url}
                      onClick={() => {
                        setDraftName(p.name);
                        setDraftUrl(p.url);
                        setDraftToken("");
                        setDialog("connect");
                      }}
                    >
                      <span>
                        <b>{p.name}</b>
                        <small>{p.url}</small>
                      </span>
                      <ArrowUpRight size={17} />
                    </button>
                  ))}
                  <button
                    onClick={() => {
                      setDraftToken("");
                      setDialog("connect");
                    }}
                  >
                    <Plus size={16} />
                    Add connection
                  </button>
                </section>
                <section className="panel guide">
                  <ShieldCheck size={24} />
                  <h2>Deployment access</h2>
                  <p>
                    Set <code>FLUXDB_TOKEN</code> on your server and use HTTPS
                    through your reverse proxy. Set allowed browser origins with{" "}
                    <code>FLUXDB_CORS_ORIGINS</code>.
                  </p>
                  <p>
                    Line protocol is available at <code>POST /write?db=…</code>.
                    The v2 aliases provide a subset of InfluxDB compatibility.
                  </p>
                </section>
              </div>
            </div>
          )}
          {tab === "Settings" && (
            <>
              <section className="panel">
                <div className="panel-heading">
                  <div>
                    <h2>Storage</h2>
                    <p>{db || "Select a database"}</p>
                  </div>
                  <HardDrive size={20} />
                </div>
                <div className="settings-row">
                  <div>
                    <h3>Flush to disk</h3>
                    <p>
                      Persist the memtable as a checksummed SSTable. Writes are
                      already recorded in the WAL.
                    </p>
                  </div>
                  <button
                    disabled={!db || !online || busy}
                    onClick={() =>
                      void act(async () => {
                        await request(`${dbPath}/flush`, { method: "POST" });
                        await changed();
                        notify("Memtable flushed successfully");
                      })
                    }
                  >
                    Flush memtable
                  </button>
                </div>
                <div className="settings-row">
                  <div>
                    <h3>Server uptime</h3>
                    <p>
                      {telemetry
                        ? `${format.format(telemetry.uptime_seconds)} seconds`
                        : "Unavailable"}
                    </p>
                  </div>
                  <span className="badge">Single node</span>
                </div>
              </section>
              <section className="panel">
                <div className="panel-heading">
                  <div>
                    <h2>Retention policy</h2>
                    <p>
                      Expired points are removed by the server every 60 seconds.
                    </p>
                  </div>
                </div>
                <div className="settings-row">
                  <div>
                    <label>
                      Keep data for{" "}
                      <input
                        aria-label="Retention days"
                        type="number"
                        min="0"
                        max="3650"
                        step="any"
                        value={retentionDays}
                        onChange={(e) => {
                          setRetentionDays(e.target.value);
                          setRetentionConfirmed(false);
                        }}
                      />{" "}
                      days
                    </label>
                    <p>
                      Use 0 for unlimited retention. Reducing retention
                      permanently deletes expired points.
                    </p>
                    <label className="retention-confirm">
                      <input
                        type="checkbox"
                        checked={retentionConfirmed}
                        onChange={(e) =>
                          setRetentionConfirmed(e.target.checked)
                        }
                      />
                      I understand that expired data will be deleted.
                    </label>
                  </div>
                  <button
                    disabled={busy || !db || !online || !retentionConfirmed}
                    onClick={() =>
                      void act(async () => {
                        const seconds = Math.round(
                          Number(retentionDays) * 86400,
                        );
                        if (
                          !Number.isFinite(seconds) ||
                          seconds < 0 ||
                          seconds > 315360000
                        )
                          throw new Error(
                            "Enter a retention period from 0 to 3650 days",
                          );
                        await request(`${dbPath}/retention`, {
                          method: "PUT",
                          body: JSON.stringify({ seconds }),
                        });
                        await changed();
                        notify("Retention policy saved");
                      })
                    }
                  >
                    Save retention
                  </button>
                </div>
              </section>
              <section className="panel">
                <div className="settings-row">
                  <div>
                    <h3>Compact & checkpoint</h3>
                    <p>
                      Merge SSTables and reclaim obsolete WAL segments with an
                      atomic checkpoint.
                    </p>
                  </div>
                  <button
                    disabled={busy || !db || !online}
                    onClick={() =>
                      void act(async () => {
                        await request(`${dbPath}/compact`, { method: "POST" });
                        await changed();
                        notify("Compaction completed");
                      })
                    }
                  >
                    Compact database
                  </button>
                </div>
                <div className="settings-row">
                  <div>
                    <h3>Export database</h3>
                    <p>
                      Download a consistent JSON snapshot of all live points.
                      Retention metadata is included.
                    </p>
                  </div>
                  <button
                    disabled={busy || !db || !online}
                    onClick={() =>
                      void act(async () => {
                        const snapshot = await request(`${dbPath}/export`);
                        download(
                          JSON.stringify(snapshot, null, 2),
                          `${db}-snapshot.json`,
                        );
                        notify("Snapshot exported");
                      })
                    }
                  >
                    <ArrowDownToLine size={16} />
                    Export snapshot
                  </button>
                </div>
              </section>
              <section className="panel danger-panel">
                <div className="settings-row">
                  <div>
                    <h3>Delete database</h3>
                    <p>Permanently remove this database and its stored data.</p>
                  </div>
                  <button
                    className="danger"
                    disabled={!db || !online || busy}
                    onClick={() => {
                      setConfirm("");
                      setDialog("delete");
                    }}
                  >
                    Delete database
                  </button>
                </div>
              </section>
            </>
          )}
          <footer>
            <span>
              FluxDB Studio <span className="footer-dot">·</span> Rust
              time-series engine
            </span>
            <span>
              {updated
                ? `Last updated ${updated.toLocaleTimeString()}`
                : "Waiting for server"}
            </span>
          </footer>
        </main>
      </div>
      <Assistant
        key={`${connection.url}:${token}`}
        open={assistantOpen}
        onClose={() => setAssistantOpen(false)}
        database={db}
        page={tab}
        serverUrl={connection.url}
        token={token}
        online={online}
        busy={busy}
        request={request}
        onActionBusy={setBusy}
        onChanged={async (message) => {
          await changed();
          notify(message);
        }}
        onOpenQuery={(sql) => {
          setQuery(sql);
          setResult(null);
          setTab("Query workspace");
          setAssistantOpen(false);
        }}
        onOpenWrite={(body) => {
          setPayload(body);
          setTab("Write data");
          setAssistantOpen(false);
        }}
      />
      {(dialog || deletePoint) && (
        <div className="modal-backdrop">
          <div
            ref={modalRef}
            className="modal"
            role="dialog"
            aria-modal="true"
            aria-labelledby="dialog-title"
          >
            <button
              className="modal-close icon-button"
              aria-label="Close dialog"
              disabled={busy}
              onClick={() => {
                setDialog(null);
                setDeletePoint(null);
              }}
            >
              <X size={20} />
            </button>
            <h2 id="dialog-title">
              {deletePoint
                ? "Delete this point?"
                : dialog === "connect"
                  ? "Connect to a server"
                  : dialog === "create"
                    ? "Create a database"
                    : "Delete database?"}
            </h2>
            {notice?.error && (
              <div role="alert" className="notice error">
                <span>{notice.text}</span>
              </div>
            )}
            {dialog === "connect" && (
              <form
                onSubmit={(e) => {
                  e.preventDefault();
                  void connect();
                }}
              >
                <p>Connect to an existing FluxDB HTTP server.</p>
                <label>
                  Connection name
                  <input
                    autoFocus
                    value={draftName}
                    onChange={(e) => setDraftName(e.target.value)}
                    required
                  />
                </label>
                <label>
                  Server URL
                  <input
                    type="url"
                    value={draftUrl}
                    onChange={(e) => setDraftUrl(e.target.value)}
                    required
                  />
                </label>
                <label>
                  Access token <small>(if configured)</small>
                  <input
                    type="password"
                    autoComplete="off"
                    value={draftToken}
                    onChange={(e) => setDraftToken(e.target.value)}
                  />
                </label>
                <p className="muted">
                  Tokens are never saved to browser storage.
                </p>
                <button className="primary" disabled={busy}>
                  {busy ? "Connecting…" : "Test & connect"}
                </button>
              </form>
            )}
            {dialog === "create" && (
              <form
                onSubmit={(e) => {
                  e.preventDefault();
                  void act(async () => {
                    await request(
                      `/api/v1/databases/${encodeURIComponent(newDb)}`,
                      { method: "POST" },
                    );
                    await changed();
                    selectDb(newDb);
                    setDialog(null);
                    setNewDb("");
                    notify("Database created");
                  });
                }}
              >
                <p>Choose a name for your time-series database.</p>
                <label>
                  Database name
                  <input
                    autoFocus
                    value={newDb}
                    onChange={(e) => setNewDb(e.target.value)}
                    pattern="[A-Za-z0-9_-]{1,64}"
                    maxLength={64}
                    required
                    placeholder="observability"
                  />
                </label>
                <p className="muted">
                  Letters, numbers, underscores and hyphens. Up to 64
                  characters.
                </p>
                <button className="primary" disabled={busy || !online}>
                  {busy ? "Creating…" : "Create database"}
                </button>
              </form>
            )}
            {dialog === "delete" && (
              <form
                onSubmit={(e) => {
                  e.preventDefault();
                  void act(async () => {
                    await request(dbPath, { method: "DELETE" });
                    setDialog(null);
                    await changed();
                    notify("Database deleted");
                  });
                }}
              >
                <p>
                  This permanently deletes <b>{db}</b> and all its data. Type
                  its name to confirm.
                </p>
                <label>
                  Database name
                  <input
                    autoFocus
                    value={confirm}
                    onChange={(e) => setConfirm(e.target.value)}
                  />
                </label>
                <button className="danger" disabled={busy || confirm !== db}>
                  Permanently delete
                </button>
              </form>
            )}
            {deletePoint && (
              <>
                <p>
                  Delete <b>{deletePoint.measurement}</b> at{" "}
                  <code>{deletePoint.timestamp}</code> with tags{" "}
                  <code>{JSON.stringify(deletePoint.tags)}</code>?
                </p>
                <button
                  className="danger"
                  disabled={busy}
                  onClick={() =>
                    void act(async () => {
                      await request(`${dbPath}/points`, {
                        method: "DELETE",
                        body: JSON.stringify({
                          measurement: deletePoint.measurement,
                          tags: deletePoint.tags,
                          start: deletePoint.timestamp,
                          end: deletePoint.timestamp,
                          exact: true,
                        }),
                      });
                      setDeletePoint(null);
                      await changed();
                      notify("Point deleted");
                    })
                  }
                >
                  Delete point
                </button>
              </>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
function Empty({
  title,
  text,
  action,
  label,
}: {
  title: string;
  text: string;
  action?: () => void;
  label?: string;
}) {
  return (
    <div className="empty">
      <span>
        <Database size={25} />
      </span>
      <h3>{title}</h3>
      <p>{text}</p>
      {action && (
        <button className="text-button" onClick={action}>
          {label}
          <ArrowUpRight size={15} />
        </button>
      )}
    </div>
  );
}
