import { useEffect, useRef, useState } from "react";
import {
  ArrowUpRight,
  Check,
  ChevronDown,
  KeyRound,
  Loader2,
  Send,
  Settings2,
  ShieldCheck,
  Sparkles,
  Trash2,
  X,
} from "lucide-react";
import "./assistant.css";

type Action = {
  operation: string;
  database: string;
  payload: Record<string, unknown>;
  explanation: string;
};
type Step = { tool: string; status: string; detail: string };
type Entry = {
  id: number;
  role: "user" | "assistant";
  text: string;
  actions?: Action[];
  steps?: Step[];
  result?: unknown;
};
type Reply = { reply: string; actions: Action[]; steps: Step[]; model: string };
type Config = { configured: boolean; default_model: string };
type Request = <T = unknown>(path: string, options?: RequestInit) => Promise<T>;
type Props = {
  open: boolean;
  onClose: () => void;
  database: string;
  page: string;
  serverUrl: string;
  token: string;
  online: boolean;
  busy: boolean;
  request: Request;
  onChanged: (message: string) => Promise<void>;
  onActionBusy: (busy: boolean) => void;
  onOpenQuery: (query: string) => void;
  onOpenWrite: (payload: string) => void;
};
const labels: Record<string, string> = {
  query: "Run SQL query",
  write: "Write / update points",
  delete_points: "Delete points",
  create_database: "Create database",
  drop_database: "Delete database",
  retention: "Change retention",
  flush: "Flush memtable",
  compact: "Compact database",
  export: "Export snapshot",
};
const routes: Record<string, { method: string; suffix: string }> = {
  query: { method: "POST", suffix: "/query" },
  write: { method: "POST", suffix: "/points" },
  delete_points: { method: "DELETE", suffix: "/points" },
  create_database: { method: "POST", suffix: "" },
  drop_database: { method: "DELETE", suffix: "" },
  retention: { method: "PUT", suffix: "/retention" },
  flush: { method: "POST", suffix: "/flush" },
  compact: { method: "POST", suffix: "/compact" },
  export: { method: "GET", suffix: "/export" },
};
function preview(value: unknown) {
  const text = JSON.stringify(value, null, 2) || "Completed";
  return text.length > 14000
    ? text.slice(0, 14000) +
        "\n… Preview truncated. Open Query workspace for full results."
    : text;
}
function Text({ text }: { text: string }) {
  return (
    <>
      {text.split(/(```[\s\S]*?```)/g).map((part, i) =>
        part.startsWith("```") ? (
          <pre key={i}>
            <code>{part.replace(/^```[^\n]*\n?/, "").replace(/```$/, "")}</code>
          </pre>
        ) : (
          <p key={i}>{part}</p>
        ),
      )}
    </>
  );
}
export default function Assistant(props: Props) {
  const { open, database, page, serverUrl, token, request } = props;
  const [config, setConfig] = useState<Config | null>(null);
  const [model, setModel] = useState("");
  const [models, setModels] = useState<{ id: string; name: string }[]>([]);
  const [loadingModels, setLoadingModels] = useState(false);
  const [key, setKey] = useState("");
  const [settings, setSettings] = useState(false);
  const [allowRead, setAllowRead] = useState(false);
  const [entries, setEntries] = useState<Entry[]>([]);
  const [draft, setDraft] = useState("");
  const [working, setWorking] = useState(false);
  const [error, setError] = useState("");
  const [status, setStatus] = useState<
    Record<
      string,
      { state: "running" | "done" | "error"; result?: unknown; error?: string }
    >
  >({});
  const [reviews, setReviews] = useState<Record<string, string>>({});
  const sequence = useRef(0);
  const generation = useRef(0);
  const controller = useRef<AbortController | null>(null);
  const input = useRef<HTMLTextAreaElement>(null);
  const log = useRef<HTMLDivElement>(null);
  const gate = useRef(false);
  const completed = useRef(new Set<string>());
  const scope = useRef(database);
  scope.current = database;
  useEffect(() => {
    generation.current++;
    controller.current?.abort();
    controller.current = null;
    gate.current = false;
    setEntries([]);
    setStatus({});
    setReviews({});
    setError("");
    setDraft("");
    setWorking(false);
    completed.current.clear();
  }, [database]);
  useEffect(
    () => () => {
      generation.current++;
      controller.current?.abort();
    },
    [],
  );
  useEffect(() => {
    if (!open) return;
    let active = true;
    void request<Config>("/api/v1/assistant/config")
      .then((value) => {
        if (active) {
          setConfig(value);
          setModel((old) => old || value.default_model);
          if (!value.configured) setSettings(true);
        }
      })
      .catch((e) => {
        if (active) setError(e.message);
      });
    input.current?.focus();
    const mobile = window.matchMedia?.("(max-width: 600px)");
    const background = Array.from(
      document.querySelectorAll<HTMLElement>(".sidebar,.main-shell"),
    );
    const updateMobile = () =>
      background.forEach((node) => {
        node.inert = Boolean(mobile?.matches);
      });
    updateMobile();
    mobile?.addEventListener("change", updateMobile);
    const escape = (e: KeyboardEvent) => {
      if (e.key === "Escape") props.onClose();
      if (e.key === "Tab" && mobile?.matches) {
        const items = Array.from(
          document.querySelectorAll<HTMLElement>(
            ".assistant-panel button:not(:disabled),.assistant-panel input:not(:disabled),.assistant-panel select:not(:disabled),.assistant-panel textarea:not(:disabled),.assistant-panel a[href]",
          ),
        );
        const first = items[0],
          last = items[items.length - 1];
        if (e.shiftKey && document.activeElement === first) {
          e.preventDefault();
          last?.focus();
        } else if (!e.shiftKey && document.activeElement === last) {
          e.preventDefault();
          first?.focus();
        }
      }
    };
    document.addEventListener("keydown", escape);
    return () => {
      active = false;
      document.removeEventListener("keydown", escape);
      mobile?.removeEventListener("change", updateMobile);
      background.forEach((node) => {
        node.inert = false;
      });
    };
  }, [open, request]);
  useEffect(() => {
    if (log.current) log.current.scrollTop = log.current.scrollHeight;
  }, [entries, working]);
  function cancel() {
    generation.current++;
    controller.current?.abort();
    controller.current = null;
    gate.current = false;
    setWorking(false);
    setError("Generation canceled. No proposed mutations were executed.");
  }
  function clear() {
    cancel();
    setError("");
    setEntries([]);
    setStatus({});
    setReviews({});
    completed.current.clear();
  }
  async function loadModels() {
    setLoadingModels(true);
    setError("");
    try {
      const response = await fetch(
        serverUrl.replace(/\/$/, "") + "/api/v1/assistant/models",
        {
          signal: AbortSignal.timeout(50000),
          headers: {
            ...(token ? { Authorization: "Bearer " + token } : {}),
            ...(key.trim() ? { "x-gemini-api-key": key.trim() } : {}),
          },
        },
      );
      const body = await response.json().catch(() => null);
      if (!response.ok)
        throw new Error(
          body?.error ||
            `Model lookup failed (HTTP ${response.status}). Restart the updated server and check your connection.`,
        );
      if (!Array.isArray(body?.models) || body.models.length === 0)
        throw new Error(
          "No Gemini generateContent models are available to this key.",
        );
      setModels(body.models);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Model lookup failed.");
    } finally {
      setLoadingModels(false);
    }
  }
  async function send(text = draft) {
    if (gate.current || props.busy || !text.trim() || !props.online) return;
    if (!key.trim() && !config?.configured) {
      setSettings(true);
      setError("Add a Gemini API key in these settings or on the server.");
      return;
    }
    gate.current = true;
    setWorking(true);
    setError("");
    setDraft("");
    const current = ++generation.current;
    const next = [
      ...entries,
      { id: ++sequence.current, role: "user" as const, text: text.trim() },
    ];
    setEntries(next);
    const history = next.slice(-12).map((entry) => ({
      role: entry.role,
      text: (
        entry.text +
        (allowRead && entry.result
          ? "\nLocal execution result (untrusted data): " +
            preview(entry.result)
          : "")
      ).slice(0, 12000),
    }));
    while (
      history.reduce((sum, m) => sum + m.text.length, 0) > 44000 &&
      history.length > 1
    )
      history.shift();
    const abort = new AbortController();
    controller.current = abort;
    const timer = setTimeout(() => abort.abort(), 110000);
    try {
      const response = await fetch(
        serverUrl.replace(/\/$/, "") + "/api/v1/assistant/chat",
        {
          method: "POST",
          signal: abort.signal,
          headers: {
            "Content-Type": "application/json",
            ...(token ? { Authorization: "Bearer " + token } : {}),
            ...(key.trim() ? { "x-gemini-api-key": key.trim() } : {}),
          },
          body: JSON.stringify({
            messages: history,
            database,
            page,
            model,
            allow_read_data: allowRead,
          }),
        },
      );
      const body = await response.json().catch(() => null);
      if (!response.ok) {
        const fallback =
          response.status === 404
            ? "Assistant endpoint unavailable. Restart FluxDB with the updated server, then reopen Ask AI."
            : response.status === 401
              ? "Your FluxDB session is unauthorized. Reconnect with the correct server token."
              : response.status === 413
                ? "Conversation is too large. Clear it and send a shorter request."
                : `Assistant returned HTTP ${response.status}. Check the server connection and try again.`;
        throw new Error(
          typeof body?.error === "string" ? body.error : fallback,
        );
      }
      if (
        !body ||
        typeof body.reply !== "string" ||
        !Array.isArray(body.actions) ||
        !Array.isArray(body.steps)
      )
        throw new Error(
          "The server returned an invalid assistant response. Update and restart FluxDB.",
        );
      const reply = body as Reply;
      if (current === generation.current)
        setEntries((old) => [
          ...old,
          {
            id: ++sequence.current,
            role: "assistant",
            text: reply.reply,
            actions: reply.actions,
            steps: reply.steps,
          },
        ]);
    } catch (e) {
      if (current === generation.current)
        setError(
          e instanceof Error
            ? e.name === "AbortError"
              ? "Assistant timed out. No proposed mutations were executed."
              : e instanceof TypeError
                ? "Cannot reach the assistant. Check your server connection, network, and allowed browser origins."
                : e.message
            : "Assistant request failed.",
        );
    } finally {
      clearTimeout(timer);
      if (current === generation.current) {
        setWorking(false);
        gate.current = false;
        controller.current = null;
      }
    }
  }
  async function execute(action: Action, id: string) {
    if (gate.current || props.busy || completed.current.has(id)) return;
    if (
      action.operation !== "create_database" &&
      action.database !== scope.current
    ) {
      setError(
        "This proposal belongs to a different database. Ask again in the current context.",
      );
      return;
    }
    const route = routes[action.operation];
    if (!route) {
      setError("Unsupported operation");
      return;
    }
    gate.current = true;
    setWorking(true);
    props.onActionBusy(true);
    setError("");
    setStatus((old) => ({ ...old, [id]: { state: "running" } }));
    try {
      const result = await request<unknown>(
        "/api/v1/databases/" +
          encodeURIComponent(action.database) +
          route.suffix,
        {
          method: route.method,
          ...(["query", "write", "delete_points", "retention"].includes(
            action.operation,
          )
            ? { body: JSON.stringify(action.payload) }
            : {}),
        },
      );
      let localResult = result;
      if (action.operation === "export") {
        const blob = new Blob([JSON.stringify(result, null, 2)], {
          type: "application/json",
        });
        const url = URL.createObjectURL(blob);
        const link = document.createElement("a");
        link.href = url;
        link.download = action.database + "-snapshot.json";
        link.click();
        setTimeout(() => URL.revokeObjectURL(url), 1000);
        localResult = { downloaded: action.database + "-snapshot.json" };
      }
      completed.current.add(id);
      setStatus((old) => ({
        ...old,
        [id]: { state: "done", result: localResult },
      }));
      setEntries((old) => [
        ...old,
        {
          id: ++sequence.current,
          role: "assistant",
          text: `Executed ${labels[action.operation]} on ${action.database} successfully.`,
          result: localResult,
        },
      ]);
      await props.onChanged(
        `Assistant: ${labels[action.operation]} completed for ${action.database}`,
      );
    } catch (e) {
      setStatus((old) => ({
        ...old,
        [id]: {
          state: "error",
          error: e instanceof Error ? e.message : "Operation failed",
        },
      }));
    } finally {
      gate.current = false;
      setWorking(false);
      props.onActionBusy(false);
    }
  }
  if (!open) return null;
  return (
    <aside className="assistant-panel" aria-label="FluxDB assistant">
      <header className="assistant-header">
        <div className="assistant-mark">
          <Sparkles size={20} />
        </div>
        <div>
          <h2>FluxDB Copilot</h2>
          <span>Your database, in conversation</span>
        </div>
        <button
          className="icon-button"
          title="Assistant settings"
          aria-label="Assistant settings"
          onClick={() => setSettings(!settings)}
        >
          <Settings2 size={18} />
        </button>
        <button
          className="icon-button"
          aria-label="Close assistant"
          onClick={props.onClose}
        >
          <X size={20} />
        </button>
      </header>
      <div className="assistant-context">
        <span>
          <i /> {database || "No database selected"}
        </span>
        <small>{page}</small>
      </div>
      {settings && (
        <section className="assistant-settings" aria-label="Gemini settings">
          <div className="assistant-settings-title">
            <KeyRound size={16} />
            <strong>Gemini connection</strong>
            <button
              className="icon-button"
              aria-label="Collapse Gemini settings"
              onClick={() => setSettings(false)}
            >
              <ChevronDown size={16} />
            </button>
          </div>
          <p>
            {config?.configured
              ? "A server key is available. Your session key overrides it."
              : "Set GEMINI_API_KEY in the server .env or use a session key below."}
          </p>
          <label>
            Gemini API key{" "}
            <input
              type="password"
              autoComplete="off"
              placeholder="Session only · never saved"
              value={key}
              disabled={working}
              onChange={(e) => setKey(e.target.value)}
            />
          </label>
          {key && (
            <button
              className="text-button"
              disabled={working}
              onClick={() => setKey("")}
            >
              Clear session key
            </button>
          )}
          <label>
            Gemini model ID{" "}
            <input
              list="gemini-models"
              value={model}
              disabled={working}
              onChange={(e) => setModel(e.target.value)}
              placeholder="gemini-3.8-flash"
            />
          </label>
          <datalist id="gemini-models">
            <option value="gemini-3.8-flash" />
            <option value="gemini-2.5-pro" />
            <option value="gemini-3.1-pro-preview" />
          </datalist>
          <button
            className="text-button"
            disabled={
              working || loadingModels || (!key.trim() && !config?.configured)
            }
            onClick={() => void loadModels()}
          >
            {loadingModels
              ? "Checking Gemini connection…"
              : "Check key & load available models"}
          </button>
          {models.length > 0 && (
            <label>
              Available models
              <select
                value={models.some((item) => item.id === model) ? model : ""}
                onChange={(event) => setModel(event.target.value)}
                disabled={working}
              >
                <option value="" disabled>
                  Select a model from your API project
                </option>
                {models.map((item) => (
                  <option key={item.id} value={item.id}>
                    {item.name || item.id} ({item.id})
                  </option>
                ))}
              </select>
            </label>
          )}
          <label className="assistant-check">
            <input
              type="checkbox"
              checked={allowRead}
              disabled={working}
              onChange={(e) => setAllowRead(e.target.checked)}
            />
            <span>
              Allow read-only data analysis
              <small>
                Send up to 100 query rows per tool call and local execution
                previews to Gemini.
              </small>
            </span>
          </label>
          <p className="assistant-privacy">
            Your messages, selected database name, page, and requested schema
            metadata go to Google through this server. Keys stay in memory or
            server environment. Only use a server you trust. API access and
            billing are managed in{" "}
            <a
              href="https://aistudio.google.com/apikey"
              target="_blank"
              rel="noreferrer"
            >
              Google AI Studio
            </a>
            .
          </p>
        </section>
      )}
      <div
        ref={log}
        className="assistant-log"
        role="log"
        aria-live="polite"
        aria-relevant="additions"
      >
        {entries.length === 0 && (
          <div className="assistant-welcome">
            <div className="assistant-orbit">
              <Sparkles size={28} />
            </div>
            <span>BUILT FOR YOUR WORKSPACE</span>
            <h3>What would you like to do?</h3>
            <p>
              Explore your schema, turn a question into SQL, or prepare a
              database operation.
            </p>
            <div className="assistant-suggestions">
              {[
                "Explain how FluxDB stores my data",
                "Inspect my schema and suggest a useful query",
                "Help me write my first data point",
                "How do I back up this database?",
              ].map((text) => (
                <button
                  key={text}
                  disabled={working || props.busy || !props.online}
                  onClick={() => void send(text)}
                >
                  {text}
                  <ArrowUpRight size={15} />
                </button>
              ))}
            </div>
          </div>
        )}
        {entries.map((entry) => (
          <div key={entry.id} className={`assistant-entry ${entry.role}`}>
            <span className="assistant-speaker">
              {entry.role === "user" ? "You" : "FluxDB Copilot"}
            </span>
            <Text text={entry.text} />
            {entry.steps && entry.steps.length > 0 && (
              <details className="assistant-steps">
                <summary>
                  {entry.steps.length} tool steps ·{" "}
                  {entry.steps.some((s) => s.status === "error")
                    ? "includes feedback"
                    : "completed"}
                </summary>
                {entry.steps.map((s, i) => (
                  <div key={i}>
                    <span>
                      {s.status === "error" ? "•" : "✓"}{" "}
                      {s.tool.replace(/_/g, " ")}
                    </span>
                    <small>{s.detail}</small>
                  </div>
                ))}
              </details>
            )}
            {entry.actions?.map((action, index) => {
              const id = entry.id + ":" + index;
              const state = status[id];
              const destructive = [
                "delete_points",
                "drop_database",
                "retention",
              ].includes(action.operation);
              const mutating = !["query", "export"].includes(action.operation);
              const reviewed =
                !mutating ||
                (destructive
                  ? reviews[id] === action.database
                  : reviews[id] === "reviewed");
              return (
                <section
                  key={id}
                  className={`assistant-action ${destructive ? "destructive" : ""}`}
                  aria-label={`Proposed ${labels[action.operation] || action.operation}`}
                >
                  <div className="assistant-action-title">
                    <span>
                      {destructive
                        ? "DESTRUCTIVE"
                        : mutating
                          ? "REVIEW REQUIRED"
                          : "READ ONLY"}
                    </span>
                    {state?.state === "done" && <Check size={16} />}
                  </div>
                  <h4>{labels[action.operation] || action.operation}</h4>
                  <p>{action.explanation}</p>
                  <div className="assistant-target">
                    Target database <b>{action.database}</b>
                  </div>
                  <pre>
                    <code>
                      {action.operation === "query"
                        ? String(action.payload.query)
                        : JSON.stringify(action.payload, null, 2)}
                    </code>
                  </pre>
                  {!state?.state || state.state === "error" ? (
                    <>
                      {destructive ? (
                        <label className="assistant-confirm">
                          Type <b>{action.database}</b> to confirm
                          <input
                            aria-label={`Confirm ${action.operation} database`}
                            value={reviews[id] || ""}
                            onChange={(e) =>
                              setReviews((old) => ({
                                ...old,
                                [id]: e.target.value,
                              }))
                            }
                          />
                        </label>
                      ) : mutating ? (
                        <label className="assistant-check">
                          <input
                            type="checkbox"
                            checked={reviewed}
                            onChange={(e) =>
                              setReviews((old) => ({
                                ...old,
                                [id]: e.target.checked ? "reviewed" : "",
                              }))
                            }
                          />
                          I reviewed this operation and its target.
                        </label>
                      ) : null}
                      <div className="assistant-action-buttons">
                        <button
                          className={destructive ? "danger" : "primary"}
                          disabled={
                            !reviewed || working || props.busy || !props.online
                          }
                          onClick={() => void execute(action, id)}
                        >
                          {mutating ? "Confirm & run" : "Run operation"}
                        </button>
                        {action.operation === "query" && (
                          <button
                            disabled={working || props.busy}
                            onClick={() =>
                              props.onOpenQuery(String(action.payload.query))
                            }
                          >
                            Open in editor
                          </button>
                        )}
                        {action.operation === "write" && (
                          <button
                            disabled={working || props.busy}
                            onClick={() =>
                              props.onOpenWrite(
                                JSON.stringify(action.payload, null, 2),
                              )
                            }
                          >
                            Open in writer
                          </button>
                        )}
                      </div>
                    </>
                  ) : null}
                  {state?.state === "running" && (
                    <p role="status">Executing confirmed operation…</p>
                  )}
                  {state?.state === "done" && (
                    <p className="assistant-success">✓ Operation completed</p>
                  )}
                  {state?.state === "error" && (
                    <p className="assistant-error" role="alert">
                      {state.error} Check whether the operation committed before
                      retrying.
                    </p>
                  )}
                  {state?.result != null && (
                    <details>
                      <summary>
                        Local result · shared only if data analysis is enabled
                      </summary>
                      <pre>{preview(state.result)}</pre>
                    </details>
                  )}
                </section>
              );
            })}
          </div>
        ))}
        {working && (
          <div className="assistant-thinking" role="status">
            <Loader2 size={16} className="spin" />
            {controller.current
              ? "Inspecting context and preparing a response…"
              : "Running your confirmed operation…"}
          </div>
        )}
      </div>
      <div className="assistant-compose">
        {error && (
          <div role="alert" className="assistant-error">
            {error}
          </div>
        )}
        <div className="assistant-trust">
          <ShieldCheck size={14} />
          <span>
            {allowRead ? "Bounded reads enabled" : "Schema context only"} ·
            Changes require review
          </span>
          <button
            className="icon-button"
            aria-label="Clear assistant conversation"
            disabled={working}
            onClick={clear}
          >
            <Trash2 size={14} />
          </button>
        </div>
        <form
          onSubmit={(e) => {
            e.preventDefault();
            void send();
          }}
        >
          <textarea
            ref={input}
            aria-label="Message FluxDB assistant"
            placeholder="Ask about your data or describe an operation…"
            value={draft}
            maxLength={8000}
            disabled={working}
            onChange={(e) => setDraft(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter" && !e.shiftKey) {
                e.preventDefault();
                void send();
              }
            }}
          />
          <button
            className="assistant-send"
            aria-label="Send assistant message"
            disabled={working || props.busy || !props.online || !draft.trim()}
          >
            <Send size={17} />
          </button>
        </form>
        <div className="assistant-compose-footer">
          <span>{model || "Gemini"} · Check AI-generated operations</span>
          {working && controller.current && (
            <button onClick={cancel}>Cancel generation</button>
          )}
        </div>
      </div>
    </aside>
  );
}
