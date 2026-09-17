/**
 * The project agent.
 *
 * Three things happen on this screen, and they are deliberately not three
 * separate features: asking a question, running the one-click investigation,
 * and saving a standing question to run on a schedule all produce the same
 * `AgentRun`, so they render through the same components.
 *
 * What makes it worth reading is the timeline. Every tool the agent called is
 * shown in order, with what it returned or why it was refused, so an answer can
 * be checked rather than taken on faith. Proposals are shown as cards with the
 * exact payload; approving one calls the ordinary REST route, where the
 * operator's permissions are checked again.
 */

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  AlertTriangle,
  Ban,
  Bot,
  Check,
  ChevronDown,
  ChevronRight,
  Clock,
  KeyRound,
  Loader2,
  Play,
  Plus,
  Send,
  Sparkles,
  Trash2,
  Wrench,
} from "lucide-react";
import { api, ApiError } from "../lib/api";
import type {
  AgentConfig,
  AgentProposal,
  AgentRun,
  AgentStep,
  SavedAgent,
} from "../lib/types";
import {
  ConfirmDialog,
  EmptyState,
  Modal,
  Notice,
  PageHeader,
  Section,
  Spinner,
  Tabs,
  useAction,
  useLoader,
} from "../components/ui";
import { useProject } from "./ProjectContext";
import { useToast } from "../lib/toast";
import { relative } from "../lib/format";

/** The visitor's own provider key, kept in tab memory only. */
let sessionKey = "";

type Turn =
  | { role: "user"; text: string }
  | { role: "agent"; run: AgentRun }
  | { role: "error"; text: string };

/** One message in the history handed to the server. */
type Message = { role: "user" | "assistant"; text: string };

const EXAMPLES = [
  "Which measurement has the most points, and when did it last receive data?",
  "Why would p99 latency be high right now?",
  "Explain what my dashboard panels are actually measuring.",
  "Are any monitors alerting, and what caused them?",
];

export default function Agent() {
  const { detail, targets } = useProject();
  const [tab, setTab] = useState<"ask" | "scheduled" | "history">("ask");
  const config = useLoader(() => api.agentConfig(), []);

  return (
    <>
      <PageHeader
        title="AI agent"
        description="Ask about this project's data and the agent investigates it: schema, bounded queries, monitors, dashboards and request telemetry. It prepares changes for you to approve and never applies them itself."
      />
      {config.data?.own_key_required && (
        <KeyRequired reason={config.data.own_key_reason} />
      )}
      <Tabs
        value={tab}
        onChange={setTab}
        tabs={[
          { id: "ask", label: "Ask" },
          { id: "scheduled", label: "Scheduled agents" },
          { id: "history", label: "History" },
        ]}
      />
      {tab === "ask" && (
        <Ask
          projectId={detail.project.id}
          config={config.data}
          bucketCount={targets.filter((t) => t.kind === "cloud").length}
        />
      )}
      {tab === "scheduled" && <Scheduled projectId={detail.project.id} />}
      {tab === "history" && <History projectId={detail.project.id} />}
    </>
  );
}

// ---------------------------------------------------------------------------
// Ask
// ---------------------------------------------------------------------------

function Ask({
  projectId,
  config,
  bucketCount,
}: {
  projectId: string;
  config: AgentConfig | null;
  bucketCount: number;
}) {
  const [turns, setTurns] = useState<Turn[]>([]);
  const [question, setQuestion] = useState("");
  const [busy, setBusy] = useState<"chat" | "insights" | null>(null);
  const bottom = useRef<HTMLDivElement>(null);

  useEffect(() => {
    bottom.current?.scrollIntoView({ behavior: "smooth", block: "end" });
  }, [turns, busy]);

  /** History sent to the server: the agent's own summaries, not its timelines. */
  const history = useMemo<Message[]>(
    () =>
      turns.flatMap<Message>((turn) => {
        if (turn.role === "user") return [{ role: "user", text: turn.text }];
        if (turn.role === "agent" && turn.run.summary) {
          return [{ role: "assistant", text: turn.run.summary }];
        }
        return [];
      }),
    [turns],
  );

  const send = useCallback(
    async (text: string) => {
      const trimmed = text.trim();
      if (!trimmed || busy) return;
      setQuestion("");
      setTurns((current) => [...current, { role: "user", text: trimmed }]);
      setBusy("chat");
      try {
        const { run } = await api.ask(
          projectId,
          [...history, { role: "user", text: trimmed }],
          { page: "agent", geminiKey: sessionKey || undefined },
        );
        setTurns((current) => [...current, { role: "agent", run }]);
      } catch (cause) {
        setTurns((current) => [
          ...current,
          {
            role: "error",
            text:
              cause instanceof ApiError
                ? cause.message
                : "The agent could not be reached.",
          },
        ]);
      } finally {
        setBusy(null);
      }
    },
    [busy, history, projectId],
  );

  const investigate = useCallback(async () => {
    if (busy) return;
    setTurns((current) => [
      ...current,
      {
        role: "user",
        text: "Investigate this project and tell me what you find.",
      },
    ]);
    setBusy("insights");
    try {
      const { run } = await api.investigate(projectId, sessionKey || undefined);
      setTurns((current) => [...current, { role: "agent", run }]);
    } catch (cause) {
      setTurns((current) => [
        ...current,
        {
          role: "error",
          text:
            cause instanceof ApiError
              ? cause.message
              : "The investigation could not be started.",
        },
      ]);
    } finally {
      setBusy(null);
    }
  }, [busy, projectId]);

  return (
    <>
      <Section
        title="One click"
        description={
          bucketCount > 1
            ? "Investigates up to four buckets at once, a sub-agent each running concurrently, then reconciles what they found into a single report."
            : "Inspects this project end to end: schema, recent data, monitor state and request telemetry."
        }
      >
        <div className="agent-actions">
          <button
            type="button"
            className="btn btn-primary"
            onClick={() => void investigate()}
            disabled={busy !== null}
          >
            {busy === "insights" ? (
              <Loader2 size={16} className="spin" aria-hidden />
            ) : (
              <Sparkles size={16} aria-hidden />
            )}
            {busy === "insights"
              ? "Investigating…"
              : "Investigate this project"}
          </button>
          {config && (
            <p className="agent-allowance">
              {config.own_key_required ? (
                // The notice above already carries the call to action; showing
                // it twice reads as two different things to do.
                <>Runs on the key you provide</>
              ) : (
                <>
                  {config.limits.per_hour - config.limits.used_this_hour} of{" "}
                  {config.limits.per_hour} investigations left this hour
                  {" · "}
                  <KeyButton />
                </>
              )}
            </p>
          )}
        </div>
      </Section>

      <div className="agent-thread">
        {turns.length === 0 && (
          <EmptyState
            icon={<Bot size={22} aria-hidden />}
            title="Ask about this project"
            description="The agent reads schema, runs bounded queries, and checks monitors and telemetry before answering. It shows every step."
            action={
              <ul className="agent-examples">
                {EXAMPLES.map((example) => (
                  <li key={example}>
                    <button type="button" onClick={() => void send(example)}>
                      {example}
                    </button>
                  </li>
                ))}
              </ul>
            }
          />
        )}
        {turns.map((turn, index) => (
          <TurnView key={index} turn={turn} projectId={projectId} />
        ))}
        {busy && (
          <div className="agent-turn is-agent">
            <Spinner
              label={
                busy === "insights"
                  ? "Fanning out across buckets…"
                  : "Investigating…"
              }
            />
          </div>
        )}
        <div ref={bottom} />
      </div>

      <form
        className="agent-composer"
        onSubmit={(event) => {
          event.preventDefault();
          void send(question);
        }}
      >
        <textarea
          value={question}
          onChange={(event) => setQuestion(event.target.value)}
          onKeyDown={(event) => {
            if (event.key === "Enter" && !event.shiftKey) {
              event.preventDefault();
              void send(question);
            }
          }}
          placeholder="Ask about this project's data, a dashboard, or why something is slow…"
          rows={2}
          maxLength={16000}
          aria-label="Ask the agent"
        />
        <button
          type="submit"
          className="btn btn-primary"
          disabled={busy !== null || !question.trim()}
        >
          <Send size={15} aria-hidden /> Ask
        </button>
      </form>
      <p className="agent-footnote">
        Stored values are treated as data, never as instructions. Changes are
        always proposed for your approval — the agent cannot apply them.
      </p>
      {busy === null && turns.length > 0 && (
        <button
          type="button"
          className="btn btn-ghost btn-sm"
          onClick={() => setTurns([])}
        >
          Clear conversation
        </button>
      )}
    </>
  );
}

function TurnView({ turn, projectId }: { turn: Turn; projectId: string }) {
  if (turn.role === "user") {
    return (
      <div className="agent-turn is-user">
        <p>{turn.text}</p>
      </div>
    );
  }
  if (turn.role === "error") {
    return (
      <div className="agent-turn is-agent">
        <Notice tone="danger" title="The agent stopped">
          {turn.text}
        </Notice>
      </div>
    );
  }
  return <RunView run={turn.run} projectId={projectId} />;
}

// ---------------------------------------------------------------------------
// A run
// ---------------------------------------------------------------------------

export function RunView({
  run,
  projectId,
}: {
  run: AgentRun;
  projectId: string;
}) {
  return (
    <div className="agent-turn is-agent">
      <div className="agent-answer">
        <span className="agent-avatar" aria-hidden>
          <Bot size={16} />
        </span>
        <div className="agent-answer-body">
          {run.summary ? (
            <Answer text={run.summary} />
          ) : (
            <p className="muted">The agent returned no answer.</p>
          )}
          {run.state === "partial" && run.error && (
            <Notice tone="warning" title="Partial result">
              {run.error}
            </Notice>
          )}
          {run.state === "failed" && run.error && (
            <Notice tone="danger" title="The agent failed">
              {run.error}
            </Notice>
          )}
          {run.proposals.length > 0 && (
            <div className="agent-proposals">
              {run.proposals.map((proposal, index) => (
                <ProposalCard
                  key={index}
                  proposal={proposal}
                  projectId={projectId}
                />
              ))}
            </div>
          )}
          {run.steps.length > 0 && <Timeline steps={run.steps} />}
        </div>
      </div>
    </div>
  );
}

/** Renders the model's prose. Paragraphs and simple lists only — no HTML. */
function Answer({ text }: { text: string }) {
  const blocks = text.split(/\n{2,}/).filter((block) => block.trim());
  return (
    <>
      {blocks.map((block, index) => {
        const lines = block.split("\n").filter((line) => line.trim());
        const bulleted = lines.every((line) => /^\s*[-*•]\s+/.test(line));
        const numbered = lines.every((line) => /^\s*\d+[.)]\s+/.test(line));
        if (bulleted && lines.length > 1) {
          return (
            <ul key={index}>
              {lines.map((line, item) => (
                <li key={item}>{line.replace(/^\s*[-*•]\s+/, "")}</li>
              ))}
            </ul>
          );
        }
        if (numbered && lines.length > 1) {
          return (
            <ol key={index}>
              {lines.map((line, item) => (
                <li key={item}>{line.replace(/^\s*\d+[.)]\s+/, "")}</li>
              ))}
            </ol>
          );
        }
        return <p key={index}>{block}</p>;
      })}
    </>
  );
}

/** What the agent actually did, so an answer can be checked. */
function Timeline({ steps }: { steps: AgentStep[] }) {
  const [open, setOpen] = useState(false);
  const failed = steps.filter((step) => !step.ok).length;
  const agents = new Set(
    steps.map((step) => step.agent).filter((name): name is string => !!name),
  );
  return (
    <div className="agent-timeline">
      <button
        type="button"
        className="agent-timeline-toggle"
        onClick={() => setOpen(!open)}
        aria-expanded={open}
      >
        {open ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
        <Wrench size={14} aria-hidden />
        {steps.length} step{steps.length === 1 ? "" : "s"}
        {agents.size > 0 &&
          ` across ${agents.size} sub-agent${agents.size === 1 ? "" : "s"}`}
        {failed > 0 && (
          <span className="agent-timeline-failed">{failed} refused</span>
        )}
      </button>
      {open && (
        <ol className="agent-steps">
          {steps.map((step, index) => (
            <li key={index} className={step.ok ? undefined : "is-failed"}>
              <span className="agent-step-icon" aria-hidden>
                {step.ok ? <Check size={13} /> : <Ban size={13} />}
              </span>
              <code>{step.tool}</code>
              <span className="agent-step-detail">{step.detail}</span>
              {step.agent && (
                <span className="agent-step-agent">{step.agent}</span>
              )}
              <span className="agent-step-time">{step.duration_ms} ms</span>
            </li>
          ))}
        </ol>
      )}
    </div>
  );
}

/** A change the agent prepared. Approving it calls the ordinary REST route. */
function ProposalCard({
  proposal,
  projectId,
}: {
  proposal: AgentProposal;
  projectId: string;
}) {
  const { targets, reload } = useProject();
  const toast = useToast();
  const { run, isBusy } = useAction();
  const [applied, setApplied] = useState(false);
  const [confirming, setConfirming] = useState(false);

  const apply = async () => {
    const target = targets.find(
      (candidate) =>
        candidate.kind === "cloud" && candidate.bucketId === proposal.bucket_id,
    );
    const payload = proposal.payload as Record<string, unknown>;
    try {
      switch (proposal.kind) {
        case "write": {
          if (!target) throw new Error("That bucket is no longer available.");
          const { dataClient } = await import("../lib/api");
          const written = await dataClient(target).writePoints(
            payload.points as never,
          );
          toast.success(`Wrote ${written.written} point(s).`);
          break;
        }
        case "delete_points": {
          if (!target) throw new Error("That bucket is no longer available.");
          const { dataClient } = await import("../lib/api");
          const deleted = await dataClient(target).deletePoints(
            payload as never,
          );
          toast.success(`Deleted ${deleted.deleted} point(s).`);
          break;
        }
        case "retention": {
          if (!proposal.bucket_id) throw new Error("No bucket named.");
          await api.setRetention(
            projectId,
            proposal.bucket_id,
            Number(payload.seconds),
          );
          toast.success("Retention updated.");
          break;
        }
        case "create_bucket": {
          await api.createBucket(
            projectId,
            String(payload.name),
            Number(payload.retention_seconds ?? 0),
          );
          toast.success("Bucket created.");
          break;
        }
        case "drop_bucket": {
          if (!proposal.bucket_id) throw new Error("No bucket named.");
          await api.deleteBucket(projectId, proposal.bucket_id);
          toast.success("Bucket deleted.");
          break;
        }
        case "flush":
        case "compact": {
          if (!target) throw new Error("That bucket is no longer available.");
          const { dataClient } = await import("../lib/api");
          const client = dataClient(target);
          await (proposal.kind === "flush" ? client.flush() : client.compact());
          toast.success(`${proposal.kind} completed.`);
          break;
        }
      }
      setApplied(true);
      reload();
    } catch (cause) {
      toast.failure(cause, "The operation failed.");
    }
  };

  return (
    <div
      className={`agent-proposal${proposal.destructive ? " is-destructive" : ""}`}
    >
      <header>
        {proposal.destructive && <AlertTriangle size={15} aria-hidden />}
        <strong>{label(proposal.kind)}</strong>
        {proposal.bucket_name && <code>{proposal.bucket_name}</code>}
        {applied && <span className="agent-proposal-done">Applied</span>}
      </header>
      <p>{proposal.explanation}</p>
      <details>
        <summary>Exact payload</summary>
        <pre>{JSON.stringify(proposal.payload, null, 2)}</pre>
      </details>
      {!applied && (
        <div className="agent-proposal-actions">
          <button
            type="button"
            className={`btn btn-sm ${proposal.destructive ? "btn-danger" : "btn-primary"}`}
            disabled={isBusy("apply")}
            onClick={() =>
              proposal.destructive
                ? setConfirming(true)
                : void run("apply", apply)
            }
          >
            {proposal.destructive ? "Review and apply" : "Apply"}
          </button>
          <span className="muted">Nothing has been changed yet.</span>
        </div>
      )}
      {confirming && (
        <ConfirmDialog
          title={`${label(proposal.kind)}?`}
          confirmText={proposal.bucket_name ?? undefined}
          confirmLabel="Apply"
          description={
            <>
              <p>{proposal.explanation}</p>
              <p>
                This was prepared by the agent. Check the payload before
                applying; it cannot be undone.
              </p>
            </>
          }
          onClose={() => setConfirming(false)}
          onConfirm={() => {
            setConfirming(false);
            void run("apply", apply);
          }}
        />
      )}
    </div>
  );
}

function label(kind: AgentProposal["kind"]): string {
  return {
    write: "Write points",
    delete_points: "Delete points",
    retention: "Change retention",
    create_bucket: "Create bucket",
    drop_bucket: "Delete bucket",
    flush: "Flush to disk",
    compact: "Compact",
  }[kind];
}

// ---------------------------------------------------------------------------
// Own key
// ---------------------------------------------------------------------------

function KeyRequired({ reason }: { reason: string }) {
  return (
    <Notice tone="info" title="Add your own AI key to use the agent">
      {reason} <KeyButton />
    </Notice>
  );
}

function KeyButton() {
  const [open, setOpen] = useState(false);
  const [draft, setDraft] = useState(sessionKey);
  return (
    <>
      <button
        type="button"
        className="btn btn-ghost btn-sm"
        onClick={() => {
          setDraft(sessionKey);
          setOpen(true);
        }}
      >
        <KeyRound size={14} aria-hidden />
        {sessionKey ? "Change your key" : "Use your own key"}
      </button>
      {open && (
        <Modal
          title="Your Gemini API key"
          onClose={() => setOpen(false)}
          footer={
            <>
              <button
                type="button"
                className="btn btn-ghost"
                onClick={() => {
                  sessionKey = "";
                  setOpen(false);
                }}
              >
                Clear
              </button>
              <button
                type="button"
                className="btn btn-primary"
                onClick={() => {
                  sessionKey = draft.trim();
                  setOpen(false);
                }}
              >
                Save for this tab
              </button>
            </>
          }
        >
          <p>
            Held in this browser tab only. It is sent with each request to this
            server, forwarded to Google, and never written to disk or to the
            database. Reload the page and it is gone.
          </p>
          <label className="field">
            <span>API key</span>
            <input
              type="password"
              value={draft}
              onChange={(event) => setDraft(event.target.value)}
              placeholder="AIza…"
              autoComplete="off"
              spellCheck={false}
            />
          </label>
          <p className="muted">
            Create one at{" "}
            <a
              href="https://aistudio.google.com/apikey"
              target="_blank"
              rel="noreferrer noopener"
            >
              Google AI Studio
            </a>
            . Usage is billed to your own API project.
          </p>
        </Modal>
      )}
    </>
  );
}

// ---------------------------------------------------------------------------
// Scheduled agents
// ---------------------------------------------------------------------------

function Scheduled({ projectId }: { projectId: string }) {
  const agents = useLoader(
    () => api.savedAgents(projectId).then((r) => r.agents),
    [projectId],
  );
  const [editing, setEditing] = useState<SavedAgent | "new" | null>(null);
  const [removing, setRemoving] = useState<SavedAgent | null>(null);
  const [lastRun, setLastRun] = useState<AgentRun | null>(null);
  const { run, isBusy } = useAction();
  const toast = useToast();

  return (
    <Section
      title="Standing investigations"
      description="A saved question the agent runs on a schedule, recording what it finds. Runs happen server-side using the workspace's AI allowance; findings appear under History."
      actions={
        <button
          type="button"
          className="btn btn-primary btn-sm"
          onClick={() => setEditing("new")}
        >
          <Plus size={15} aria-hidden /> New agent
        </button>
      }
    >
      {agents.loading && <Spinner label="Loading agents" />}
      {agents.error && <Notice tone="danger">{agents.error}</Notice>}
      {agents.data?.length === 0 && (
        <EmptyState
          icon={<Clock size={22} aria-hidden />}
          title="No scheduled agents"
          description="Save a question like “check error rates across every bucket and report anything unusual” and have it run nightly."
        />
      )}
      {agents.data && agents.data.length > 0 && (
        <ul className="agent-list">
          {agents.data.map((agent) => (
            <li key={agent.id}>
              <div className="agent-list-main">
                <strong>{agent.name}</strong>
                <p>{agent.instruction}</p>
                <span className="muted">
                  {agent.interval_minutes === 0
                    ? "Manual only"
                    : `Every ${humanInterval(agent.interval_minutes)}`}
                  {!agent.enabled && " · paused"}
                  {agent.last_run_at &&
                    ` · last run ${relative(agent.last_run_at)} (${agent.last_state})`}
                </span>
              </div>
              <div className="agent-list-actions">
                <button
                  type="button"
                  className="btn btn-ghost btn-sm"
                  disabled={isBusy(agent.id)}
                  onClick={() =>
                    void run(agent.id, async () => {
                      try {
                        const { run: result } = await api.runSavedAgent(
                          projectId,
                          agent.id,
                          sessionKey || undefined,
                        );
                        setLastRun(result);
                        agents.reload();
                      } catch (cause) {
                        toast.failure(cause, "The agent failed.");
                      }
                    })
                  }
                >
                  {isBusy(agent.id) ? (
                    <Loader2 size={14} className="spin" aria-hidden />
                  ) : (
                    <Play size={14} aria-hidden />
                  )}
                  Run now
                </button>
                <button
                  type="button"
                  className="btn btn-ghost btn-sm"
                  onClick={() => setEditing(agent)}
                >
                  Edit
                </button>
                <button
                  type="button"
                  className="btn btn-ghost btn-sm"
                  onClick={() => setRemoving(agent)}
                  aria-label={`Delete ${agent.name}`}
                >
                  <Trash2 size={14} aria-hidden />
                </button>
              </div>
            </li>
          ))}
        </ul>
      )}
      {lastRun && <RunView run={lastRun} projectId={projectId} />}
      {editing && (
        <AgentForm
          projectId={projectId}
          agent={editing === "new" ? null : editing}
          onClose={() => setEditing(null)}
          onSaved={() => {
            setEditing(null);
            agents.reload();
          }}
        />
      )}
      {removing && (
        <ConfirmDialog
          title={`Delete ${removing.name}?`}
          confirmLabel="Delete agent"
          description="Its recorded findings stay under History."
          onClose={() => setRemoving(null)}
          onConfirm={() => {
            void (async () => {
              await api.deleteSavedAgent(projectId, removing.id);
              setRemoving(null);
              agents.reload();
            })();
          }}
        />
      )}
    </Section>
  );
}

function AgentForm({
  projectId,
  agent,
  onClose,
  onSaved,
}: {
  projectId: string;
  agent: SavedAgent | null;
  onClose: () => void;
  onSaved: () => void;
}) {
  const [name, setName] = useState(agent?.name ?? "");
  const [instruction, setInstruction] = useState(agent?.instruction ?? "");
  const [interval, setInterval] = useState(agent?.interval_minutes ?? 1440);
  const [enabled, setEnabled] = useState(agent?.enabled ?? true);
  const [error, setError] = useState<string | null>(null);
  const { run, isBusy } = useAction();

  return (
    <Modal
      title={agent ? `Edit ${agent.name}` : "New scheduled agent"}
      onClose={onClose}
    >
      <form
        onSubmit={(event) => {
          event.preventDefault();
          void run("save", async () => {
            setError(null);
            const input = {
              name,
              instruction,
              interval_minutes: interval,
              enabled,
            };
            try {
              if (agent) {
                await api.updateSavedAgent(projectId, agent.id, input);
              } else {
                await api.createSavedAgent(projectId, input);
              }
              onSaved();
            } catch (cause) {
              setError(
                cause instanceof Error ? cause.message : "Could not save.",
              );
            }
          });
        }}
      >
        {error && <Notice tone="danger">{error}</Notice>}
        <label className="field">
          <span>Name</span>
          <input
            value={name}
            onChange={(event) => setName(event.target.value)}
            maxLength={80}
            required
            placeholder="Nightly error sweep"
          />
        </label>
        <label className="field">
          <span>What should it investigate?</span>
          <textarea
            value={instruction}
            onChange={(event) => setInstruction(event.target.value)}
            rows={4}
            maxLength={2000}
            required
            placeholder="Check error rates and p99 latency across every bucket, and report anything that looks unusual compared with the rest of the day."
          />
        </label>
        <label className="field">
          <span>How often</span>
          <select
            value={interval}
            onChange={(event) => setInterval(Number(event.target.value))}
          >
            <option value={0}>Only when I ask</option>
            <option value={60}>Every hour</option>
            <option value={360}>Every 6 hours</option>
            <option value={1440}>Every day</option>
            <option value={10080}>Every week</option>
          </select>
        </label>
        <label className="field field-check">
          <input
            type="checkbox"
            checked={enabled}
            onChange={(event) => setEnabled(event.target.checked)}
          />
          <span>Enabled</span>
        </label>
        <p className="muted">
          Scheduled runs spend this workspace's AI allowance. The agent still
          only proposes changes — a scheduled run never applies anything.
        </p>
        <div className="modal-actions">
          <button type="button" className="btn btn-ghost" onClick={onClose}>
            Cancel
          </button>
          <button
            type="submit"
            className="btn btn-primary"
            disabled={isBusy("save")}
          >
            {agent ? "Save changes" : "Create agent"}
          </button>
        </div>
      </form>
    </Modal>
  );
}

/** "Every hour", "Every 6 hours" — not "Every 1 hour(s)". */
function humanInterval(minutes: number): string {
  const unit = (count: number, noun: string) =>
    count === 1 ? noun : `${count} ${noun}s`;
  if (minutes % 10080 === 0) return unit(minutes / 10080, "week");
  if (minutes % 1440 === 0) return unit(minutes / 1440, "day");
  if (minutes % 60 === 0) return unit(minutes / 60, "hour");
  return `${minutes} minutes`;
}

// ---------------------------------------------------------------------------
// History
// ---------------------------------------------------------------------------

function History({ projectId }: { projectId: string }) {
  const runs = useLoader(
    () => api.agentRuns(projectId).then((r) => r.runs),
    [projectId],
  );
  const [open, setOpen] = useState<string | null>(null);

  return (
    <Section
      title="Previous investigations"
      description="Every run is recorded with the tools it called, including failures and anything it proposed. Also written to the audit trail."
    >
      {runs.loading && <Spinner label="Loading history" />}
      {runs.error && <Notice tone="danger">{runs.error}</Notice>}
      {runs.data?.length === 0 && (
        <EmptyState
          icon={<Bot size={22} aria-hidden />}
          title="Nothing yet"
          description="Ask the agent something, or run the one-click investigation."
        />
      )}
      {runs.data && runs.data.length > 0 && (
        <ul className="agent-runs">
          {runs.data.map((run) => (
            <li key={run.id}>
              <button
                type="button"
                className="agent-run-head"
                onClick={() => setOpen(open === run.id ? null : run.id)}
                aria-expanded={open === run.id}
              >
                {open === run.id ? (
                  <ChevronDown size={14} />
                ) : (
                  <ChevronRight size={14} />
                )}
                <span className={`badge badge-${badge(run.state)}`}>
                  {run.state}
                </span>
                <span className="agent-run-kind">{run.kind}</span>
                <span className="agent-run-question">{run.question}</span>
                <span className="muted">{relative(run.at)}</span>
              </button>
              {open === run.id && <RunView run={run} projectId={projectId} />}
            </li>
          ))}
        </ul>
      )}
    </Section>
  );
}

function badge(state: AgentRun["state"]): string {
  return state === "ok"
    ? "success"
    : state === "partial"
      ? "warning"
      : "danger";
}
