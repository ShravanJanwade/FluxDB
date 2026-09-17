import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  cleanup,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import type { AgentConfig, AgentRun } from "../src/lib/types";

/**
 * The agent screen's job is to make an answer checkable and a change
 * deliberate. These cover exactly that: the tool timeline is present, and a
 * proposal cannot reach the server without an explicit click — a destructive
 * one not without typing the bucket name.
 */

const config: AgentConfig = {
  provider: "Gemini",
  default_model: "gemini-2.5-flash",
  server_key_available: true,
  own_key_required: false,
  own_key_reason: "",
  limits: {
    per_hour: 20,
    used_this_hour: 3,
    rounds: 6,
    queries: 12,
    max_rows: 100,
    proposals: 6,
  },
  mutations: "review_required",
};

const run: AgentRun = {
  id: "run_1",
  project_id: "p1",
  agent_id: null,
  kind: "chat",
  question: "What is in here?",
  summary: "Mean CPU usage is 42.5% across eight hosts.",
  findings: [],
  steps: [
    {
      tool: "list_buckets",
      detail: "1 bucket",
      ok: true,
      duration_ms: 2,
      agent: null,
    },
    {
      tool: "run_query",
      detail: "Agent queries require an explicit LIMIT between 1 and 100",
      ok: false,
      duration_ms: 1,
      agent: null,
    },
  ],
  proposals: [],
  state: "ok",
  error: null,
  duration_ms: 900,
  at: Date.now(),
};

const project = {
  project: { id: "p1", name: "First project", writable: true },
  buckets: [{ id: "bkt_1", name: "metrics" }],
};

const mocks = vi.hoisted(() => ({
  agentConfig: vi.fn(),
  ask: vi.fn(),
  investigate: vi.fn(),
  savedAgents: vi.fn(),
  agentRuns: vi.fn(),
  setRetention: vi.fn(),
}));

vi.mock("../src/lib/api", async () => {
  const actual =
    await vi.importActual<typeof import("../src/lib/api")>("../src/lib/api");
  return {
    ...actual,
    api: { ...actual.api, ...mocks },
    dataClient: () => ({
      writePoints: vi.fn(async () => ({ written: 1 })),
      deletePoints: vi.fn(async () => ({ deleted: 1 })),
      flush: vi.fn(),
      compact: vi.fn(),
    }),
  };
});

// The screen reads its project from context; a minimal stand-in keeps the test
// about the agent rather than about the shell.
vi.mock("../src/app/ProjectContext", () => ({
  useProject: () => ({
    detail: project,
    reload: vi.fn(),
    targets: [
      { kind: "cloud", projectId: "p1", bucketId: "bkt_1", name: "metrics" },
    ],
    target: null,
    selectTarget: vi.fn(),
    client: null,
    targetKey: () => "k",
  }),
}));

async function open() {
  const { default: Agent } = await import("../src/app/Agent");
  const { ToastProvider } = await import("../src/lib/toast");
  return render(
    <MemoryRouter>
      <ToastProvider>
        <Agent />
      </ToastProvider>
    </MemoryRouter>,
  );
}

beforeEach(() => {
  mocks.agentConfig.mockResolvedValue(config);
  mocks.savedAgents.mockResolvedValue({ agents: [] });
  mocks.agentRuns.mockResolvedValue({ runs: [] });
  mocks.ask.mockResolvedValue({ run, model: "gemini-2.5-flash" });
  mocks.investigate.mockResolvedValue({ run, model: "gemini-2.5-flash" });
  mocks.setRetention.mockResolvedValue(undefined);
});

afterEach(() => {
  cleanup();
  vi.clearAllMocks();
});

describe("the agent screen", () => {
  it("shows what the agent did, including a refused tool", async () => {
    const user = userEvent.setup();
    const { container } = await open();

    await user.type(screen.getByLabelText("Ask the agent"), "What is in here?");
    await user.click(screen.getByRole("button", { name: /^Ask$/ }));

    await waitFor(() =>
      expect(screen.getByText(/Mean CPU usage is 42.5%/)).toBeTruthy(),
    );

    // The timeline is collapsed until asked for: it is there to be checked.
    expect(container.querySelector(".agent-steps")).toBeNull();
    await user.click(screen.getByRole("button", { name: /2 steps/ }));

    const steps = container.querySelectorAll(".agent-steps li");
    expect(steps).toHaveLength(2);
    // A refusal is shown as a refusal, not hidden.
    expect(steps[1].className).toContain("is-failed");
    expect(steps[1].textContent).toContain("explicit LIMIT");
  });

  it("reports the remaining allowance on the shared key", async () => {
    await open();
    await waitFor(() =>
      expect(
        screen.getByText(/17 of 20 investigations left this hour/),
      ).toBeTruthy(),
    );
  });

  it("tells a guest how to enable the agent instead of failing at click time", async () => {
    mocks.agentConfig.mockResolvedValue({
      ...config,
      server_key_available: false,
      own_key_required: true,
      own_key_reason:
        "The shared AI key is reserved for registered workspaces.",
    });
    await open();
    await waitFor(() =>
      expect(
        screen.getByText(/reserved for registered workspaces/),
      ).toBeTruthy(),
    );
    expect(
      screen.getByRole("button", { name: /Use your own key/ }),
    ).toBeTruthy();
  });

  it("does not apply a proposal until it is approved", async () => {
    mocks.ask.mockResolvedValue({
      model: "gemini-2.5-flash",
      run: {
        ...run,
        proposals: [
          {
            kind: "retention" as const,
            bucket_id: "bkt_1",
            bucket_name: "metrics",
            payload: { seconds: 604800 },
            explanation: "Set a 7 day retention policy on this bucket.",
            destructive: true,
          },
        ],
      },
    });
    const user = userEvent.setup();
    const { container } = await open();

    await user.type(screen.getByLabelText("Ask the agent"), "Trim old data");
    await user.click(screen.getByRole("button", { name: /^Ask$/ }));
    await waitFor(() =>
      expect(screen.getByText(/Set a 7 day retention policy/)).toBeTruthy(),
    );

    // Rendering a proposal must not have called anything.
    expect(mocks.setRetention).not.toHaveBeenCalled();
    expect(screen.getByText("Nothing has been changed yet.")).toBeTruthy();
    // The exact payload is available to read before approving.
    expect(
      container.querySelector(".agent-proposal pre")?.textContent,
    ).toContain("604800");

    // A destructive proposal opens a confirmation rather than applying.
    await user.click(screen.getByRole("button", { name: /Review and apply/ }));
    const dialog = await screen.findByRole("dialog");
    expect(mocks.setRetention).not.toHaveBeenCalled();

    // And that confirmation requires the bucket name.
    const confirm = within(dialog).getByRole("button", { name: /^Apply$/ });
    expect(confirm.hasAttribute("disabled")).toBe(true);
    await user.type(within(dialog).getByRole("textbox"), "metrics");
    await user.click(within(dialog).getByRole("button", { name: /^Apply$/ }));

    await waitFor(() =>
      expect(mocks.setRetention).toHaveBeenCalledWith("p1", "bkt_1", 604800),
    );
  });

  it("marks the destructive proposal so it cannot be mistaken for a receipt", async () => {
    mocks.ask.mockResolvedValue({
      model: "gemini-2.5-flash",
      run: {
        ...run,
        proposals: [
          {
            kind: "drop_bucket" as const,
            bucket_id: "bkt_1",
            bucket_name: "metrics",
            payload: {},
            explanation: "Delete the bucket and every point in it.",
            destructive: true,
          },
        ],
      },
    });
    const user = userEvent.setup();
    const { container } = await open();
    await user.type(screen.getByLabelText("Ask the agent"), "remove it");
    await user.click(screen.getByRole("button", { name: /^Ask$/ }));

    await waitFor(() =>
      expect(container.querySelector(".agent-proposal")).toBeTruthy(),
    );
    const card = container.querySelector(".agent-proposal")!;
    expect(card.className).toContain("is-destructive");
    expect(card.textContent).toContain("Delete bucket");
  });
});
