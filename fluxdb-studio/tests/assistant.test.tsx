import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import Assistant from "../src/Assistant";

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});
function setup(
  operation = "write",
  payload: object = {
    points: [{ measurement: "cpu", timestamp: "1", fields: { usage: 42 } }],
  },
) {
  const request = vi.fn(async (path: string) =>
    path.endsWith("/config")
      ? { configured: true, default_model: "gemini-test" }
      : { written: 1 },
  );
  const fetcher = vi.fn(
    async () =>
      new Response(
        JSON.stringify({
          reply: "Prepared operation for review.",
          actions: [
            {
              operation,
              database: "testdb",
              payload,
              explanation: "Requested operation",
            },
          ],
          steps: [],
          model: "gemini-test",
        }),
        { status: 200 },
      ),
  );
  vi.stubGlobal("fetch", fetcher);
  const props = {
    open: true,
    onClose: vi.fn(),
    database: "testdb",
    page: "Overview",
    serverUrl: "http://localhost:8086",
    token: "flux-token",
    online: true,
    busy: false,
    request: request as any,
    onChanged: vi.fn(async () => {}),
    onActionBusy: vi.fn(),
    onOpenQuery: vi.fn(),
    onOpenWrite: vi.fn(),
  };
  const view = render(<Assistant {...props} />);
  return { ...view, props, request, fetcher, user: userEvent.setup() };
}
async function ask(user: ReturnType<typeof userEvent.setup>) {
  await screen.findByText(/gemini-test/);
  await user.type(
    screen.getByRole("textbox", { name: "Message FluxDB assistant" }),
    "Prepare this operation",
  );
  await user.click(
    screen.getByRole("button", { name: "Send assistant message" }),
  );
  await screen.findByText("Prepared operation for review.");
}
describe("FluxDB agent execution boundaries", () => {
  it("loads provider models with the session key and selects a returned ID", async () => {
    const { user, fetcher } = setup();
    await screen.findByText(/gemini-test/);
    fetcher.mockImplementation(
      async () =>
        new Response(
          JSON.stringify({
            models: [{ id: "gemini-available", name: "Available Gemini" }],
          }),
          { status: 200 },
        ),
    );
    await user.click(
      screen.getByRole("button", { name: "Assistant settings" }),
    );
    await user.type(
      screen.getByLabelText("Gemini API key"),
      "fake-session-key",
    );
    await user.click(
      screen.getByRole("button", { name: "Check key & load available models" }),
    );
    await user.selectOptions(
      await screen.findByRole("combobox", { name: "Available models" }),
      "gemini-available",
    );
    expect(
      (
        screen.getByRole("combobox", {
          name: "Gemini model ID",
        }) as HTMLInputElement
      ).value,
    ).toBe("gemini-available");
    expect(fetcher).toHaveBeenCalledWith(
      "http://localhost:8086/api/v1/assistant/models",
      expect.objectContaining({
        headers: expect.objectContaining({
          "x-gemini-api-key": "fake-session-key",
        }),
      }),
    );
  });
  it("requires review before writing and prevents duplicate execution", async () => {
    const { user, request, props } = setup();
    await ask(user);
    const run = screen.getByRole("button", {
      name: "Confirm & run",
    }) as HTMLButtonElement;
    expect(run.disabled).toBe(true);
    expect(request).toHaveBeenCalledTimes(1);
    await user.click(
      screen.getByRole("checkbox", {
        name: "I reviewed this operation and its target.",
      }),
    );
    await user.click(run);
    await waitFor(() => expect(props.onChanged).toHaveBeenCalledTimes(1));
    expect(request).toHaveBeenLastCalledWith(
      "/api/v1/databases/testdb/points",
      expect.objectContaining({ method: "POST" }),
    );
    expect(screen.queryByRole("button", { name: "Confirm & run" })).toBeNull();
  });
  it("requires the exact database name before a destructive operation", async () => {
    const { user, request } = setup("drop_database", {});
    await ask(user);
    const run = screen.getByRole("button", {
      name: "Confirm & run",
    }) as HTMLButtonElement;
    await user.type(
      screen.getByRole("textbox", { name: "Confirm drop_database database" }),
      "wrong",
    );
    expect(run.disabled).toBe(true);
    expect(request).toHaveBeenCalledTimes(1);
    await user.clear(
      screen.getByRole("textbox", { name: "Confirm drop_database database" }),
    );
    await user.type(
      screen.getByRole("textbox", { name: "Confirm drop_database database" }),
      "testdb",
    );
    await user.click(run);
    await waitFor(() =>
      expect(request).toHaveBeenLastCalledWith(
        "/api/v1/databases/testdb",
        expect.objectContaining({ method: "DELETE" }),
      ),
    );
  });
  it("integrates query proposals with the editor and local execution", async () => {
    const query = "SELECT * FROM cpu LIMIT 10";
    const { user, request, props } = setup("query", { query });
    await ask(user);
    await user.click(screen.getByRole("button", { name: "Open in editor" }));
    expect(props.onOpenQuery).toHaveBeenCalledWith(query);
    await user.click(screen.getByRole("button", { name: "Run operation" }));
    await waitFor(() =>
      expect(request).toHaveBeenLastCalledWith(
        "/api/v1/databases/testdb/query",
        expect.objectContaining({
          method: "POST",
          body: JSON.stringify({ query }),
        }),
      ),
    );
  });
  it("keeps session keys out of storage and chat bodies", async () => {
    const { user, fetcher } = setup();
    const persist = vi.spyOn(Storage.prototype, "setItem");
    await user.click(
      screen.getByRole("button", { name: "Assistant settings" }),
    );
    await user.type(
      screen.getByLabelText("Gemini API key"),
      "fake-session-key",
    );
    await ask(user);
    const options = (
      fetcher.mock.calls[0] as unknown as [string, RequestInit]
    )[1];
    expect(options.headers).toMatchObject({
      "x-gemini-api-key": "fake-session-key",
      Authorization: "Bearer flux-token",
    });
    expect(String(options.body)).not.toContain("fake-session-key");
    expect(persist).not.toHaveBeenCalled();
    persist.mockRestore();
  });
  it("discards proposals when the selected database changes", async () => {
    const { user, rerender, props } = setup();
    await ask(user);
    rerender(<Assistant {...props} database="anotherdb" />);
    expect(screen.queryByRole("button", { name: "Confirm & run" })).toBeNull();
    expect(screen.queryByText("Prepared operation for review.")).toBeNull();
  });
  it("explains an old-server 404 instead of a generic request failure", async () => {
    const { user, fetcher } = setup();
    fetcher.mockImplementation(async () => new Response("", { status: 404 }));
    await screen.findByText(/gemini-test/);
    await user.type(
      screen.getByRole("textbox", { name: "Message FluxDB assistant" }),
      "Help with FluxDB",
    );
    await user.click(
      screen.getByRole("button", { name: "Send assistant message" }),
    );
    expect((await screen.findByRole("alert")).textContent).toContain(
      "Restart FluxDB with the updated server",
    );
  });
});
