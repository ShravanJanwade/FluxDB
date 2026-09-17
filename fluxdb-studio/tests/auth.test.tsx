/**
 * The sign-in screen and the session provider.
 *
 * These exercise the paths a visitor actually takes on their first contact with
 * the product: the one-click demo, a rejected password, and a deployment where
 * GitHub sign-in is not configured.
 */

import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import AuthScreen from "../src/auth/AuthScreen";
import { SessionProvider } from "../src/lib/session";
import { ThemeProvider } from "../src/lib/theme";
import { ToastProvider } from "../src/lib/toast";

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

type Handler = (
  url: string,
  init?: RequestInit,
) => Response | Promise<Response>;

function json(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

const SESSION = {
  account: {
    id: "acc_1",
    email: "ada@example.com",
    name: "Ada",
    avatar_url: null,
    kind: "standard",
    has_password: true,
    created_at: 1,
    expires_at: null,
  },
  organizations: [
    {
      id: "org_1",
      name: "Ada's workspace",
      slug: "adas-workspace",
      plan: "free",
      role: "owner",
      expires_at: null,
      is_demo: false,
      projects: [
        {
          id: "proj1",
          name: "First project",
          slug: "first-project",
          description: "",
          demo: false,
          created_at: 1,
        },
      ],
    },
  ],
};

/**
 * Stub `fetch` with a routing table. Unmatched paths fail loudly rather than
 * returning something plausible, so a test cannot pass against a request it
 * did not mean to make.
 */
function stubFetch(routes: Record<string, Handler>) {
  const calls: string[] = [];
  const fetcher = vi.fn(
    async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      calls.push(`${init?.method ?? "GET"} ${url}`);
      for (const [path, handler] of Object.entries(routes)) {
        if (url.startsWith(path)) return handler(url, init);
      }
      return json({ error: `no stub for ${url}` }, 500);
    },
  );
  vi.stubGlobal("fetch", fetcher);
  return { calls };
}

function renderAuth(mode: "signin" | "signup", initialPath = "/login") {
  return render(
    <MemoryRouter initialEntries={[initialPath]}>
      <ThemeProvider>
        <ToastProvider>
          <SessionProvider>
            <Routes>
              <Route path="/login" element={<AuthScreen mode={mode} />} />
              <Route path="/signup" element={<AuthScreen mode={mode} />} />
              <Route path="/app" element={<p>Console loaded</p>} />
            </Routes>
          </SessionProvider>
        </ToastProvider>
      </ThemeProvider>
    </MemoryRouter>,
  );
}

const NO_SESSION: Handler = () => json({ error: "Sign in to continue" }, 401);

const CONFIG_WITHOUT_GITHUB: Handler = () =>
  json({
    version: "0.1.0",
    providers: { github: false },
    guest_enabled: true,
    demo_project_id: "demofluxdb99",
    control_plane: "sqlite",
    limits: {
      projects_per_org: 5,
      buckets_per_project: 8,
      points_per_project: 2_000_000,
      guest_lifetime_hours: 24,
    },
  });

describe("AuthScreen", () => {
  it("signs in and lands in the console", async () => {
    stubFetch({
      "/api/cloud/config": CONFIG_WITHOUT_GITHUB,
      "/api/cloud/auth/session": NO_SESSION,
      "/api/cloud/auth/login": (_url, init) => {
        const body = JSON.parse(String(init?.body));
        expect(body).toEqual({
          email: "ada@example.com",
          password: "a-long-enough-passphrase",
        });
        return json(SESSION);
      },
    });
    renderAuth("signin");

    await userEvent.type(
      await screen.findByLabelText(/email/i),
      "ada@example.com",
    );
    await userEvent.type(
      screen.getByLabelText(/password/i),
      "a-long-enough-passphrase",
    );
    await userEvent.click(screen.getByRole("button", { name: /^sign in$/i }));

    expect(await screen.findByText("Console loaded")).toBeTruthy();
  });

  it("shows the server's reason when a password is wrong and stays put", async () => {
    stubFetch({
      "/api/cloud/config": CONFIG_WITHOUT_GITHUB,
      "/api/cloud/auth/session": NO_SESSION,
      "/api/cloud/auth/login": () =>
        json(
          {
            error: "That email address or password is incorrect",
            code: "unauthenticated",
          },
          401,
        ),
    });
    renderAuth("signin");

    await userEvent.type(
      await screen.findByLabelText(/email/i),
      "ada@example.com",
    );
    await userEvent.type(screen.getByLabelText(/password/i), "wrong-password");
    await userEvent.click(screen.getByRole("button", { name: /^sign in$/i }));

    const alert = await screen.findByRole("alert");
    expect(alert.textContent).toContain("incorrect");
    expect(screen.queryByText("Console loaded")).toBeNull();
  });

  it("starts the demo from the ?demo=1 link without a second click", async () => {
    const { calls } = stubFetch({
      "/api/cloud/config": CONFIG_WITHOUT_GITHUB,
      "/api/cloud/auth/session": NO_SESSION,
      "/api/cloud/auth/guest": () =>
        json(
          {
            ...SESSION,
            account: { ...SESSION.account, kind: "guest" },
          },
          201,
        ),
    });
    renderAuth("signin", "/login?demo=1");

    expect(await screen.findByText("Console loaded")).toBeTruthy();
    // Exactly one workspace is created, even under React's development
    // double-invocation of effects.
    expect(calls.filter((call) => call.includes("/auth/guest"))).toHaveLength(
      1,
    );
  });

  it("disables GitHub sign-in when the deployment has no OAuth application", async () => {
    stubFetch({
      "/api/cloud/config": CONFIG_WITHOUT_GITHUB,
      "/api/cloud/auth/session": NO_SESSION,
    });
    renderAuth("signin");

    const button = await screen.findByRole("button", {
      name: /github sign-in not configured/i,
    });
    expect(button).toHaveProperty("disabled", true);
    expect(button.getAttribute("title")).toContain("GITHUB_CLIENT_ID");
  });

  it("offers GitHub sign-in as a link when it is configured", async () => {
    stubFetch({
      "/api/cloud/config": () =>
        json({
          version: "0.1.0",
          providers: { github: true },
          guest_enabled: true,
          demo_project_id: "demofluxdb99",
          control_plane: "postgres",
          limits: {
            projects_per_org: 5,
            buckets_per_project: 8,
            points_per_project: 2_000_000,
            guest_lifetime_hours: 24,
          },
        }),
      "/api/cloud/auth/session": NO_SESSION,
    });
    renderAuth("signin");

    const link = await screen.findByRole("link", {
      name: /continue with github/i,
    });
    // The return path is carried through the provider round trip.
    expect(link.getAttribute("href")).toBe(
      "/api/cloud/auth/github/start?next=%2Fapp",
    );
  });

  it("mirrors the server's password policy while typing", async () => {
    stubFetch({
      "/api/cloud/config": CONFIG_WITHOUT_GITHUB,
      "/api/cloud/auth/session": NO_SESSION,
    });
    renderAuth("signup", "/signup");

    const password = await screen.findByLabelText(/password/i);
    await userEvent.type(password, "short");
    expect(await screen.findByText(/too short/i)).toBeTruthy();

    await userEvent.clear(password);
    await userEvent.type(password, "alllowercase1");
    expect(
      await screen.findByText(/add a capital, a digit or a symbol/i),
    ).toBeTruthy();

    await userEvent.clear(password);
    await userEvent.type(password, "correct horse battery staple");
    expect(await screen.findByText(/strong/i)).toBeTruthy();
  });

  it("redirects an already signed-in visitor straight past the form", async () => {
    stubFetch({
      "/api/cloud/config": CONFIG_WITHOUT_GITHUB,
      "/api/cloud/auth/session": () => json(SESSION),
    });
    renderAuth("signin");
    await waitFor(() =>
      expect(screen.getByText("Console loaded")).toBeTruthy(),
    );
  });
});
