import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { ThemeProvider } from "../src/lib/theme";
import Landing from "../src/marketing/Landing";

const publicStats = vi.fn();
vi.mock("../src/lib/api", () => ({
  api: { publicStats: () => publicStats() },
}));

afterEach(() => {
  cleanup();
  publicStats.mockReset();
});

function landing() {
  return render(
    <MemoryRouter>
      <ThemeProvider>
        <Landing />
      </ThemeProvider>
    </MemoryRouter>,
  );
}

describe("the hero", () => {
  it("leads with the one-click demo, then a free workspace", () => {
    publicStats.mockRejectedValue(new Error("offline"));
    const { container } = landing();
    const ctas = [
      ...container.querySelectorAll<HTMLAnchorElement>(".m-hero .m-cta a"),
    ];

    expect(ctas.map((a) => a.getAttribute("href"))).toEqual([
      "/login?demo=1",
      "/signup",
    ]);
    expect(ctas[0].className).toContain("btn-primary");
  });

  it("quotes this instance's own figures, and nothing when it can't reach them", async () => {
    publicStats.mockResolvedValue({
      version: "1.4.0",
      uptime_seconds: 7200,
      demo_points: 1250000,
      demo_sstables: 12,
      requests_recorded: 1800,
      query_p50_ms: 1.2,
      query_p95_ms: 3.4,
      query_p99_ms: 8.9,
    });
    landing();

    expect(await screen.findByText("Live from this instance")).toBeTruthy();
    expect(screen.getByText(/ms API p95/)).toBeTruthy();
    expect(screen.getByText("engine v1.4.0")).toBeTruthy();

    cleanup();
    publicStats.mockRejectedValue(new Error("offline"));
    const { container } = landing();
    await Promise.resolve();
    expect(container.querySelector(".m-live")).toBeNull();
  });
});

describe("the use cases section", () => {
  it("links each sector to its live app", () => {
    publicStats.mockRejectedValue(new Error("offline"));
    const { container } = landing();
    const links = [
      ...container.querySelectorAll<HTMLAnchorElement>("#use-cases a.btn"),
    ].map((link) => link.getAttribute("href"));

    expect(links).toEqual([
      "https://fluxdb-use-cases.vercel.app/energy",
      "https://fluxdb-use-cases.vercel.app/healthcare",
      "https://fluxdb-use-cases.vercel.app/markets",
    ]);
  });
});

describe("the limits", () => {
  // The page states what FluxDB is not as plainly as what it is.
  it("are on the front page", () => {
    publicStats.mockRejectedValue(new Error("offline"));
    const { container } = landing();
    const titles = [...container.querySelectorAll("#limits h3")].map(
      (h) => h.textContent,
    );

    expect(titles).toEqual([
      "Single node",
      "Memory-bound reads",
      "Not InfluxDB",
      "Demo sleeps",
    ]);
  });
});

describe("the quickstart", () => {
  it("keeps a space between a comment and the command that follows", () => {
    publicStats.mockRejectedValue(new Error("offline"));
    const { container } = landing();
    const code = container.querySelector("#quickstart pre")?.textContent ?? "";

    expect(code).toContain(
      "# Write a point with a project API key from the console.\ncurl -X POST",
    );
    expect(
      container.querySelectorAll("#quickstart .is-comment").length,
    ).toBeGreaterThan(0);
  });
});
