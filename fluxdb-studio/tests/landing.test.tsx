import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { ThemeProvider } from "../src/lib/theme";
import Landing from "../src/marketing/Landing";

afterEach(cleanup);

function landing() {
  return render(
    <MemoryRouter>
      <ThemeProvider>
        <Landing />
      </ThemeProvider>
    </MemoryRouter>,
  );
}

describe("the console tour checklist", () => {
  // `.m-checklist li` is a two-column grid, [icon][text]. A grid container
  // blockifies every child, so loose text nodes and inline <code>/<kbd> each
  // became their own grid item and flowed through the icon column — the list
  // rendered with its text stepping in and out of alignment and the shortcut
  // keys stacked vertically.
  it("gives every item exactly one icon and one text wrapper", () => {
    const { container } = landing();
    const items = [...container.querySelectorAll(".m-checklist li")];

    expect(items.length).toBeGreaterThan(0);
    for (const item of items) {
      expect(item.children).toHaveLength(2);
      expect(item.children[0].tagName).toBe("svg");
      expect(item.children[1].tagName).toBe("SPAN");
      // Nothing may sit between them as a bare text node.
      for (const node of item.childNodes) {
        if (node.nodeType === Node.TEXT_NODE) {
          expect(node.textContent?.trim()).toBe("");
        }
      }
    }
  });

  // JSX drops whitespace that spans a newline, so `carry\n<code>` rendered as
  // "carry$timeFilter" with the words run together.
  it("keeps a space before an inline code chip", () => {
    const { container } = landing();
    const text = [...container.querySelectorAll(".m-checklist li")]
      .map((item) => item.textContent ?? "")
      .join(" ");

    expect(text).toContain("carry $timeFilter");
    expect(text).toContain("$interval");
    expect(text).not.toMatch(/\w\$time/);
  });

  it("groups the command palette shortcut so its keys stay together", () => {
    const { container } = landing();
    const keys = container.querySelector(".m-keys");

    expect(keys).not.toBeNull();
    expect([...keys!.querySelectorAll("kbd")].map((k) => k.textContent)).toEqual(
      ["Ctrl", "⌘", "K"],
    );
  });
});
