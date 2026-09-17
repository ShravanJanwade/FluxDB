/**
 * Colour contrast, checked against the stylesheet rather than by eye.
 *
 * Both of the failures this catches were shipped by a previous revision: white
 * button labels on the dark theme's accent fill measured 3.3:1, and the light
 * theme's success green measured 3.8:1 on white. Neither looks obviously wrong;
 * both are unreadable for some people. Parsing the tokens means a palette
 * change cannot quietly reintroduce that.
 */

import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

const WCAG_AA_TEXT = 4.5;
/**
 * A chart mark below 3:1 is a documented relief case, not a failure, provided a
 * legend and a table view exist. This is the floor at which a mark is visible
 * at all.
 */
const MARK_FLOOR = 2;

const css = readFileSync(
  join(__dirname, "..", "src", "styles", "tokens.css"),
  "utf8",
);

/** Custom properties declared inside one selector block. */
function block(selector: string): Record<string, string> {
  const start = css.indexOf(selector);
  if (start === -1) throw new Error(`selector not found: ${selector}`);
  const open = css.indexOf("{", start);
  const close = css.indexOf("\n}", open);
  const body = css.slice(open, close);
  const variables: Record<string, string> = {};
  for (const [, name, value] of body.matchAll(/(--[\w-]+)\s*:\s*([^;]+);/g)) {
    variables[name] = value.trim();
  }
  return variables;
}

const light = block(":root {");
const dark = { ...light, ...block(':root[data-theme="dark"]') };

/** Follow `var(--x)` chains to a literal colour. */
function resolve(
  value: string,
  scope: Record<string, string>,
  depth = 0,
): string {
  if (depth > 8) throw new Error(`variable cycle around ${value}`);
  const reference = value.match(/^var\((--[\w-]+)\)$/);
  if (!reference) return value;
  const next = scope[reference[1]];
  if (!next) throw new Error(`undefined variable ${reference[1]}`);
  return resolve(next, scope, depth + 1);
}

function channels(colour: string): [number, number, number] {
  const [r, g, b] = rgba(colour);
  return [r, g, b];
}

/** Red, green, blue, alpha. Alpha defaults to 1 for an opaque colour. */
function rgba(colour: string): [number, number, number, number] {
  const hex = colour.trim();
  if (hex.startsWith("#")) {
    const digits =
      hex.length === 4
        ? [...hex.slice(1)].map((d) => d + d).join("")
        : hex.slice(1, 7);
    return [
      Number.parseInt(digits.slice(0, 2), 16),
      Number.parseInt(digits.slice(2, 4), 16),
      Number.parseInt(digits.slice(4, 6), 16),
      hex.length === 9 ? Number.parseInt(hex.slice(7, 9), 16) / 255 : 1,
    ];
  }
  const numbers = hex.match(/[\d.]+/g);
  if (!numbers || numbers.length < 3)
    throw new Error(`unreadable colour ${colour}`);
  return [
    Number(numbers[0]),
    Number(numbers[1]),
    Number(numbers[2]),
    numbers.length > 3 ? Number(numbers[3]) : 1,
  ];
}

/**
 * Composite a possibly translucent colour over what sits behind it.
 *
 * Several surface and status tokens in the dark theme are `rgba(...)` tints.
 * Measuring their raw channels as if opaque reports a contrast that no reader
 * ever sees — the tint is 16% over a dark page, not the full-strength colour.
 */
function over(colour: string, backdrop: string): string {
  const [r, g, b, alpha] = rgba(colour);
  if (alpha >= 1) return colour;
  const [br, bg, bb] = channels(backdrop);
  const blend = (top: number, bottom: number) =>
    Math.round(top * alpha + bottom * (1 - alpha));
  return `rgb(${blend(r, br)}, ${blend(g, bg)}, ${blend(b, bb)})`;
}

function luminance(colour: string): number {
  const [r, g, b] = channels(colour).map((value) => {
    const channel = value / 255;
    return channel <= 0.03928
      ? channel / 12.92
      : ((channel + 0.055) / 1.055) ** 2.4;
  });
  return 0.2126 * r + 0.7152 * g + 0.0722 * b;
}

function contrast(a: string, b: string): number {
  const [hi, lo] = [luminance(a), luminance(b)].sort((x, y) => y - x);
  return (hi + 0.05) / (lo + 0.05);
}

/** Foreground tokens that carry body text on `--surface`. */
const FOREGROUNDS = [
  "--text",
  "--text-secondary",
  "--text-muted",
  "--accent-text",
  "--success",
  "--warning",
  "--danger",
  "--info",
];

describe.each([
  ["light", light],
  ["dark", dark],
])("%s theme", (name, scope) => {
  const surface = resolve(scope["--surface"], scope);

  it.each(FOREGROUNDS)("%s reads on --surface", (token) => {
    const ratio = contrast(resolve(scope[token], scope), surface);
    expect(
      ratio,
      `${token} on --surface in the ${name} theme is ${ratio.toFixed(2)}:1`,
    ).toBeGreaterThanOrEqual(WCAG_AA_TEXT);
  });

  it("white labels read on the accent fill", () => {
    // `--accent` is only ever a fill behind white text; the lighter tints used
    // as foregrounds live in `--accent-text`.
    const ratio = contrast(resolve(scope["--accent"], scope), "#ffffff");
    expect(
      ratio,
      `white on --accent in the ${name} theme is ${ratio.toFixed(2)}:1`,
    ).toBeGreaterThanOrEqual(WCAG_AA_TEXT);
  });

  it("borders are visible against their surface", () => {
    // A border is a graphical object, so the lower AA threshold applies — but
    // an invisible border makes a table unreadable regardless of the number.
    const ratio = contrast(resolve(scope["--border-strong"], scope), surface);
    expect(
      ratio,
      `--border-strong on --surface in the ${name} theme is ${ratio.toFixed(2)}:1`,
    ).toBeGreaterThanOrEqual(1.4);
  });

  it("every chart series still reads as a mark on the plot background", () => {
    // Chart marks are graphical objects, where sub-3:1 is a documented relief
    // case rather than a failure: it obliges a legend and a table view, which
    // the query workspace and the table panel kind provide. The floor asserted
    // here is that a mark is visible at all. Colour-vision separation and the
    // lightness band are checked by the palette validator, not re-derived here.
    for (let index = 1; index <= 8; index += 1) {
      const token = `--series-${index}`;
      const ratio = contrast(resolve(scope[token], scope), surface);
      expect(
        ratio,
        `${token} on --surface in the ${name} theme is ${ratio.toFixed(2)}:1`,
      ).toBeGreaterThanOrEqual(MARK_FLOOR);
    }
  });

  it("no two series slots resolve to the same colour", () => {
    const used = Array.from({ length: 8 }, (_, index) =>
      resolve(scope[`--series-${index + 1}`], scope).toLowerCase(),
    );
    expect(
      new Set(used).size,
      `duplicate series colour in the ${name} theme`,
    ).toBe(8);
  });
});

describe("token hygiene", () => {
  it("defines a dark value for every surface and ink token", () => {
    const overridden = block(':root[data-theme="dark"]');
    for (const token of [
      "--bg",
      "--surface",
      "--surface-hover",
      "--surface-active",
      "--text",
      "--text-secondary",
      "--text-muted",
      "--border",
      "--border-strong",
      "--accent",
      "--accent-text",
    ]) {
      expect(Object.keys(overridden), `${token} has no dark value`).toContain(
        token,
      );
    }
  });

  it("resolves every variable reference", () => {
    for (const scope of [light, dark]) {
      for (const [token, value] of Object.entries(scope)) {
        if (!value.startsWith("var(")) continue;
        expect(
          () => resolve(value, scope),
          `${token} -> ${value}`,
        ).not.toThrow();
      }
    }
  });
});

// ---------------------------------------------------------------------------
// Pairings declared in the component stylesheets
// ---------------------------------------------------------------------------

/**
 * The token-level checks above cannot catch a bad *pairing*: the tokens are
 * each fine, and the mistake is putting two of them together. That is exactly
 * how the agent's question bubble shipped unreadable — `--accent-text` is
 * accent-COLOURED text meant for a neutral background, and pairing it with the
 * `--accent` fill put brand-700 on brand-600.
 *
 * So this walks every rule in the component stylesheets that sets both a
 * background and a colour from tokens, resolves them, and measures.
 */
const COMPONENT_SHEETS = [
  "console.css",
  "marketing.css",
  "auth.css",
  "docs.css",
] as const;

type Pairing = {
  sheet: string;
  selector: string;
  background: string;
  color: string;
};

function pairings(): Pairing[] {
  const found: Pairing[] = [];
  for (const sheet of COMPONENT_SHEETS) {
    const text = readFileSync(
      join(__dirname, "..", "src", "styles", sheet),
      "utf8",
    );
    // Rule blocks: a selector, then declarations up to the closing brace.
    for (const [, selector, body] of text.matchAll(/([^{}]+)\{([^{}]*)\}/g)) {
      const background = body.match(
        /(?:^|[\s;])background(?:-color)?\s*:\s*var\((--[\w-]+)\)/,
      );
      const color = body.match(/(?:^|[\s;])color\s*:\s*var\((--[\w-]+)\)/);
      if (!background || !color) continue;
      found.push({
        sheet,
        selector: selector.trim().replace(/\s+/g, " "),
        background: background[1],
        color: color[1],
      });
    }
  }
  return found;
}

/**
 * Pairings whose contrast is deliberately below the text threshold, with the
 * reason. Each is decoration rather than prose a reader has to follow.
 */
const ACCEPTED_BELOW_AA = new Set<string>([
  // The agent's avatar is a bot glyph in a soft disc. WCAG asks 3:1 of a
  // graphical object rather than 4.5:1, and this measures 3.42:1 in the dark
  // theme. Raising it would mean a harder accent on the one element that should
  // recede behind the answer next to it.
  "console.css .agent-avatar {--accent on --accent-soft}",
]);

describe("foreground and background pairings", () => {
  const all = pairings();

  it("finds pairings to check in the first place", () => {
    // A regex that silently matches nothing would make this suite vacuous.
    expect(all.length).toBeGreaterThan(10);
  });

  for (const theme of ["light", "dark"] as const) {
    const scope = theme === "light" ? light : dark;

    it(`meet AA for text in the ${theme} theme`, () => {
      const failures: string[] = [];
      for (const pairing of all) {
        // A token that only exists in one theme's block is not a pairing bug.
        if (!scope[pairing.background] || !scope[pairing.color]) continue;
        let ratio: number;
        try {
          // A translucent fill shows the page through it, so both colours are
          // composited before measuring.
          const page = resolve(scope["--bg"], scope);
          const background = over(
            resolve(scope[pairing.background], scope),
            page,
          );
          const foreground = over(
            resolve(scope[pairing.color], scope),
            background,
          );
          ratio = contrast(background, foreground);
        } catch {
          // Gradients and non-colour values are out of scope here.
          continue;
        }
        // Keyed by sheet too, so an exception cannot silently cover a
        // same-named selector in another stylesheet.
        const key = `${pairing.sheet} ${pairing.selector} {${pairing.color} on ${pairing.background}}`;
        if (ratio < WCAG_AA_TEXT && !ACCEPTED_BELOW_AA.has(key)) {
          failures.push(`${key} = ${ratio.toFixed(2)}:1`);
        }
      }
      expect(failures, failures.join("\n")).toEqual([]);
    });
  }
});
