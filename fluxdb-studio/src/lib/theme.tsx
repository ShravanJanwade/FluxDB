/** Light, dark, or whatever the operating system says. */

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";

export type ThemePreference = "light" | "dark" | "system";
export type Appearance = "light" | "dark";

const STORAGE_KEY = "fluxdb.theme";

type ThemeContextValue = {
  preference: ThemePreference;
  appearance: Appearance;
  setPreference: (preference: ThemePreference) => void;
  toggle: () => void;
};

const ThemeContext = createContext<ThemeContextValue | null>(null);

function storedPreference(): ThemePreference {
  try {
    const value = localStorage.getItem(STORAGE_KEY);
    if (value === "light" || value === "dark" || value === "system")
      return value;
  } catch {
    // Private browsing can refuse storage; the default is still usable.
  }
  return "system";
}

function systemAppearance(): Appearance {
  return typeof window !== "undefined" &&
    window.matchMedia?.("(prefers-color-scheme: dark)").matches
    ? "dark"
    : "light";
}

export function ThemeProvider({ children }: { children: ReactNode }) {
  const [preference, setStoredPreference] =
    useState<ThemePreference>(storedPreference);
  const [system, setSystem] = useState<Appearance>(systemAppearance);

  useEffect(() => {
    // Absent in some embedded webviews, and in jsdom. Without it the stored
    // preference still applies; only "follow the system" stops updating live.
    const query = window.matchMedia?.("(prefers-color-scheme: dark)");
    if (!query?.addEventListener) return;
    const listener = (event: MediaQueryListEvent) =>
      setSystem(event.matches ? "dark" : "light");
    query.addEventListener("change", listener);
    return () => query.removeEventListener("change", listener);
  }, []);

  const appearance: Appearance = preference === "system" ? system : preference;

  useEffect(() => {
    document.documentElement.dataset.theme = appearance;
    // Keep the browser chrome in step with the page on mobile.
    document
      .querySelector('meta[name="theme-color"]')
      ?.setAttribute("content", appearance === "dark" ? "#0a0c12" : "#ffffff");
  }, [appearance]);

  const setPreference = useCallback((next: ThemePreference) => {
    setStoredPreference(next);
    try {
      localStorage.setItem(STORAGE_KEY, next);
    } catch {
      // A preference that cannot be persisted still applies for this tab.
    }
  }, []);

  const value = useMemo(
    () => ({
      preference,
      appearance,
      setPreference,
      toggle: () => setPreference(appearance === "dark" ? "light" : "dark"),
    }),
    [preference, appearance, setPreference],
  );

  return (
    <ThemeContext.Provider value={value}>{children}</ThemeContext.Provider>
  );
}

export function useTheme(): ThemeContextValue {
  const context = useContext(ThemeContext);
  if (!context) throw new Error("useTheme must be used inside ThemeProvider");
  return context;
}

/** Apply the stored preference before React mounts, so there is no flash of the
 *  wrong theme on a cold load. */
export function applyStoredThemeEarly() {
  const preference = storedPreference();
  document.documentElement.dataset.theme =
    preference === "system" ? systemAppearance() : preference;
}
