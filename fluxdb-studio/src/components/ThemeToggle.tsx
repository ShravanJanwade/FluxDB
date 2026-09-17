import { Monitor, Moon, Sun } from "lucide-react";
import { useTheme, type ThemePreference } from "../lib/theme";

const OPTIONS: { value: ThemePreference; label: string; Icon: typeof Sun }[] = [
  { value: "light", label: "Light", Icon: Sun },
  { value: "dark", label: "Dark", Icon: Moon },
  { value: "system", label: "Match system", Icon: Monitor },
];

/** Three-state control rather than a switch, so "follow the system" stays a
 *  choice the visitor can return to. */
export function ThemeToggle({ compact = false }: { compact?: boolean }) {
  const { preference, setPreference } = useTheme();
  return (
    <div
      className={`theme-toggle${compact ? " theme-toggle-compact" : ""}`}
      role="radiogroup"
      aria-label="Colour theme"
    >
      {OPTIONS.map(({ value, label, Icon }) => (
        <button
          key={value}
          type="button"
          role="radio"
          aria-checked={preference === value}
          aria-label={label}
          title={label}
          className={preference === value ? "is-selected" : undefined}
          onClick={() => setPreference(value)}
        >
          <Icon size={14} aria-hidden />
        </button>
      ))}
    </div>
  );
}
