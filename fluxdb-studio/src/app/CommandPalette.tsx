/** Keyboard navigation for everything in the console. */

import { useEffect, useMemo, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";
import {
  Activity,
  BarChart3,
  Bell,
  BookOpen,
  CornerDownLeft,
  Database,
  FolderKanban,
  Gauge,
  KeyRound,
  LayoutDashboard,
  Search,
  Server,
  Settings,
  Table2,
  Terminal,
  Users,
} from "lucide-react";
import type { OrgSummary } from "../lib/types";

type Command = {
  id: string;
  label: string;
  group: string;
  hint?: string;
  to: string;
  Icon: typeof Database;
};

export function CommandPalette({
  projectRoot,
  organizations,
  onClose,
}: {
  projectRoot: string;
  organizations: OrgSummary[];
  onClose: () => void;
}) {
  const navigate = useNavigate();
  const [term, setTerm] = useState("");
  const [cursor, setCursor] = useState(0);
  const input = useRef<HTMLInputElement>(null);
  const list = useRef<HTMLDivElement>(null);

  const commands = useMemo<Command[]>(() => {
    const pages: Command[] = [
      {
        id: "overview",
        label: "Overview",
        group: "Go to",
        to: projectRoot,
        Icon: LayoutDashboard,
      },
      {
        id: "buckets",
        label: "Buckets",
        group: "Go to",
        to: `${projectRoot}/buckets`,
        Icon: Database,
      },
      {
        id: "explorer",
        label: "Data explorer",
        group: "Go to",
        to: `${projectRoot}/explorer`,
        Icon: Table2,
      },
      {
        id: "query",
        label: "Query workspace",
        group: "Go to",
        to: `${projectRoot}/query`,
        Icon: Terminal,
      },
      {
        id: "dashboards",
        label: "Dashboards",
        group: "Go to",
        to: `${projectRoot}/dashboards`,
        Icon: BarChart3,
      },
      {
        id: "monitors",
        label: "Monitors & alerts",
        group: "Go to",
        to: `${projectRoot}/monitors`,
        Icon: Bell,
      },
      {
        id: "keys",
        label: "API keys",
        group: "Go to",
        to: `${projectRoot}/keys`,
        Icon: KeyRound,
      },
      {
        id: "connections",
        label: "Your own servers",
        group: "Go to",
        to: `${projectRoot}/connections`,
        Icon: Server,
      },
      {
        id: "health",
        label: "Instance health",
        group: "Go to",
        to: `${projectRoot}/health`,
        Icon: Gauge,
      },
      {
        id: "members",
        label: "Members",
        group: "Go to",
        to: `${projectRoot}/members`,
        Icon: Users,
      },
      {
        id: "activity",
        label: "Activity",
        group: "Go to",
        to: `${projectRoot}/activity`,
        Icon: Activity,
      },
      {
        id: "settings",
        label: "Project settings",
        group: "Go to",
        to: `${projectRoot}/settings`,
        Icon: Settings,
      },
      {
        id: "docs",
        label: "Developer resources",
        group: "Go to",
        to: "/docs",
        Icon: BookOpen,
      },
    ];
    const projects: Command[] = organizations.flatMap((org) =>
      org.projects.map((project) => ({
        id: `project-${project.id}`,
        label: project.name,
        group: "Open project",
        hint: org.is_demo ? `${org.name} · read-only` : org.name,
        to: `/app/p/${project.id}`,
        Icon: FolderKanban,
      })),
    );
    return [...pages, ...projects];
  }, [projectRoot, organizations]);

  // Subsequence match, so "dexp" finds "Data explorer" the way an editor's
  // jump-to-file does.
  const matches = useMemo(() => {
    const needle = term.trim().toLowerCase();
    if (!needle) return commands;
    return commands.filter((command) => {
      const haystack =
        `${command.label} ${command.hint ?? ""} ${command.group}`.toLowerCase();
      let index = 0;
      for (const character of needle) {
        if (character === " ") continue;
        index = haystack.indexOf(character, index);
        if (index === -1) return false;
        index += 1;
      }
      return true;
    });
  }, [commands, term]);

  useEffect(() => setCursor(0), [term]);
  useEffect(() => input.current?.focus(), []);

  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        onClose();
      } else if (event.key === "ArrowDown") {
        event.preventDefault();
        setCursor((current) => Math.min(matches.length - 1, current + 1));
      } else if (event.key === "ArrowUp") {
        event.preventDefault();
        setCursor((current) => Math.max(0, current - 1));
      } else if (event.key === "Enter" && matches[cursor]) {
        event.preventDefault();
        navigate(matches[cursor].to);
        onClose();
      }
    };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [matches, cursor, navigate, onClose]);

  useEffect(() => {
    list.current
      ?.querySelector<HTMLElement>(`[data-index="${cursor}"]`)
      ?.scrollIntoView({ block: "nearest" });
  }, [cursor]);

  let lastGroup = "";

  return (
    <div
      className="palette-scrim"
      onMouseDown={(event) => {
        if (event.target === event.currentTarget) onClose();
      }}
    >
      <div
        className="palette"
        role="dialog"
        aria-modal="true"
        aria-label="Command palette"
      >
        <div className="palette-input">
          <Search size={16} aria-hidden />
          <input
            ref={input}
            value={term}
            onChange={(event) => setTerm(event.target.value)}
            placeholder="Search pages and projects…"
            aria-label="Search"
            autoComplete="off"
            spellCheck={false}
          />
          <kbd>Esc</kbd>
        </div>
        <div className="palette-list" ref={list} role="listbox">
          {matches.length === 0 && (
            <p className="palette-empty">Nothing matches “{term}”.</p>
          )}
          {matches.map((command, index) => {
            const header = command.group !== lastGroup ? command.group : null;
            lastGroup = command.group;
            return (
              <div key={command.id}>
                {header && <div className="palette-group">{header}</div>}
                <button
                  type="button"
                  role="option"
                  aria-selected={index === cursor}
                  data-index={index}
                  className={`palette-item${index === cursor ? " is-active" : ""}`}
                  onMouseEnter={() => setCursor(index)}
                  onClick={() => {
                    navigate(command.to);
                    onClose();
                  }}
                >
                  <command.Icon size={15} aria-hidden />
                  <span>{command.label}</span>
                  {command.hint && <small>{command.hint}</small>}
                  {index === cursor && (
                    <CornerDownLeft
                      size={13}
                      aria-hidden
                      className="palette-enter"
                    />
                  )}
                </button>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}
