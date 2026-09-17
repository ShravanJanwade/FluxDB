/**
 * The console frame: workspace switchers, navigation, and the project context
 * every screen inside it reads from.
 *
 * Navigation is organised the way the data is — organisation, project, bucket —
 * because a console that does not match the shape of what it manages makes the
 * shape harder to learn.
 */

import { useEffect, useMemo, useState } from "react";
import {
  Link,
  NavLink,
  Outlet,
  useLocation,
  useNavigate,
  useParams,
} from "react-router-dom";
import {
  Activity,
  BarChart3,
  Bell,
  Bot,
  BookOpen,
  Building2,
  ChevronRight,
  Database,
  FolderKanban,
  Gauge,
  KeyRound,
  LayoutDashboard,
  LogOut,
  Menu as MenuIcon,
  PanelsTopLeft,
  Plus,
  Search,
  Server,
  Settings,
  Sparkles,
  Table2,
  Terminal,
  Users,
  X,
} from "lucide-react";
import { Logo, LogoGlyph } from "../components/Logo";
import { ThemeToggle } from "../components/ThemeToggle";
import { Menu, Modal, Spinner, useLoader } from "../components/ui";
import { api } from "../lib/api";
import { initials, relative } from "../lib/format";
import { useSession } from "../lib/session";
import { useToast } from "../lib/toast";
import type { OrgSummary } from "../lib/types";
import { ProjectProvider } from "./ProjectContext";
import { AssistantLauncher } from "./AssistantPanel";
import { CommandPalette } from "./CommandPalette";
import "../styles/console.css";

export default function Shell() {
  const { projectId } = useParams<{ projectId: string }>();
  const { session, organizations, signOut, findProject, refresh } =
    useSession();
  const navigate = useNavigate();
  const location = useLocation();
  const toast = useToast();
  const [navOpen, setNavOpen] = useState(false);
  const [paletteOpen, setPaletteOpen] = useState(false);
  const [creating, setCreating] = useState(false);

  const detail = useLoader(
    (signal) =>
      projectId
        ? api.project(projectId, signal)
        : Promise.reject(new Error("No project selected")),
    [projectId],
  );

  // Close the mobile drawer whenever the route changes, so a tap on a link does
  // not leave the overlay covering the page it just opened.
  useEffect(() => setNavOpen(false), [location.pathname]);

  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "k") {
        event.preventDefault();
        setPaletteOpen(true);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  const placement = projectId ? findProject(projectId) : null;
  const org = placement?.org ?? null;

  if (detail.loading && !detail.data) {
    return (
      <div className="shell-loading">
        <Spinner label="Opening your workspace…" />
      </div>
    );
  }

  if (detail.error || !detail.data) {
    return (
      <div className="shell-loading">
        <div className="card shell-error">
          <h2>That project could not be opened</h2>
          <p>
            {detail.error ??
              "It may have been deleted, or you may no longer be a member."}
          </p>
          <div className="shell-error-actions">
            <button type="button" className="btn" onClick={detail.reload}>
              Try again
            </button>
            <Link className="btn btn-primary" to="/app">
              Back to your workspaces
            </Link>
          </div>
        </div>
      </div>
    );
  }

  const project = detail.data.project;
  const root = `/app/p/${project.id}`;
  const guest = session?.account.kind === "guest";

  const sections: {
    label: string;
    items: {
      to: string;
      label: string;
      Icon: typeof Database;
      end?: boolean;
      /** Warms this screen's chunk on hover, so the click does not wait. */
      load?: () => Promise<unknown>;
    }[];
  }[] = [
    {
      label: "Project",
      items: [
        {
          to: root,
          label: "Overview",
          Icon: LayoutDashboard,
          end: true,
          load: () => import("./Overview"),
        },
        {
          to: `${root}/buckets`,
          label: "Buckets",
          Icon: Database,
          load: () => import("./Buckets"),
        },
        {
          to: `${root}/explorer`,
          label: "Data explorer",
          Icon: Table2,
          load: () => import("./Explorer"),
        },
        {
          to: `${root}/query`,
          label: "Query workspace",
          Icon: Terminal,
          load: () => import("./QueryWorkspace"),
        },
        {
          to: `${root}/dashboards`,
          label: "Dashboards",
          Icon: BarChart3,
          load: () => import("./Dashboards"),
        },
        {
          to: `${root}/monitors`,
          label: "Monitors & alerts",
          Icon: Bell,
          load: () => import("./Monitors"),
        },
        {
          to: `${root}/agent`,
          label: "AI agent",
          Icon: Bot,
          load: () => import("./Agent"),
        },
      ],
    },
    {
      label: "Connect",
      items: [
        {
          to: `${root}/keys`,
          label: "API keys",
          Icon: KeyRound,
          load: () => import("./ApiKeys"),
        },
        {
          to: `${root}/connections`,
          label: "Your own servers",
          Icon: Server,
          load: () => import("./Connections"),
        },
        {
          to: `${root}/health`,
          label: "Instance health",
          Icon: Gauge,
          load: () => import("./Health"),
        },
      ],
    },
    {
      label: "Workspace",
      items: [
        {
          to: `${root}/members`,
          label: "Members",
          Icon: Users,
          load: () => import("./Members"),
        },
        {
          to: `${root}/activity`,
          label: "Activity",
          Icon: Activity,
          load: () => import("./Activity"),
        },
        {
          to: `${root}/settings`,
          label: "Settings",
          Icon: Settings,
          load: () => import("./Settings"),
        },
      ],
    },
  ];

  return (
    <ProjectProvider detail={detail.data} reload={detail.reload}>
      <div className={`shell${navOpen ? " nav-open" : ""}`}>
        <aside className="sidebar" aria-label="Workspace navigation">
          <div className="sidebar-top">
            <Link to="/" className="sidebar-brand" aria-label="FluxDB home">
              <Logo size={24} />
            </Link>
            <button
              type="button"
              className="btn btn-ghost btn-icon sidebar-close"
              onClick={() => setNavOpen(false)}
              aria-label="Close navigation"
            >
              <X size={16} aria-hidden />
            </button>
          </div>

          <div className="switchers">
            <OrgSwitcher
              organizations={organizations}
              current={org}
              onCreateProject={() => setCreating(true)}
            />
            <ProjectSwitcher
              org={org}
              currentProjectId={project.id}
              onCreateProject={() => setCreating(true)}
            />
          </div>

          <nav className="nav">
            {sections.map((section) => (
              <div key={section.label} className="nav-section">
                <span className="nav-label">{section.label}</span>
                {section.items.map((item) => (
                  <NavLink
                    key={item.to}
                    to={item.to}
                    end={item.end}
                    className={({ isActive }) =>
                      `nav-item${isActive ? " is-active" : ""}`
                    }
                    onMouseEnter={item.load}
                    onFocus={item.load}
                    onPointerDown={item.load}
                  >
                    <item.Icon size={16} aria-hidden />
                    {item.label}
                  </NavLink>
                ))}
              </div>
            ))}
            <div className="nav-section">
              <span className="nav-label">Learn</span>
              <NavLink
                to="/docs"
                className={({ isActive }) =>
                  `nav-item${isActive ? " is-active" : ""}`
                }
                onMouseEnter={() => void import("./Docs")}
                onFocus={() => void import("./Docs")}
              >
                <BookOpen size={16} aria-hidden />
                Developer resources
              </NavLink>
            </div>
          </nav>

          <div className="sidebar-foot">
            <Menu
              label="Account menu"
              align="start"
              trigger={
                <span className="account-trigger">
                  <span className="avatar" aria-hidden>
                    {session?.account.avatar_url ? (
                      <img src={session.account.avatar_url} alt="" />
                    ) : (
                      initials(
                        session?.account.name ?? session?.account.email ?? "?",
                      )
                    )}
                  </span>
                  <span className="account-text">
                    <strong>{session?.account.name}</strong>
                    <small>
                      {guest ? "Guest session" : session?.account.email}
                    </small>
                  </span>
                </span>
              }
            >
              {(close) => (
                <>
                  <div className="menu-heading">
                    {session?.account.email}
                    {guest && session?.account.expires_at && (
                      <small>
                        Sandbox removed {relative(session.account.expires_at)}
                      </small>
                    )}
                  </div>
                  <Link
                    className="menu-item"
                    to={`${root}/account`}
                    onClick={close}
                  >
                    <Settings size={15} aria-hidden /> Account settings
                  </Link>
                  <div className="menu-row">
                    <span>Theme</span>
                    <ThemeToggle compact />
                  </div>
                  <button
                    type="button"
                    className="menu-item"
                    onClick={async () => {
                      close();
                      await signOut();
                      navigate("/", { replace: true });
                    }}
                  >
                    <LogOut size={15} aria-hidden /> Sign out
                  </button>
                </>
              )}
            </Menu>
          </div>
        </aside>

        <div
          className="nav-scrim"
          onClick={() => setNavOpen(false)}
          aria-hidden
        />

        <div className="main">
          <header className="topbar">
            <button
              type="button"
              className="btn btn-ghost btn-icon topbar-menu"
              onClick={() => setNavOpen(true)}
              aria-label="Open navigation"
            >
              <MenuIcon size={18} aria-hidden />
            </button>
            <nav className="crumbs" aria-label="Breadcrumb">
              <span>{detail.data.org.name}</span>
              <ChevronRight size={14} aria-hidden />
              <strong>{project.name}</strong>
              {project.demo && (
                <span className="badge badge-accent">Demo · read-only</span>
              )}
              {project.role === "viewer" && !project.demo && (
                <span className="badge">Viewer</span>
              )}
            </nav>
            <div className="topbar-actions">
              <button
                type="button"
                className="palette-trigger"
                onClick={() => setPaletteOpen(true)}
              >
                <Search size={14} aria-hidden />
                <span>Search or jump to…</span>
                <kbd>{navigator.platform.includes("Mac") ? "⌘" : "Ctrl"} K</kbd>
              </button>
              <AssistantLauncher
                page={location.pathname.split("/").pop() ?? "overview"}
              />
              <Link className="btn btn-sm" to={`${root}/query`}>
                <Sparkles size={15} aria-hidden /> Run a query
              </Link>
            </div>
          </header>

          {guest && (
            <GuestBanner expiresAt={session?.account.expires_at ?? null} />
          )}

          <main className="content" id="content">
            <Outlet />
          </main>
        </div>

        {paletteOpen && (
          <CommandPalette
            projectRoot={root}
            organizations={organizations}
            onClose={() => setPaletteOpen(false)}
          />
        )}

        {creating && org && (
          <CreateProjectDialog
            org={org}
            onClose={() => setCreating(false)}
            onCreated={async (id) => {
              setCreating(false);
              await refresh();
              toast.success("Project created");
              navigate(`/app/p/${id}`);
            }}
          />
        )}
      </div>
    </ProjectProvider>
  );
}

function OrgSwitcher({
  organizations,
  current,
  onCreateProject,
}: {
  organizations: OrgSummary[];
  current: OrgSummary | null;
  onCreateProject: () => void;
}) {
  return (
    <Menu
      label="Switch workspace"
      trigger={
        <span className="switcher-trigger">
          <span className="switcher-icon">
            <Building2 size={14} aria-hidden />
          </span>
          <span className="switcher-text">
            <small>Workspace</small>
            <strong>{current?.name ?? "Select a workspace"}</strong>
          </span>
        </span>
      }
    >
      {(close) => (
        <>
          <div className="menu-heading">Your workspaces</div>
          {organizations.map((org) => (
            <Link
              key={org.id}
              className={`menu-item${org.id === current?.id ? " is-current" : ""}`}
              to={
                org.projects[0]
                  ? `/app/p/${org.projects[0].id}`
                  : `/app/p/${current?.projects[0]?.id ?? ""}`
              }
              onClick={close}
            >
              <Building2 size={15} aria-hidden />
              <span className="menu-item-text">
                {org.name}
                <small>
                  {org.is_demo
                    ? "Shared showcase · read-only"
                    : `${org.role} · ${org.projects.length} project${org.projects.length === 1 ? "" : "s"}`}
                </small>
              </span>
            </Link>
          ))}
          <div className="menu-separator" />
          <button
            type="button"
            className="menu-item"
            onClick={() => {
              close();
              onCreateProject();
            }}
          >
            <Plus size={15} aria-hidden /> New project
          </button>
        </>
      )}
    </Menu>
  );
}

function ProjectSwitcher({
  org,
  currentProjectId,
  onCreateProject,
}: {
  org: OrgSummary | null;
  currentProjectId: string;
  onCreateProject: () => void;
}) {
  const current = org?.projects.find(
    (project) => project.id === currentProjectId,
  );
  return (
    <Menu
      label="Switch project"
      trigger={
        <span className="switcher-trigger">
          <span className="switcher-icon switcher-icon-accent">
            <FolderKanban size={14} aria-hidden />
          </span>
          <span className="switcher-text">
            <small>Project</small>
            <strong>{current?.name ?? "Project"}</strong>
          </span>
        </span>
      }
    >
      {(close) => (
        <>
          <div className="menu-heading">
            Projects in {org?.name ?? "this workspace"}
          </div>
          {(org?.projects ?? []).map((project) => (
            <Link
              key={project.id}
              className={`menu-item${project.id === currentProjectId ? " is-current" : ""}`}
              to={`/app/p/${project.id}`}
              onClick={close}
            >
              <PanelsTopLeft size={15} aria-hidden />
              <span className="menu-item-text">
                {project.name}
                {project.demo && <small>Read-only showcase</small>}
              </span>
            </Link>
          ))}
          {!org?.is_demo && (
            <>
              <div className="menu-separator" />
              <button
                type="button"
                className="menu-item"
                onClick={() => {
                  close();
                  onCreateProject();
                }}
              >
                <Plus size={15} aria-hidden /> New project
              </button>
            </>
          )}
        </>
      )}
    </Menu>
  );
}

function GuestBanner({ expiresAt }: { expiresAt: number | null }) {
  return (
    <div className="guest-banner">
      <LogoGlyph size={18} />
      <span>
        You are exploring as a guest. Your sandbox and everything in it is
        removed {expiresAt ? relative(expiresAt) : "within 24 hours"}.
      </span>
      <Link className="btn btn-sm btn-primary" to="/signup">
        Keep this work — create an account
      </Link>
    </div>
  );
}

function CreateProjectDialog({
  org,
  onClose,
  onCreated,
}: {
  org: OrgSummary;
  onClose: () => void;
  onCreated: (projectId: string) => void | Promise<void>;
}) {
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const targetOrg = useMemo(() => org, [org]);

  async function submit(event: React.FormEvent) {
    event.preventDefault();
    setBusy(true);
    setError(null);
    try {
      const project = await api.createProject(targetOrg.id, name, description);
      await onCreated(project.id);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "That did not work");
    } finally {
      setBusy(false);
    }
  }

  return (
    <Modal
      title="New project"
      description={`In ${targetOrg.name}. A project owns its own buckets, API keys, dashboards and monitors.`}
      onClose={onClose}
      footer={
        <>
          <button
            type="button"
            className="btn"
            onClick={onClose}
            disabled={busy}
          >
            Cancel
          </button>
          <button
            type="submit"
            form="create-project"
            className="btn btn-primary"
            disabled={busy || name.trim().length === 0}
          >
            Create project
          </button>
        </>
      }
    >
      <form id="create-project" onSubmit={submit} className="stack">
        {error && <div className="form-error">{error}</div>}
        <label className="field">
          <span className="label">Name</span>
          <input
            className="input"
            value={name}
            onChange={(event) => setName(event.target.value)}
            placeholder="Production observability"
            maxLength={64}
            required
          />
        </label>
        <label className="field">
          <span className="label">Description</span>
          <textarea
            className="input"
            rows={2}
            value={description}
            onChange={(event) => setDescription(event.target.value)}
            placeholder="What this project holds, and who looks after it."
            maxLength={280}
          />
          <span className="hint">Optional. Up to 280 characters.</span>
        </label>
      </form>
    </Modal>
  );
}
