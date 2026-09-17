import React, { lazy, Suspense } from "react";
import ReactDOM from "react-dom/client";
import {
  BrowserRouter,
  Navigate,
  Route,
  Routes,
  useLocation,
} from "react-router-dom";
import AuthScreen from "./auth/AuthScreen";
import Landing from "./marketing/Landing";
import { DirectProvider } from "./lib/direct";
import { SessionProvider, useSession } from "./lib/session";
import { applyStoredThemeEarly, ThemeProvider } from "./lib/theme";
import { ToastProvider } from "./lib/toast";
import "./styles/tokens.css";

// The console and the documentation are loaded on demand. A first-time visitor
// lands on the marketing page, and there is no reason for them to download the
// charting library or the workspace screens to read it.
const Shell = lazy(() => import("./app/Shell"));
const Docs = lazy(() => import("./app/Docs"));
const Overview = lazy(() => import("./app/Overview"));
const Buckets = lazy(() => import("./app/Buckets"));
const Explorer = lazy(() => import("./app/Explorer"));
const QueryWorkspace = lazy(() => import("./app/QueryWorkspace"));
const Dashboards = lazy(() => import("./app/Dashboards"));
const Monitors = lazy(() => import("./app/Monitors"));
const ApiKeys = lazy(() => import("./app/ApiKeys"));
const Connections = lazy(() => import("./app/Connections"));
const Health = lazy(() => import("./app/Health"));
const Members = lazy(() => import("./app/Members"));
const Activity = lazy(() => import("./app/Activity"));
const ProjectSettings = lazy(() =>
  import("./app/Settings").then((module) => ({
    default: module.ProjectSettings,
  })),
);
const AccountSettings = lazy(() =>
  import("./app/Settings").then((module) => ({
    default: module.AccountSettings,
  })),
);

/** Shown while a lazily loaded screen is in flight. */
function Loading({ label }: { label: string }) {
  return (
    <div className="shell-loading">
      <div className="loading" role="status">
        <span className="spinner" aria-hidden />
        <span>{label}</span>
      </div>
    </div>
  );
}

// Applied before the first paint so a dark-theme visitor never sees a white
// flash on a cold load.
applyStoredThemeEarly();

/** Requires a session, and remembers where the visitor was going. */
function RequireSession({ children }: { children: React.ReactNode }) {
  const { status } = useSession();
  const location = useLocation();
  if (status === "loading") {
    return <Loading label="Checking your session…" />;
  }
  if (status === "anonymous") {
    return (
      <Navigate
        to={`/login?next=${encodeURIComponent(location.pathname + location.search)}`}
        replace
      />
    );
  }
  return <>{children}</>;
}

/** `/app` has no project in it; send the visitor to one they can open. */
function AppEntry() {
  const { status, defaultProjectId } = useSession();
  if (status === "loading") {
    return <Loading label="Opening your workspace…" />;
  }
  if (!defaultProjectId) {
    return (
      <div className="shell-loading">
        <div className="card shell-error">
          <h2>No projects yet</h2>
          <p>
            Your account has no project to open. This is unusual — signing up
            creates one. Try signing out and in again, or start the demo.
          </p>
          <div className="shell-error-actions">
            <a className="btn" href="/login?demo=1">
              Open the demo
            </a>
            <a className="btn btn-primary" href="/login">
              Sign in again
            </a>
          </div>
        </div>
      </div>
    );
  }
  return <Navigate to={`/app/p/${defaultProjectId}`} replace />;
}

function NotFound() {
  return (
    <div className="shell-loading">
      <div className="card shell-error">
        <h2>That page does not exist</h2>
        <p>
          The link may be out of date, or the project may have been deleted.
        </p>
        <div className="shell-error-actions">
          <a className="btn" href="/">
            Go to the front page
          </a>
          <a className="btn btn-primary" href="/app">
            Open the console
          </a>
        </div>
      </div>
    </div>
  );
}

function App() {
  return (
    <BrowserRouter>
      <ThemeProvider>
        <ToastProvider>
          <SessionProvider>
            <DirectProvider>
              <Suspense fallback={<Loading label="Loading…" />}>
                <Routes>
                  <Route path="/" element={<Landing />} />
                  <Route path="/login" element={<AuthScreen mode="signin" />} />
                  <Route
                    path="/signup"
                    element={<AuthScreen mode="signup" />}
                  />
                  <Route path="/docs" element={<Docs />} />
                  <Route
                    path="/app"
                    element={
                      <RequireSession>
                        <AppEntry />
                      </RequireSession>
                    }
                  />
                  <Route
                    path="/app/p/:projectId"
                    element={
                      <RequireSession>
                        <Shell />
                      </RequireSession>
                    }
                  >
                    <Route index element={<Overview />} />
                    <Route path="buckets" element={<Buckets />} />
                    <Route path="explorer" element={<Explorer />} />
                    <Route path="query" element={<QueryWorkspace />} />
                    <Route path="dashboards" element={<Dashboards />} />
                    <Route path="monitors" element={<Monitors />} />
                    <Route path="keys" element={<ApiKeys />} />
                    <Route path="connections" element={<Connections />} />
                    <Route path="health" element={<Health />} />
                    <Route path="members" element={<Members />} />
                    <Route path="activity" element={<Activity />} />
                    <Route path="settings" element={<ProjectSettings />} />
                    <Route path="account" element={<AccountSettings />} />
                  </Route>
                  <Route path="*" element={<NotFound />} />
                </Routes>
              </Suspense>
            </DirectProvider>
          </SessionProvider>
        </ToastProvider>
      </ThemeProvider>
    </BrowserRouter>
  );
}

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
