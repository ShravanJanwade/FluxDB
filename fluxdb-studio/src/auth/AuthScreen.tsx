/**
 * Sign in, sign up, and the one-click demo.
 *
 * All three live on one screen because they are the same decision: how much do
 * you want to commit before looking around. The demo is given equal weight to
 * the form deliberately — a database console is worth nothing until you can see
 * data in it.
 */

import { useEffect, useRef, useState } from "react";
import {
  Link,
  useLocation,
  useNavigate,
  useSearchParams,
} from "react-router-dom";
import {
  AlertTriangle,
  ArrowRight,
  Check,
  Database,
  Github,
  KeyRound,
  Loader2,
  Play,
  ShieldCheck,
} from "lucide-react";
import { Logo } from "../components/Logo";
import { ThemeToggle } from "../components/ThemeToggle";
import { ApiError } from "../lib/api";
import { useSession } from "../lib/session";
import "../styles/auth.css";

type Mode = "signin" | "signup";

export default function AuthScreen({ mode }: { mode: Mode }) {
  const { status, config, signIn, signUp, startGuest } = useSession();
  const navigate = useNavigate();
  const location = useLocation();
  const [params] = useSearchParams();

  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [name, setName] = useState("");
  const [busy, setBusy] = useState<"form" | "guest" | null>(null);
  const [error, setError] = useState<string | null>(
    params.get("error") ?? null,
  );
  const guestRequested = useRef(false);

  const next = params.get("next") ?? "/app";

  // Already signed in: skip the screen entirely rather than showing a form
  // that would immediately redirect after submission.
  useEffect(() => {
    if (status === "authenticated") navigate(next, { replace: true });
  }, [status, navigate, next]);

  useEffect(() => {
    document.title =
      mode === "signup" ? "Create your FluxDB workspace" : "Sign in to FluxDB";
  }, [mode]);

  // `?demo=1` from the marketing page starts the demo without a second click.
  // The ref guards against a re-run under React's development double-invoke,
  // which would otherwise create two guest workspaces.
  useEffect(() => {
    if (params.get("demo") !== "1" || guestRequested.current) return;
    if (status !== "anonymous") return;
    guestRequested.current = true;
    void openDemo();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [params, status]);

  async function openDemo() {
    setBusy("guest");
    setError(null);
    try {
      await startGuest();
      navigate("/app", { replace: true });
    } catch (cause) {
      setError(
        cause instanceof ApiError
          ? cause.message
          : "The demo could not be started. Try again in a moment.",
      );
    } finally {
      setBusy(null);
    }
  }

  async function submit(event: React.FormEvent) {
    event.preventDefault();
    setBusy("form");
    setError(null);
    try {
      if (mode === "signup") {
        await signUp(email, password, name);
      } else {
        await signIn(email, password);
      }
      navigate(next, { replace: true });
    } catch (cause) {
      setError(
        cause instanceof ApiError
          ? cause.message
          : "That did not work. Check the details and try again.",
      );
    } finally {
      setBusy(null);
    }
  }

  const githubReady = config?.providers.github ?? false;
  const githubHref = `/api/cloud/auth/github/start?next=${encodeURIComponent(next)}`;

  return (
    <div className="auth">
      <div className="auth-form-side">
        <header className="auth-top">
          <Link to="/" aria-label="FluxDB home">
            <Logo />
          </Link>
          <ThemeToggle compact />
        </header>

        <main className="auth-panel">
          <h1>
            {mode === "signup" ? "Create your workspace" : "Welcome back"}
          </h1>
          <p className="auth-sub">
            {mode === "signup"
              ? "You get an organisation, a project and a bucket immediately — plus read access to the shared demo fleet."
              : "Sign in to your projects, buckets and API keys."}
          </p>

          {error && (
            <div className="auth-error" role="alert">
              <AlertTriangle size={16} aria-hidden />
              <span>{error}</span>
            </div>
          )}

          <button
            type="button"
            className="btn btn-lg auth-demo"
            onClick={openDemo}
            disabled={busy !== null}
          >
            {busy === "guest" ? (
              <Loader2 size={17} className="auth-spin" aria-hidden />
            ) : (
              <Play size={17} aria-hidden />
            )}
            {busy === "guest"
              ? "Preparing your sandbox…"
              : "Explore the demo instead"}
          </button>
          <p className="auth-demo-note">
            No email address, no password. A private sandbox you can write to,
            removed automatically after{" "}
            {config?.limits.guest_lifetime_hours ?? 24} hours.
          </p>

          <div className="auth-divider">
            <span>
              or {mode === "signup" ? "sign up" : "sign in"} with email
            </span>
          </div>

          <form onSubmit={submit} className="auth-fields">
            {mode === "signup" && (
              <label className="field">
                <span className="label">Name</span>
                <input
                  className="input"
                  type="text"
                  autoComplete="name"
                  placeholder="Ada Lovelace"
                  value={name}
                  onChange={(event) => setName(event.target.value)}
                  maxLength={64}
                />
                <span className="hint">
                  Optional. Used on your workspace and in the audit trail.
                </span>
              </label>
            )}
            <label className="field">
              <span className="label">Email</span>
              <input
                className="input"
                type="email"
                required
                autoComplete="email"
                autoFocus
                placeholder="you@example.com"
                value={email}
                onChange={(event) => setEmail(event.target.value)}
              />
            </label>
            <label className="field">
              <span className="label">Password</span>
              <input
                className="input"
                type="password"
                required
                autoComplete={
                  mode === "signup" ? "new-password" : "current-password"
                }
                placeholder={
                  mode === "signup" ? "At least 10 characters" : "Your password"
                }
                value={password}
                onChange={(event) => setPassword(event.target.value)}
              />
              {mode === "signup" && <PasswordMeter password={password} />}
            </label>
            <button
              type="submit"
              className="btn btn-primary btn-lg"
              disabled={busy !== null}
            >
              {busy === "form" ? (
                <Loader2 size={17} className="auth-spin" aria-hidden />
              ) : null}
              {mode === "signup" ? "Create workspace" : "Sign in"}
              {busy !== "form" && <ArrowRight size={16} aria-hidden />}
            </button>
          </form>

          <div className="auth-divider">
            <span>or continue with</span>
          </div>

          {githubReady ? (
            <a className="btn btn-lg auth-github" href={githubHref}>
              <Github size={17} aria-hidden /> Continue with GitHub
            </a>
          ) : (
            <button
              type="button"
              className="btn btn-lg auth-github"
              disabled
              title="This deployment has no GitHub OAuth application configured. Set GITHUB_CLIENT_ID and GITHUB_CLIENT_SECRET to enable it."
            >
              <Github size={17} aria-hidden /> GitHub sign-in not configured
              here
            </button>
          )}

          <p className="auth-switch">
            {mode === "signup" ? (
              <>
                Already have an account? <Link to="/login">Sign in</Link>
              </>
            ) : (
              <>
                No account yet? <Link to="/signup">Create one free</Link>
              </>
            )}
          </p>

          {location.pathname === "/signup" && (
            <p className="auth-fineprint">
              This deployment does not send email, so there is no verification
              step and no password reset by mail. Use the demo if you would
              rather not create an account at all.
            </p>
          )}
        </main>
      </div>

      <aside className="auth-aside" aria-label="About FluxDB">
        <div className="auth-aside-inner">
          <span className="auth-aside-eyebrow">FluxDB</span>
          <h2>
            A time-series database you can read end to end — and a console worth
            using.
          </h2>
          <ul>
            <li>
              <span>
                <Database size={16} aria-hidden />
              </span>
              <div>
                <strong>Real storage engine</strong>
                Write-ahead log, skip-list memtable, compressed typed SSTables,
                bloom filters and compaction — in Rust.
              </div>
            </li>
            <li>
              <span>
                <KeyRound size={16} aria-hidden />
              </span>
              <div>
                <strong>Projects and API keys</strong>
                Scoped, revocable keys for agents and SDKs over line protocol or
                JSON, isolated per project.
              </div>
            </li>
            <li>
              <span>
                <ShieldCheck size={16} aria-hidden />
              </span>
              <div>
                <strong>Or bring your own server</strong>
                Point this console at a FluxDB on your machine. Its token stays
                in your browser tab.
              </div>
            </li>
          </ul>
          <div className="auth-aside-quote">
            <Check size={15} aria-hidden />
            <p>
              The demo workspace ships with a seeded production fleet that has
              an incident in it. Finding the failing service with one SQL query
              takes about thirty seconds.
            </p>
          </div>
        </div>
      </aside>
    </div>
  );
}

/**
 * Mirrors the server's policy exactly — 10 characters minimum, 16 or more
 * accepted on length alone, otherwise three of four character classes — so the
 * form never encourages a password the API will reject.
 */
function PasswordMeter({ password }: { password: string }) {
  if (password.length === 0) {
    return (
      <span className="hint">
        At least 10 characters. 16 or more needs nothing else.
      </span>
    );
  }
  const length = [...password].length;
  const classes = [
    /[a-z]/.test(password),
    /[A-Z]/.test(password),
    /\d/.test(password),
    /[^\p{L}\p{N}]/u.test(password),
  ].filter(Boolean).length;

  const acceptable = length >= 16 || (length >= 10 && classes >= 3);
  const strength = acceptable ? (length >= 16 ? 3 : 2) : length >= 10 ? 1 : 0;
  const labels = [
    "Too short",
    "Add a capital, a digit or a symbol",
    "Good",
    "Strong",
  ];
  return (
    <span className={`auth-meter strength-${strength}`}>
      <span className="auth-meter-track" aria-hidden>
        <i style={{ width: `${((strength + 1) / 4) * 100}%` }} />
      </span>
      {labels[strength]}
    </span>
  );
}
