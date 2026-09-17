/**
 * Who is signed in, and what they can open.
 *
 * The session is resolved once on load from the `HttpOnly` cookie. The token
 * itself is never visible to JavaScript, so "am I signed in" is a question only
 * the server can answer; this provider caches that answer and the workspace
 * tree that comes with it.
 */

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";
import { ApiError, api } from "./api";
import type {
  DeploymentConfig,
  OrgSummary,
  ProjectSummary,
  Session,
} from "./types";

type Status = "loading" | "anonymous" | "authenticated";

type SessionContextValue = {
  status: Status;
  session: Session | null;
  config: DeploymentConfig | null;
  /** Non-fatal problem reaching the control plane, worth showing once. */
  error: string | null;
  organizations: OrgSummary[];
  refresh: () => Promise<void>;
  signIn: (email: string, password: string) => Promise<Session>;
  signUp: (email: string, password: string, name: string) => Promise<Session>;
  startGuest: () => Promise<Session>;
  signOut: () => Promise<void>;
  findProject: (
    projectId: string,
  ) => { org: OrgSummary; project: ProjectSummary } | null;
  /** First project the visitor can open, used to land them somewhere useful. */
  defaultProjectId: string | null;
};

const SessionContext = createContext<SessionContextValue | null>(null);

export function SessionProvider({ children }: { children: ReactNode }) {
  const [status, setStatus] = useState<Status>("loading");
  const [session, setSession] = useState<Session | null>(null);
  const [config, setConfig] = useState<DeploymentConfig | null>(null);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async (signal?: AbortSignal) => {
    try {
      const current = await api.session(signal);
      setSession(current);
      setStatus("authenticated");
      setError(null);
    } catch (cause) {
      if (cause instanceof DOMException && cause.name === "AbortError") return;
      setSession(null);
      setStatus("anonymous");
      // A 401 is the normal signed-out case and is not worth reporting; a
      // network or server failure is.
      setError(
        cause instanceof ApiError && !cause.unauthenticated
          ? cause.message
          : null,
      );
    }
  }, []);

  useEffect(() => {
    const controller = new AbortController();
    void load(controller.signal);
    api
      .config()
      .then(setConfig)
      .catch(() => setConfig(null));
    return () => controller.abort();
  }, [load]);

  const adopt = useCallback((next: Session) => {
    setSession(next);
    setStatus("authenticated");
    setError(null);
    return next;
  }, []);

  const value = useMemo<SessionContextValue>(() => {
    const organizations = session?.organizations ?? [];
    const projects = organizations.flatMap((org) =>
      org.projects.map((project) => ({ org, project })),
    );
    return {
      status,
      session,
      config,
      error,
      organizations,
      refresh: () => load(),
      signIn: async (email, password) =>
        adopt(await api.signIn(email, password)),
      signUp: async (email, password, name) =>
        adopt(await api.signUp(email, password, name)),
      startGuest: async () => adopt(await api.startGuest()),
      signOut: async () => {
        try {
          await api.signOut();
        } finally {
          setSession(null);
          setStatus("anonymous");
        }
      },
      findProject: (projectId) =>
        projects.find((entry) => entry.project.id === projectId) ?? null,
      // Prefer a workspace the visitor owns over the shared showcase, so a
      // returning account lands in its own project.
      defaultProjectId:
        projects.find((entry) => !entry.org.is_demo)?.project.id ??
        projects[0]?.project.id ??
        null,
    };
  }, [status, session, config, error, load, adopt]);

  return (
    <SessionContext.Provider value={value}>{children}</SessionContext.Provider>
  );
}

export function useSession(): SessionContextValue {
  const context = useContext(SessionContext);
  if (!context)
    throw new Error("useSession must be used inside SessionProvider");
  return context;
}
