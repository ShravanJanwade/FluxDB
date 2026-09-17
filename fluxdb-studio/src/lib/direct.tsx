/**
 * Connections to a FluxDB server the visitor runs themselves.
 *
 * The browser talks to that server directly. Its administration token is held
 * in memory for the lifetime of the tab and is deliberately never written to
 * `localStorage` and never sent to this origin: a hosted console has no
 * business holding the keys to someone else's database. The consequence — the
 * token must be entered again after a reload — is stated in the UI.
 */

import {
  createContext,
  useCallback,
  useContext,
  useMemo,
  useState,
  type ReactNode,
} from "react";
import { probeDirectServer, type DirectServerInfo } from "./api";

export type DirectServer = {
  /** Origin without a trailing slash. */
  url: string;
  /** Session-only. Never persisted. */
  token: string;
  version: string;
  databases: string[];
  authenticated: boolean;
  connectedAt: number;
};

/** Addresses the visitor has used before, so they need not retype them.
 *  Addresses only: never tokens. */
const RECENT_KEY = "fluxdb.recentServers";

function loadRecent(): string[] {
  try {
    const parsed = JSON.parse(localStorage.getItem(RECENT_KEY) ?? "[]");
    return Array.isArray(parsed)
      ? parsed
          .filter((entry): entry is string => typeof entry === "string")
          .slice(0, 6)
      : [];
  } catch {
    return [];
  }
}

type DirectContextValue = {
  server: DirectServer | null;
  recent: string[];
  connect: (url: string, token: string) => Promise<DirectServer>;
  disconnect: () => void;
  /** Re-read the database list after one is created or dropped. */
  refreshDatabases: () => Promise<void>;
};

const DirectContext = createContext<DirectContextValue | null>(null);

export function DirectProvider({ children }: { children: ReactNode }) {
  const [server, setServer] = useState<DirectServer | null>(null);
  const [recent, setRecent] = useState<string[]>(loadRecent);

  const remember = useCallback((url: string) => {
    setRecent((current) => {
      const next = [url, ...current.filter((entry) => entry !== url)].slice(
        0,
        6,
      );
      try {
        localStorage.setItem(RECENT_KEY, JSON.stringify(next));
      } catch {
        // Not being able to remember an address is not worth failing over.
      }
      return next;
    });
  }, []);

  const connect = useCallback(
    async (rawUrl: string, token: string) => {
      const url = rawUrl.trim().replace(/\/+$/, "");
      const info: DirectServerInfo = await probeDirectServer(url, token);
      const connected: DirectServer = {
        url,
        token,
        version: info.version,
        databases: info.databases,
        authenticated: info.authenticated,
        connectedAt: Date.now(),
      };
      setServer(connected);
      remember(url);
      return connected;
    },
    [remember],
  );

  const value = useMemo<DirectContextValue>(
    () => ({
      server,
      recent,
      connect,
      disconnect: () => setServer(null),
      refreshDatabases: async () => {
        if (!server) return;
        const info = await probeDirectServer(server.url, server.token);
        setServer({
          ...server,
          databases: info.databases,
          version: info.version,
        });
      },
    }),
    [server, recent, connect],
  );

  return (
    <DirectContext.Provider value={value}>{children}</DirectContext.Provider>
  );
}

export function useDirect(): DirectContextValue {
  const context = useContext(DirectContext);
  if (!context) throw new Error("useDirect must be used inside DirectProvider");
  return context;
}
