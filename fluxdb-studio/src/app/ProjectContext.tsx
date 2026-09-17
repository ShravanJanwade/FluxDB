/**
 * Everything a project screen needs, loaded once.
 *
 * Also owns the selected data source. A project's buckets and any databases on
 * a self-hosted server the visitor has connected are presented as one list, so
 * the explorer, the query workspace and the dashboards are written against a
 * single `DataClient` and do not care which they are pointed at.
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
import { dataClient, type DataClient } from "../lib/api";
import { useDirect } from "../lib/direct";
import type { DataTarget, ProjectDetail } from "../lib/types";

type ProjectContextValue = {
  detail: ProjectDetail;
  reload: () => void;
  /** Project buckets and connected self-hosted databases, in one list. */
  targets: DataTarget[];
  target: DataTarget | null;
  selectTarget: (key: string) => void;
  client: DataClient | null;
  /** Stable key for a target, used by the selector and for persistence. */
  targetKey: (target: DataTarget) => string;
};

const ProjectContext = createContext<ProjectContextValue | null>(null);

function keyOf(target: DataTarget): string {
  return target.kind === "cloud"
    ? `cloud:${target.bucketId}`
    : `direct:${target.baseUrl}:${target.database}`;
}

export function ProjectProvider({
  detail,
  reload,
  children,
}: {
  detail: ProjectDetail;
  reload: () => void;
  children: ReactNode;
}) {
  const { server } = useDirect();
  const storageKey = `fluxdb.target.${detail.project.id}`;
  const [selected, setSelected] = useState<string | null>(() => {
    try {
      return sessionStorage.getItem(storageKey);
    } catch {
      return null;
    }
  });

  const targets = useMemo<DataTarget[]>(() => {
    const cloud: DataTarget[] = detail.buckets.map((bucket) => ({
      kind: "cloud",
      projectId: detail.project.id,
      bucketId: bucket.id,
      label: bucket.name,
    }));
    const direct: DataTarget[] = (server?.databases ?? []).map((database) => ({
      kind: "direct",
      baseUrl: server!.url,
      token: server!.token,
      database,
      label: database,
    }));
    return [...cloud, ...direct];
  }, [detail.buckets, detail.project.id, server]);

  // Fall back to the first available source whenever the stored selection is
  // gone — a deleted bucket or a disconnected server.
  const target = useMemo(() => {
    const match = targets.find((candidate) => keyOf(candidate) === selected);
    return match ?? targets[0] ?? null;
  }, [targets, selected]);

  const selectTarget = useCallback(
    (key: string) => {
      setSelected(key);
      try {
        sessionStorage.setItem(storageKey, key);
      } catch {
        // A selection that cannot be persisted still applies for this view.
      }
    },
    [storageKey],
  );

  useEffect(() => {
    if (target && selected !== keyOf(target)) {
      setSelected(keyOf(target));
    }
  }, [target, selected]);

  const value = useMemo<ProjectContextValue>(
    () => ({
      detail,
      reload,
      targets,
      target,
      selectTarget,
      client: target ? dataClient(target) : null,
      targetKey: keyOf,
    }),
    [detail, reload, targets, target, selectTarget],
  );

  return (
    <ProjectContext.Provider value={value}>{children}</ProjectContext.Provider>
  );
}

export function useProject(): ProjectContextValue {
  const context = useContext(ProjectContext);
  if (!context) {
    throw new Error("useProject must be used inside ProjectProvider");
  }
  return context;
}

/** The bucket record behind the selected target, when it is a cloud bucket. */
export function useSelectedBucket() {
  const { detail, target } = useProject();
  if (!target || target.kind !== "cloud") return null;
  return detail.buckets.find((bucket) => bucket.id === target.bucketId) ?? null;
}
