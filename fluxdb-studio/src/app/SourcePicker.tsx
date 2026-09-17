/** Chooses which bucket — or which database on a connected self-hosted server
 *  — the current screen reads from. */

import { Link } from "react-router-dom";
import { Cloud, Server } from "lucide-react";
import { useProject } from "./ProjectContext";

export function SourcePicker({ compact = false }: { compact?: boolean }) {
  const { targets, target, selectTarget, targetKey, detail } = useProject();

  if (targets.length === 0) {
    return (
      <Link
        className="btn btn-sm btn-primary"
        to={`/app/p/${detail.project.id}/buckets`}
      >
        Create a bucket first
      </Link>
    );
  }

  const cloudTargets = targets.filter((entry) => entry.kind === "cloud");
  const directTargets = targets.filter((entry) => entry.kind === "direct");

  return (
    <div className="source-picker">
      {!compact && (
        <span className="source-badge">
          {target?.kind === "direct" ? (
            <>
              <Server size={11} aria-hidden /> Your server
            </>
          ) : (
            <>
              <Cloud size={11} aria-hidden /> Hosted
            </>
          )}
        </span>
      )}
      <label>
        <span className="visually-hidden">Data source</span>
        <select
          className="select"
          value={target ? targetKey(target) : ""}
          onChange={(event) => selectTarget(event.target.value)}
        >
          {cloudTargets.length > 0 && (
            <optgroup label="Project buckets">
              {cloudTargets.map((entry) => (
                <option key={targetKey(entry)} value={targetKey(entry)}>
                  {entry.label}
                </option>
              ))}
            </optgroup>
          )}
          {directTargets.length > 0 && (
            <optgroup label="Your server">
              {directTargets.map((entry) => (
                <option key={targetKey(entry)} value={targetKey(entry)}>
                  {entry.label}
                </option>
              ))}
            </optgroup>
          )}
        </select>
      </label>
    </div>
  );
}

/** True when the current source is a bucket this visitor may change. */
export function useCanWrite(): boolean {
  const { detail, target } = useProject();
  if (!target) return false;
  // A self-hosted server's permissions are its own; the console assumes the
  // supplied token is an administration token, which it is by design.
  if (target.kind === "direct") return true;
  return detail.project.writable;
}
