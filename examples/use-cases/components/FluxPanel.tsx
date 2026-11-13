"use client";

import { Fragment } from "react";
import { clock, milliseconds, whole } from "../lib/format";
import { DAY, HOUR, MINUTE } from "../lib/time";
import type { FluxActivity } from "../lib/types";

const KEYWORDS =
  /\b(SELECT|FROM|WHERE|GROUP BY|ORDER BY|LIMIT|AND|OR|DESC|MEAN|MIN|MAX|SUM|COUNT|FIRST|LAST|STDDEV|time)\b|(\$timeFilter|\$interval)/g;

/** Light SQL highlighting, built from text nodes only. */
function Sql({ sql }: { sql: string }) {
  const parts: React.ReactNode[] = [];
  let index = 0;
  for (const match of sql.matchAll(KEYWORDS)) {
    if (match.index! > index) parts.push(sql.slice(index, match.index));
    parts.push(
      <span key={match.index} className={match[2] ? "macro" : "kw"}>
        {match[0]}
      </span>,
    );
    index = match.index! + match[0].length;
  }
  parts.push(sql.slice(index));
  return <pre className="sql">{parts}</pre>;
}

function span(ms: number): string {
  if (ms >= DAY) return `${Math.round(ms / DAY)} d`;
  if (ms >= HOUR) return `${Math.round(ms / HOUR)} h`;
  return `${Math.round(ms / MINUTE)} min`;
}

/**
 * The "How this page uses FluxDB" panel: what the last request wrote, every
 * query it ran with FluxDB's own execution time, and the code behind it.
 */
export function FluxPanel({
  activity,
  intro,
  code,
}: {
  activity: FluxActivity | null;
  intro: React.ReactNode;
  code: string;
}) {
  const written = activity?.ingest.reduce((sum, item) => sum + item.written, 0) ?? 0;
  const queryTime = activity?.queries.reduce((sum, q) => sum + q.serverMs, 0) ?? 0;
  return (
    <details className="flux-panel" open>
      <summary>
        <h2>How this page uses FluxDB</h2>
        {activity && (
          <span>
            Last refresh: {whole(written)} points written · {activity.queries.length} queries in {milliseconds(queryTime)} of
            FluxDB time · bucket <code>{activity.bucket}</code>
          </span>
        )}
      </summary>
      <div className="flux-body">
        <div style={{ color: "var(--ink-2)", fontSize: 14, maxWidth: 820 }}>{intro}</div>

        <div className="flux-step">
          <h3>1. Write what is new</h3>
          {!activity ? (
            <p className="form-note">Waiting for the first answer…</p>
          ) : (
            activity.ingest.map((item) => (
              <div key={item.stream} style={{ marginBottom: 10 }}>
                <div className="query-meta">
                  <b>{item.stream}</b>
                  {item.error ? (
                    <span>Feed unavailable ({item.error}); showing what FluxDB already holds.</span>
                  ) : item.written > 0 && item.range ? (
                    <>
                      <span>
                        {whole(item.written)} points,{" "}
                        {item.range.to - item.range.from < MINUTE
                          ? `readings for ${clock(item.range.to)}`
                          : `${span(item.range.to - item.range.from)} of readings up to ${clock(item.range.to)}`}
                      </span>
                      <span>
                        {item.requests} write request{item.requests === 1 ? "" : "s"}, {milliseconds(item.totalMs)}
                      </span>
                      {item.source && <span>from {item.source}</span>}
                    </>
                  ) : (
                    <span>Already up to date, nothing to write.</span>
                  )}
                </div>
                {item.sample.length > 0 && (
                  <pre className="lines" aria-label="Line protocol sent to FluxDB">
                    {item.sample.join("\n")}
                  </pre>
                )}
              </div>
            ))
          )}
        </div>

        <div className="flux-step">
          <h3>2. Query it</h3>
          {activity && (
            <ol className="query-list">
              {activity.queries.map((query, i) => (
                <li key={i}>
                  <div className="query-meta">
                    <b>{query.label}</b>
                    <span>
                      {whole(query.rows)} row{query.rows === 1 ? "" : "s"}
                    </span>
                    <span>{milliseconds(query.serverMs)} in FluxDB</span>
                    <span>{milliseconds(query.totalMs)} round trip</span>
                    <span>
                      last {span(query.window.to - query.window.from)}
                      {query.window.interval ? (
                        <Fragment>
                          , <code>$interval</code> = {query.window.interval}
                        </Fragment>
                      ) : null}
                    </span>
                  </div>
                  <Sql sql={query.sql} />
                </li>
              ))}
            </ol>
          )}
        </div>

        <div className="flux-step">
          <h3>3. The code</h3>
          <pre className="code-block">{code}</pre>
        </div>
      </div>
    </details>
  );
}
