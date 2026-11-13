/**
 * Line protocol: the text format FluxDB's write endpoint accepts.
 *
 *   power,building=lab hvac_kw=41.2,occupied=true,alarms=0i 1731412800000
 *   └─measurement─┘└tags┘ └──────────── fields ────────────┘ └─ time ─┘
 *
 * Tags are short strings you filter and group by (`WHERE building = 'lab'`,
 * `GROUP BY building`). Fields hold the measured values. Writing a point with
 * the same measurement, tags and timestamp as a stored one replaces it, which
 * is what makes the generators in this app safe to re-run.
 */

/** A number is written as a float; a bigint as an integer (`3i`). */
export type FieldValue = number | bigint | boolean | string;

export interface Point {
  measurement: string;
  tags?: Record<string, string | undefined>;
  fields: Record<string, FieldValue | null | undefined>;
  /** Milliseconds since the Unix epoch. */
  time: number;
}

function checkName(kind: string, value: string): void {
  if (value.length === 0) throw new Error(`Empty ${kind}`);
  if (/[\n\r]/.test(value)) throw new Error(`A ${kind} cannot contain a line break: ${JSON.stringify(value)}`);
}

function escapeMeasurement(value: string): string {
  checkName("measurement", value);
  return value.replace(/[, ]/g, (c) => `\\${c}`);
}

/** Tag keys, tag values and field keys escape commas, equals signs and spaces. */
function escapeKey(kind: string, value: string): string {
  checkName(kind, value);
  return value.replace(/[,= ]/g, (c) => `\\${c}`);
}

function formatField(key: string, value: FieldValue): string {
  switch (typeof value) {
    case "number":
      if (!Number.isFinite(value)) throw new Error(`Field ${key} is not a finite number: ${value}`);
      return String(value);
    case "bigint":
      return `${value}i`;
    case "boolean":
      return value ? "true" : "false";
    case "string":
      return `"${value.replace(/[\\"]/g, (c) => `\\${c}`).replace(/[\n\r]+/g, " ")}"`;
  }
}

/** One point as a line of line protocol, timestamp in milliseconds. */
export function toLine(point: Point): string {
  let line = escapeMeasurement(point.measurement);
  const tags = Object.entries(point.tags ?? {})
    .filter((entry): entry is [string, string] => entry[1] !== undefined && entry[1] !== "")
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
  for (const [key, value] of tags) {
    line += `,${escapeKey("tag key", key)}=${escapeKey("tag value", value)}`;
  }
  const fields = Object.entries(point.fields).filter(
    (entry): entry is [string, FieldValue] => entry[1] !== null && entry[1] !== undefined,
  );
  if (fields.length === 0) throw new Error(`A ${point.measurement} point needs at least one field`);
  line += " " + fields.map(([key, value]) => `${escapeKey("field key", key)}=${formatField(key, value)}`).join(",");
  if (!Number.isFinite(point.time)) throw new Error(`Invalid timestamp: ${point.time}`);
  return `${line} ${Math.round(point.time)}`;
}
