import { num } from "../../../../lib/fluxdb";
import { fail, InvalidRequestError, RateLimitedError, settings } from "../../../../lib/server/context";
import { HOUR } from "../../../../lib/time";
import { findBed, news2, OBSERVATION_LIMITS, vitalsPoint, type Vitals } from "../../../../lib/vitals";

export const dynamic = "force-dynamic";
export const maxDuration = 60;

/** Manual observations the whole ward may record per hour; the rest of the demo stays readable. */
const HOURLY_LIMIT = 60;

function reading(body: Record<string, unknown>, key: keyof typeof OBSERVATION_LIMITS, label: string): number {
  const value = Number(body[key]);
  const [low, high] = OBSERVATION_LIMITS[key];
  if (!Number.isFinite(value) || value < low || value > high) {
    throw new InvalidRequestError(`${label} must be between ${low} and ${high}`);
  }
  return value;
}

/** A nurse records a set of observations by hand; it lands next to the monitor's readings. */
export async function POST(request: Request): Promise<Response> {
  try {
    const origin = request.headers.get("origin");
    if (origin && new URL(origin).host !== new URL(request.url).host) {
      throw new InvalidRequestError("Observations can only be recorded from this site");
    }
    const { db, buckets } = settings();
    const body = (await request.json().catch(() => null)) as Record<string, unknown> | null;
    if (!body || typeof body !== "object") throw new InvalidRequestError("Send the observation as JSON");
    const bed = findBed(typeof body.bed === "string" ? body.bed : null);
    if (!bed) throw new InvalidRequestError("Choose a bed");
    const acvpu = typeof body.acvpu === "string" ? body.acvpu : "A";
    if (!["A", "C", "V", "P", "U"].includes(acvpu)) throw new InvalidRequestError("Consciousness must be A, C, V, P or U");
    const vitals: Vitals = {
      hr: Math.round(reading(body, "hr", "Pulse")),
      spo2: Math.round(reading(body, "spo2", "SpO2")),
      rr: Math.round(reading(body, "rr", "Respiratory rate")),
      sbp: Math.round(reading(body, "sbp", "Systolic BP")),
      temp_c: Math.round(reading(body, "temp_c", "Temperature") * 10) / 10,
      on_o2: body.on_o2 === true,
      acvpu: acvpu as Vitals["acvpu"],
    };

    // The cap is counted in FluxDB itself, so it holds across every server instance.
    const now = Date.now();
    const recent = await db.query(
      buckets.vitals,
      "SELECT COUNT(hr) FROM vitals WHERE $timeFilter AND source = 'manual'",
      { from: now - HOUR, to: now },
      "Manual observations this hour",
    );
    if ((num(recent.rows[0]?.count_hr) ?? 0) >= HOURLY_LIMIT) {
      throw new RateLimitedError(`The ward has recorded ${HOURLY_LIMIT} manual observations this hour. Try again later.`);
    }

    const written = await db.write(buckets.vitals, [vitalsPoint(bed, vitals, now, "manual")]);
    return Response.json(
      { line: written.sample[0], writeMs: written.totalMs, score: news2(vitals, bed.spo2Scale) },
      { status: 201, headers: { "cache-control": "no-store" } },
    );
  } catch (error) {
    return fail(error);
  }
}
