import { fail, settings } from "../../../lib/server/context";

export const dynamic = "force-dynamic";
export const maxDuration = 60;

/**
 * Which project the API key belongs to and whether the three buckets exist.
 * The home page calls this on load, which also wakes a sleeping FluxDB before
 * the visitor picks a dashboard.
 */
export async function GET(): Promise<Response> {
  try {
    const { db, buckets } = settings();
    const started = performance.now();
    const me = await db.whoami();
    const present = new Map(me.buckets.map((bucket) => [bucket.name, bucket]));
    return Response.json(
      {
        project: me.project.name,
        url: db.url,
        scopes: me.key.scopes,
        buckets: Object.entries(buckets).map(([useCase, name]) => ({
          useCase,
          name,
          present: present.has(name),
          retentionSeconds: present.get(name)?.retention_seconds ?? null,
        })),
        totalMs: Math.round(performance.now() - started),
      },
      { headers: { "cache-control": "no-store" } },
    );
  } catch (error) {
    return fail(error);
  }
}
