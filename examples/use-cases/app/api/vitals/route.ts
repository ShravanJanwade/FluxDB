import { fail, ok, settings } from "../../../lib/server/context";
import { vitalsDashboard } from "../../../lib/server/vitalsDashboard";
import { pickWindow } from "../../../lib/time";
import { BEDS, findBed } from "../../../lib/vitals";

export const dynamic = "force-dynamic";
export const maxDuration = 60;

export async function GET(request: Request): Promise<Response> {
  try {
    const { db, buckets } = settings();
    const params = new URL(request.url).searchParams;
    const window = pickWindow(params.get("window"), ["1h", "6h", "24h"], "6h");
    const bed = findBed(params.get("bed")) ?? BEDS[0];
    return ok(await vitalsDashboard(db, buckets.vitals, window, bed));
  } catch (error) {
    return fail(error);
  }
}
