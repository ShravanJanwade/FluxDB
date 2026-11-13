import { energyDashboard } from "../../../lib/server/energyDashboard";
import { fail, ok, settings } from "../../../lib/server/context";
import { pickWindow } from "../../../lib/time";

export const dynamic = "force-dynamic";
// A sleeping FluxDB takes a little while to answer its first request.
export const maxDuration = 60;

export async function GET(request: Request): Promise<Response> {
  try {
    const { db, buckets } = settings();
    const params = new URL(request.url).searchParams;
    const window = pickWindow(params.get("window"), ["6h", "24h", "7d"], "24h");
    return ok(await energyDashboard(db, buckets.energy, window));
  } catch (error) {
    return fail(error);
  }
}
