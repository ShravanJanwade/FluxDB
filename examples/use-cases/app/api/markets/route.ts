import { findProduct, PRODUCTS } from "../../../lib/markets";
import { fail, ok, settings } from "../../../lib/server/context";
import { marketsDashboard } from "../../../lib/server/marketsDashboard";
import { pickWindow } from "../../../lib/time";

export const dynamic = "force-dynamic";
export const maxDuration = 60;

export async function GET(request: Request): Promise<Response> {
  try {
    const { db, buckets } = settings();
    const params = new URL(request.url).searchParams;
    const window = pickWindow(params.get("window"), ["1h", "6h", "24h"], "6h");
    const product = findProduct(params.get("product")) ?? PRODUCTS[0];
    return ok(await marketsDashboard(db, buckets.markets, window, product));
  } catch (error) {
    return fail(error);
  }
}
