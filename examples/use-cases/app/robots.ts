import type { MetadataRoute } from "next";

/**
 * Pages may be indexed, but crawlers stay out of /api: each API call can wake
 * the FluxDB server, which then runs (and bills) for half an hour.
 */
export default function robots(): MetadataRoute.Robots {
  return { rules: [{ userAgent: "*", allow: "/", disallow: "/api/" }] };
}
