import type { Metadata } from "next";
import { MarketsDashboard } from "./MarketsDashboard";

export const metadata: Metadata = {
  title: "Crypto market board",
  description: "Real one-minute crypto candles stored in FluxDB and rolled up into candlesticks at query time.",
};

export default function MarketsPage() {
  return <MarketsDashboard />;
}
