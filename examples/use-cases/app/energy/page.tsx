import type { Metadata } from "next";
import { EnergyDashboard } from "./EnergyDashboard";

export const metadata: Metadata = {
  title: "Campus energy monitor",
  description: "Building submeter data stored in FluxDB: demand by building, end-use breakdown, solar and the daily bill.",
};

export default function EnergyPage() {
  return <EnergyDashboard />;
}
