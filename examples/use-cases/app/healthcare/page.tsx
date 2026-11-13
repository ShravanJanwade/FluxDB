import type { Metadata } from "next";
import { WardDashboard } from "./WardDashboard";

export const metadata: Metadata = {
  title: "Ward vitals board",
  description: "Bedside-monitor vital signs stored in FluxDB, scored with NEWS2, with observations recorded by hand.",
};

export default function HealthcarePage() {
  return <WardDashboard />;
}
