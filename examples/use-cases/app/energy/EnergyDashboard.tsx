"use client";

import { useState } from "react";
import { LineChart, type LineSeries } from "../../components/charts/LineChart";
import { StackedColumns } from "../../components/charts/StackedColumns";
import { FluxPanel } from "../../components/FluxPanel";
import { ChartCard, LiveStatus, LoadState, Segmented, StatTile } from "../../components/ui";
import { useLiveData } from "../../components/useLiveData";
import { END_USES, TARIFF } from "../../lib/energy";
import { clock, compact, fixed, usd, whole } from "../../lib/format";
import { DAY } from "../../lib/time";
import type { EnergyResponse } from "../../lib/types";

type WindowKey = "6h" | "24h" | "7d";

const WINDOWS: { value: WindowKey; label: string }[] = [
  { value: "6h", label: "6H" },
  { value: "24h", label: "24H" },
  { value: "7d", label: "7D" },
];

/** Categorical slots in fixed order: buildings take 1-4, end uses 5-8, so no color means two things here. */
const BUILDING_STYLE: Record<string, { color: string; short: string }> = {
  admin: { color: "var(--series-1)", short: "Admin" },
  lab: { color: "var(--series-2)", short: "Lab" },
  residence: { color: "var(--series-3)", short: "Residence" },
  garage: { color: "var(--series-4)", short: "Garage" },
};

const END_USE_COLORS = ["var(--series-5)", "var(--series-6)", "var(--series-7)", "var(--series-8)"];

const kw = (value: number) => `${whole(value)} kW`;

const CODE = `// app/api/energy/route.ts, abridged
const db = new FluxDB({ url: process.env.FLUXDB_URL, apiKey: process.env.FLUXDB_API_KEY });

// Write: one point per building per 5 minutes (line protocol under the hood)
await db.write("energy", [{
  measurement: "power",
  tags: { building: "lab" },
  fields: { hvac_kw: 74.2, lighting_kw: 21.0, plug_kw: 142.5, ev_kw: 0, solar_kw: 0, net_kw: 237.7, kwh: 19.81 },
  time: Date.now(),
}]);

// Read: FluxDB averages the readings into $interval buckets per building
const demand = await db.query(
  "energy",
  "SELECT MEAN(net_kw) FROM power WHERE $timeFilter GROUP BY time($interval), building",
  { from: Date.now() - 24 * HOUR, to: Date.now(), interval: "15m" },
);`;

export function EnergyDashboard() {
  const [windowKey, setWindowKey] = useState<WindowKey>("24h");
  const live = useLiveData<EnergyResponse>(`/api/energy?window=${windowKey}`, 60_000);
  const data = live.data;
  const tz = data?.timeZone;
  const domain: [number, number] = data ? [data.window.from, data.window.to] : [0, 1];
  const withDay = data ? data.window.to - data.window.from > DAY * 0.99 : false;

  const buildingSeries: LineSeries[] =
    data?.buildings.map((b) => ({
      id: b.id,
      label: b.name,
      short: BUILDING_STYLE[b.id]?.short,
      color: BUILDING_STYLE[b.id]?.color ?? "var(--ink-2)",
      points: b.series,
    })) ?? [];
  const times = [...new Set(buildingSeries.flatMap((s) => s.points.map((p) => p.t)))].sort((a, b) => a - b);
  const kpis = data?.kpis;
  const solarShare =
    kpis?.solarTodayKwh != null && kpis.energyTodayKwh ? (kpis.solarTodayKwh / (kpis.energyTodayKwh + kpis.solarTodayKwh)) * 100 : null;

  return (
    <>
      <div className="dash-head">
        <div>
          <p className="eyebrow">Energy · smart buildings</p>
          <h1>Campus energy monitor</h1>
          <p>
            A four-building campus near Boston. Every five minutes each building&apos;s submeter reports heating and cooling,
            lighting, plug loads, EV charging and rooftop solar; the main meter adds campus totals and the outdoor temperature.
            The meters are simulated, the storage and every number below come from FluxDB.
          </p>
        </div>
      </div>

      <div className="filters">
        <Segmented label="Time range" options={WINDOWS} value={windowKey} onChange={setWindowKey} />
        <LiveStatus live={live} />
      </div>
      <LoadState live={live} />

      {data && kpis && (
        <>
          <div className="kpis">
            <StatTile
              label="Campus demand now"
              value={kpis.demandNowKw === null ? "–" : whole(kpis.demandNowKw)}
              unit="kW"
              sub={kpis.demandPeakKw === null ? undefined : `Peak ${whole(kpis.demandPeakKw)} kW in this range`}
            />
            <StatTile
              label="Energy today"
              value={kpis.energyTodayKwh === null ? "–" : compact(kpis.energyTodayKwh)}
              unit="kWh"
              sub="Since midnight, campus time"
            />
            <StatTile
              label="Cost today"
              value={kpis.costToday === null ? "–" : usd(kpis.costToday, 0)}
              sub={`${usd(TARIFF.offPeak)} off-peak, ${usd(TARIFF.peak)} weekday 12–8 pm`}
            />
            <StatTile
              label="Solar today"
              value={kpis.solarTodayKwh === null ? "–" : whole(kpis.solarTodayKwh)}
              unit="kWh"
              sub={solarShare === null ? undefined : `${fixed(solarShare, 1)}% of today's use`}
            />
            <StatTile
              label="Outside"
              value={kpis.outdoorNowC === null ? "–" : fixed(kpis.outdoorNowC, 1)}
              unit="°C"
              sub="Campus weather station"
            />
          </div>

          <ChartCard
            title="Grid demand by building"
            subtitle={`Average kW per ${data.window.interval}, campus time. Below zero, solar is covering the building and exporting.`}
            legend={buildingSeries.map((s) => ({ label: s.label, color: s.color }))}
            table={{
              columns: ["Time", ...buildingSeries.map((s) => `${s.label} (kW)`)],
              rows: times.map((t) => [
                clock(t, tz, withDay),
                ...buildingSeries.map((s) => {
                  const v = s.points.find((p) => p.t === t)?.v;
                  return v == null ? "–" : fixed(v, 1);
                }),
              ]),
            }}
          >
            <LineChart
              series={buildingSeries}
              domain={domain}
              stepMs={data.window.intervalMs}
              format={kw}
              axisFormat={(v) => whole(v)}
              includeZero
              endLabels
              timeZone={tz}
              height={280}
              refreshing={live.refreshing}
              label="Line chart of grid demand in kilowatts for four buildings over the selected range"
            />
          </ChartCard>

          <div className="charts-2" style={{ marginTop: 16 }}>
            <ChartCard
              title="Where the campus load goes"
              subtitle={`Average kW by end use per ${data.window.interval}, before solar`}
              legend={END_USES.map((use, i) => ({ label: use.label, color: END_USE_COLORS[i], kind: "box" as const }))}
              table={{
                columns: ["Time", ...END_USES.map((use) => `${use.label} (kW)`)],
                rows: data.endUse.map((row) => [clock(row.t, tz, withDay), ...END_USES.map((use) => fixed(row[use.id], 1))]),
              }}
            >
              <StackedColumns
                keys={END_USES.map((use, i) => ({ id: use.id, label: use.label, color: END_USE_COLORS[i] }))}
                rows={data.endUse.map((row) => ({ t: row.t, values: { ...row } }))}
                domain={domain}
                bucketMs={data.window.intervalMs}
                format={kw}
                axisFormat={(v) => whole(v)}
                timeZone={tz}
                height={280}
                refreshing={live.refreshing}
                label="Stacked columns of campus load by end use"
              />
            </ChartCard>
            <div className="grid">
              <ChartCard
                title="Solar generation"
                subtitle="Admin roof and garage canopy, kW"
                table={{ columns: ["Time", "Solar (kW)"], rows: data.solar.map((p) => [clock(p.t, tz, withDay), p.v == null ? "–" : fixed(p.v, 1)]) }}
              >
                <LineChart
                  series={[{ id: "solar", label: "Solar", color: "var(--ink-2)", points: data.solar, area: true }]}
                  domain={domain}
                  stepMs={data.window.intervalMs}
                  format={kw}
                  axisFormat={(v) => whole(v)}
                  includeZero
                  timeZone={tz}
                  height={124}
                  refreshing={live.refreshing}
                  label="Solar generation in kilowatts"
                />
              </ChartCard>
              <ChartCard
                title="Outdoor temperature"
                subtitle="°C. Heating load climbs as it drops."
                table={{
                  columns: ["Time", "Outdoor (°C)"],
                  rows: data.outdoor.map((p) => [clock(p.t, tz, withDay), p.v == null ? "–" : fixed(p.v, 1)]),
                }}
              >
                <LineChart
                  series={[{ id: "outdoor", label: "Outdoor", color: "var(--ink-2)", points: data.outdoor }]}
                  domain={domain}
                  stepMs={data.window.intervalMs}
                  format={(v) => `${fixed(v, 1)} °C`}
                  axisFormat={(v) => fixed(v, 0)}
                  timeZone={tz}
                  height={124}
                  refreshing={live.refreshing}
                  label="Outdoor temperature in degrees Celsius"
                />
              </ChartCard>
            </div>
          </div>

          <section className="card" style={{ marginTop: 16 }} aria-label="Buildings">
            <div className="card-head">
              <div>
                <h2>Buildings</h2>
                <p>Totals for the selected range, one GROUP BY building query</p>
              </div>
            </div>
            <div className="table-wrap">
              <table className="data">
                <thead>
                  <tr>
                    <th>Building</th>
                    <th className="num">Now</th>
                    <th className="num">Peak</th>
                    <th className="num">Grid energy</th>
                    <th className="num">Solar</th>
                  </tr>
                </thead>
                <tbody>
                  {data.buildings.map((b) => (
                    <tr key={b.id}>
                      <td>
                        <span className="series-key" style={{ background: BUILDING_STYLE[b.id]?.color }} />
                        {b.name}
                      </td>
                      <td className="num">{b.nowKw === null ? "–" : kw(b.nowKw)}</td>
                      <td className="num">{b.peakKw === null ? "–" : kw(b.peakKw)}</td>
                      <td className="num">{b.netKwh === null ? "–" : `${compact(b.netKwh)} kWh`}</td>
                      <td className="num">{b.solarKwh ? `${compact(b.solarKwh)} kWh` : "–"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>
        </>
      )}

      <FluxPanel
        activity={data?.flux ?? null}
        code={CODE}
        intro={
          <p style={{ margin: 0 }}>
            Nothing runs on a timer. When this page asks for data, the route handler reads a watermark from FluxDB, writes every
            5-minute reading since then (up to a week after a quiet spell), and runs the queries below in parallel. Because each
            reading is a pure function of its timestamp and FluxDB replaces a point with the same measurement, tags and time,
            two visitors filling the same gap leave one copy.
          </p>
        }
      />
    </>
  );
}
