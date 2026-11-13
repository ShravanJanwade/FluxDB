import { nsToMs, num, str, type FluxDB, type QueryWindow, type Row } from "../fluxdb";
import { BUILDINGS, ENERGY_STEP, ENERGY_TIME_ZONE, energyPoints, isPeakHour, TARIFF } from "../energy";
import { catchUp, readWatermarks } from "../sync";
import { DAY, HOUR, intervalMs, startOfLocalDay, type WindowSpec } from "../time";
import type { EnergyResponse, SeriesPoint } from "../types";
import { Recorder } from "./context";

/** kW readings are 5-minute averages, so energy is SUM(kW) × 5/60 kWh. */
const KWH_PER_KW_READING = ENERGY_STEP / HOUR;

function series(rows: Row[], column: string): SeriesPoint[] {
  return rows.map((row) => ({ t: nsToMs(row.time), v: num(row[column]) }));
}

export async function energyDashboard(db: FluxDB, bucket: string, spec: WindowSpec, now = Date.now()): Promise<EnergyResponse> {
  const recorder = new Recorder(bucket);

  // 1. Write every meter reading since the last visit (up to a week).
  const { marks, query } = await readWatermarks(db, bucket, now, 7 * DAY);
  recorder.query(query);
  recorder.synced(
    await catchUp(
      db,
      bucket,
      [
        {
          stream: "campus-meters",
          stepMs: ENERGY_STEP,
          maxBackfillMs: 7 * DAY,
          produce: async (from, to) => ({ points: energyPoints(from, to), watermark: to, source: "Simulated meters" }),
        },
      ],
      marks,
      now,
    ),
  );

  // 2. Ask FluxDB for everything the page shows, in parallel.
  const window: QueryWindow = { from: now - spec.ms, to: now, interval: spec.interval };
  const today: QueryWindow = { from: startOfLocalDay(now, ENERGY_TIME_ZONE), to: now, interval: "1h" };
  const [byBuilding, campus, buildingTotals, campusNow, hourly] = await Promise.all([
    db.query(
      bucket,
      "SELECT MEAN(net_kw) FROM power WHERE $timeFilter GROUP BY time($interval), building",
      window,
      "Demand by building",
    ),
    db.query(
      bucket,
      "SELECT MEAN(hvac_kw), MEAN(lighting_kw), MEAN(plug_kw), MEAN(ev_kw), MEAN(solar_kw), MEAN(outdoor_c) FROM campus WHERE $timeFilter GROUP BY time($interval)",
      window,
      "Campus end use, solar and weather",
    ),
    db.query(
      bucket,
      "SELECT LAST(net_kw), MAX(net_kw), SUM(kwh), SUM(solar_kw) FROM power WHERE $timeFilter GROUP BY building",
      window,
      "Totals per building",
    ),
    db.query(bucket, "SELECT LAST(net_kw), MAX(net_kw), LAST(outdoor_c) FROM campus WHERE $timeFilter", window, "Campus demand now and peak"),
    db.query(
      bucket,
      "SELECT SUM(kwh), SUM(solar_kw) FROM campus WHERE $timeFilter GROUP BY time($interval)",
      today,
      "Today, hour by hour (for the bill)",
    ),
  ]);
  for (const result of [byBuilding, campus, buildingTotals, campusNow, hourly]) recorder.query(result);

  const totals = new Map(buildingTotals.rows.map((row) => [str(row.building), row]));
  const summary = campusNow.rows[0] ?? {};
  let energyToday: number | null = null;
  let costToday: number | null = null;
  let solarToday: number | null = null;
  for (const row of hourly.rows) {
    const kwh = num(row.sum_kwh);
    const solar = num(row.sum_solar_kw);
    if (kwh !== null) {
      // Price each hour by the tariff in force at its middle.
      const rate = isPeakHour(nsToMs(row.time) + HOUR / 2) ? TARIFF.peak : TARIFF.offPeak;
      energyToday = (energyToday ?? 0) + kwh;
      costToday = (costToday ?? 0) + kwh * rate;
    }
    if (solar !== null) solarToday = (solarToday ?? 0) + solar * KWH_PER_KW_READING;
  }

  return {
    window: { key: spec.key, from: window.from, to: window.to, interval: spec.interval, intervalMs: intervalMs(spec.interval) },
    timeZone: ENERGY_TIME_ZONE,
    buildings: BUILDINGS.map((building) => {
      const row = totals.get(building.id);
      const solarSum = num(row?.sum_solar_kw);
      return {
        id: building.id,
        name: building.name,
        series: series(
          byBuilding.rows.filter((r) => str(r.building) === building.id),
          "mean_net_kw",
        ),
        nowKw: num(row?.last_net_kw),
        peakKw: num(row?.max_net_kw),
        netKwh: num(row?.sum_kwh),
        solarKwh: solarSum === null ? null : solarSum * KWH_PER_KW_READING,
      };
    }),
    endUse: campus.rows.map((row) => ({
      t: nsToMs(row.time),
      hvac_kw: num(row.mean_hvac_kw) ?? 0,
      lighting_kw: num(row.mean_lighting_kw) ?? 0,
      plug_kw: num(row.mean_plug_kw) ?? 0,
      ev_kw: num(row.mean_ev_kw) ?? 0,
    })),
    solar: series(campus.rows, "mean_solar_kw"),
    outdoor: series(campus.rows, "mean_outdoor_c"),
    kpis: {
      demandNowKw: num(summary.last_net_kw),
      demandPeakKw: num(summary.max_net_kw),
      energyTodayKwh: energyToday,
      costToday,
      solarTodayKwh: solarToday,
      outdoorNowC: num(summary.last_outdoor_c),
    },
    flux: recorder.activity(),
  };
}
