import { nsToMs, num, str, type FluxDB, type Interval, type QueryWindow, type Row } from "../fluxdb";
import { catchUp, readWatermarks } from "../sync";
import { DAY, intervalMs, MINUTE, type WindowSpec } from "../time";
import type { BedStatus, VitalsResponse } from "../types";
import { BEDS, news2, VITALS_STEP, wardPoints, WARD_TIME_ZONE, type Bed, type Vitals } from "../vitals";
import { Recorder } from "./context";

const ALERT_INTERVAL: Record<string, Interval> = { "1h": "5m", "6h": "15m", "24h": "1h" };

/** Readings that would trigger a NEWS2 point or more on their own, SpO2 aside (it depends on the patient's target). */
export const ALERT_CONDITION = "(hr > 110 OR rr > 24 OR sbp <= 100 OR temp_c >= 38.1)";

const VITAL_KEYS = ["hr", "spo2", "rr", "sbp", "temp_c"] as const;

function latestVitals(row: Row | undefined): Vitals | null {
  if (!row) return null;
  const hr = num(row.last_hr);
  const spo2 = num(row.last_spo2);
  const rr = num(row.last_rr);
  const sbp = num(row.last_sbp);
  const temp = num(row.last_temp_c);
  if (hr === null || spo2 === null || rr === null || sbp === null || temp === null) return null;
  const acvpu = str(row.last_acvpu);
  return {
    hr,
    spo2,
    rr,
    sbp,
    temp_c: temp,
    on_o2: row.last_on_o2 === true,
    acvpu: (["A", "C", "V", "P", "U"].includes(acvpu) ? acvpu : "A") as Vitals["acvpu"],
  };
}

export async function vitalsDashboard(
  db: FluxDB,
  bucket: string,
  spec: WindowSpec,
  selected: Bed,
  now = Date.now(),
): Promise<VitalsResponse> {
  const recorder = new Recorder(bucket);

  // 1. Chart every monitor reading since the last visit (up to a day).
  const { marks, query } = await readWatermarks(db, bucket, now, DAY);
  recorder.query(query);
  const [report] = recorder.synced(
    await catchUp(
      db,
      bucket,
      [
        {
          stream: "ward-monitors",
          stepMs: VITALS_STEP,
          maxBackfillMs: DAY,
          produce: async (from, to) => ({ points: wardPoints(from, to), watermark: to, source: "Simulated bedside monitors" }),
        },
      ],
      marks,
      now,
    ),
  );

  // 2. Query the ward.
  const window: QueryWindow = { from: now - spec.ms, to: now, interval: spec.interval };
  const alertInterval = ALERT_INTERVAL[spec.key] ?? "15m";
  const bed = `'${selected.id}'`;
  const stats = VITAL_KEYS.map((key) => `MEAN(${key}), MIN(${key}), MAX(${key})`).join(", ");
  const [latest, trend, detail, manual, alerts, count] = await Promise.all([
    db.query(
      bucket,
      "SELECT LAST(hr), LAST(spo2), LAST(rr), LAST(sbp), LAST(temp_c), LAST(on_o2), LAST(acvpu) FROM vitals WHERE $timeFilter GROUP BY bed",
      { from: now - 15 * MINUTE, to: now },
      "Latest observation per bed",
    ),
    db.query(bucket, "SELECT MEAN(hr) FROM vitals WHERE $timeFilter GROUP BY time($interval), bed", window, "Heart-rate trend per bed"),
    db.query(
      bucket,
      `SELECT ${stats} FROM vitals WHERE $timeFilter AND bed = ${bed} GROUP BY time($interval)`,
      window,
      `Vital signs for ${selected.id}`,
    ),
    db.query(
      bucket,
      `SELECT time, hr, spo2, rr, sbp, temp_c FROM vitals WHERE $timeFilter AND bed = ${bed} AND source = 'manual'`,
      window,
      `Manual observations for ${selected.id}`,
    ),
    db.query(
      bucket,
      `SELECT COUNT(hr) FROM vitals WHERE $timeFilter AND ${ALERT_CONDITION} GROUP BY time($interval), bed`,
      { ...window, interval: alertInterval },
      "Minutes out of range",
    ),
    db.query(bucket, "SELECT COUNT(hr) FROM vitals WHERE $timeFilter", window, "Observations stored"),
  ]);
  for (const result of [latest, trend, detail, manual, alerts, count]) recorder.query(result);

  const latestByBed = new Map(latest.rows.map((row) => [str(row.bed), row]));
  const beds: BedStatus[] = BEDS.map((b) => {
    const vitals = latestVitals(latestByBed.get(b.id));
    return {
      id: b.id,
      patient: b.patient,
      reason: b.reason,
      spo2Scale: b.spo2Scale,
      latest: vitals ? { ...vitals, at: report?.watermark ?? null } : null,
      score: vitals ? news2(vitals, b.spo2Scale) : null,
      hrTrend: trend.rows.filter((row) => str(row.bed) === b.id).map((row) => ({ t: nsToMs(row.time), v: num(row.mean_hr) })),
    };
  });

  const series = Object.fromEntries(
    VITAL_KEYS.map((key) => [
      key,
      detail.rows.map((row) => ({
        t: nsToMs(row.time),
        mean: num(row[`mean_${key}`]),
        lo: num(row[`min_${key}`]),
        hi: num(row[`max_${key}`]),
      })),
    ]),
  ) as VitalsResponse["selected"]["series"];

  return {
    window: { key: spec.key, from: window.from, to: window.to, interval: spec.interval, intervalMs: intervalMs(spec.interval) },
    timeZone: WARD_TIME_ZONE,
    beds,
    selected: {
      id: selected.id,
      series,
      manual: manual.rows.map((row) => ({
        t: nsToMs(row.time),
        hr: num(row.hr),
        spo2: num(row.spo2),
        rr: num(row.rr),
        sbp: num(row.sbp),
        temp_c: num(row.temp_c),
      })),
    },
    alerts: {
      interval: alertInterval,
      intervalMs: intervalMs(alertInterval),
      cells: alerts.rows.map((row) => ({ bed: str(row.bed), t: nsToMs(row.time), minutes: num(row.count_hr) ?? 0 })),
    },
    observations: num(count.rows[0]?.count_hr) ?? 0,
    flux: recorder.activity(),
  };
}
