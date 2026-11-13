"use client";

import { useEffect, useRef, useState } from "react";
import { Heatmap } from "../../components/charts/Heatmap";
import { LineChart } from "../../components/charts/LineChart";
import { Sparkline } from "../../components/charts/Sparkline";
import { FluxPanel } from "../../components/FluxPanel";
import { LiveStatus, LoadState, RiskBadge, riskColor, Segmented, StatTile } from "../../components/ui";
import { useLiveData } from "../../components/useLiveData";
import { clock, fixed, milliseconds, whole } from "../../lib/format";
import type { BedStatus, VitalsResponse } from "../../lib/types";
import { BEDS, OBSERVATION_LIMITS, type News2 } from "../../lib/vitals";

type WindowKey = "1h" | "6h" | "24h";
type VitalKey = "hr" | "spo2" | "rr" | "sbp" | "temp_c";

const WINDOWS: { value: WindowKey; label: string }[] = [
  { value: "1h", label: "1H" },
  { value: "6h", label: "6H" },
  { value: "24h", label: "24H" },
];

/** Each vital's NEWS2 zero-score range, shaded behind its chart. */
const VITALS: { key: VitalKey; label: string; unit: string; digits: number; normal: (scale: 1 | 2) => [number, number] }[] = [
  { key: "hr", label: "Heart rate", unit: "bpm", digits: 0, normal: () => [51, 90] },
  { key: "spo2", label: "SpO₂", unit: "%", digits: 0, normal: (scale) => (scale === 2 ? [88, 92] : [96, 100]) },
  { key: "rr", label: "Respiratory rate", unit: "/min", digits: 0, normal: () => [12, 20] },
  { key: "sbp", label: "Systolic blood pressure", unit: "mmHg", digits: 0, normal: () => [111, 219] },
  { key: "temp_c", label: "Temperature", unit: "°C", digits: 1, normal: () => [36.1, 38.0] },
];

const CODE = `// app/api/vitals/observations/route.ts, abridged: a nurse records observations by hand
const db = new FluxDB({ url: process.env.FLUXDB_URL, apiKey: process.env.FLUXDB_API_KEY });

await db.write("vitals", [{
  measurement: "vitals",
  tags: { bed: "B3", source: "manual" },
  fields: { hr: 96n, spo2: 91n, rr: 22n, sbp: 118n, temp_c: 38.2, on_o2: true, acvpu: "A" },
  time: Date.now(),
}]);

// The ward board: the newest value of every vital, per bed, in one query
const latest = await db.query(
  "vitals",
  "SELECT LAST(hr), LAST(spo2), LAST(rr), LAST(sbp), LAST(temp_c), LAST(on_o2), LAST(acvpu) FROM vitals WHERE $timeFilter GROUP BY bed",
  { from: Date.now() - 15 * MINUTE, to: Date.now() },
);`;

function ScoreBreakdown({ bed }: { bed: BedStatus }) {
  if (!bed.latest || !bed.score) return <p className="form-note">No recent observations.</p>;
  const v = bed.latest;
  const rows: [string, string, number][] = [
    ["Respiratory rate", `${v.rr} /min`, bed.score.parts.rr],
    [`SpO₂ (scale ${bed.spo2Scale})`, `${v.spo2}%`, bed.score.parts.spo2],
    ["Air or oxygen", v.on_o2 ? "Oxygen" : "Air", bed.score.parts.air],
    ["Systolic BP", `${v.sbp} mmHg`, bed.score.parts.sbp],
    ["Pulse", `${v.hr} bpm`, bed.score.parts.hr],
    ["Consciousness", v.acvpu === "A" ? "Alert" : `ACVPU ${v.acvpu}`, bed.score.parts.acvpu],
    ["Temperature", `${fixed(v.temp_c, 1)} °C`, bed.score.parts.temp],
  ];
  return (
    <table className="data">
      <thead>
        <tr>
          <th>NEWS2 parameter</th>
          <th className="num">Latest</th>
          <th className="num">Points</th>
        </tr>
      </thead>
      <tbody>
        {rows.map(([name, value, points]) => (
          <tr key={name}>
            <td>{name}</td>
            <td className="num">{value}</td>
            <td className="num" style={{ fontWeight: points === 3 ? 700 : undefined }}>
              {points}
            </td>
          </tr>
        ))}
        <tr>
          <td>
            <strong>Total</strong>
          </td>
          <td className="num">
            <RiskBadge risk={bed.score.risk} />
          </td>
          <td className="num">
            <strong>{bed.score.total}</strong>
          </td>
        </tr>
      </tbody>
    </table>
  );
}

interface FormValues {
  hr: string;
  spo2: string;
  rr: string;
  sbp: string;
  temp_c: string;
  on_o2: boolean;
  acvpu: string;
}

function ObservationForm({ bed, onSaved }: { bed: BedStatus; onSaved: () => void }) {
  const latest = bed.latest;
  const [values, setValues] = useState<FormValues>({
    hr: latest ? String(latest.hr) : "",
    spo2: latest ? String(latest.spo2) : "",
    rr: latest ? String(latest.rr) : "",
    sbp: latest ? String(latest.sbp) : "",
    temp_c: latest ? fixed(latest.temp_c, 1) : "",
    on_o2: latest?.on_o2 ?? false,
    acvpu: "A",
  });
  const [busy, setBusy] = useState(false);
  const [result, setResult] = useState<{ line: string; writeMs: number; score: News2 } | null>(null);
  const [problem, setProblem] = useState<string | null>(null);
  const set = (key: keyof FormValues) => (event: React.ChangeEvent<HTMLInputElement | HTMLSelectElement>) =>
    setValues((current) => ({
      ...current,
      [key]: event.target instanceof HTMLInputElement && event.target.type === "checkbox" ? event.target.checked : event.target.value,
    }));

  async function submit(event: React.FormEvent) {
    event.preventDefault();
    setBusy(true);
    setProblem(null);
    setResult(null);
    try {
      const response = await fetch("/api/vitals/observations", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          bed: bed.id,
          hr: Number(values.hr),
          spo2: Number(values.spo2),
          rr: Number(values.rr),
          sbp: Number(values.sbp),
          temp_c: Number(values.temp_c),
          on_o2: values.on_o2,
          acvpu: values.acvpu,
        }),
      });
      const body = await response.json().catch(() => null);
      if (!response.ok) setProblem(body?.message ?? `The server answered HTTP ${response.status}`);
      else {
        setResult(body);
        onSaved();
      }
    } catch {
      setProblem("Could not reach the server");
    } finally {
      setBusy(false);
    }
  }

  const field = (key: Exclude<VitalKey, never>, label: string, step = "1") => (
    <label className="field">
      {label}
      <input
        type="number"
        inputMode="decimal"
        required
        step={step}
        min={OBSERVATION_LIMITS[key][0]}
        max={OBSERVATION_LIMITS[key][1]}
        value={values[key]}
        onChange={set(key)}
      />
    </label>
  );

  return (
    <form onSubmit={submit}>
      <div className="form-grid">
        {field("hr", "Pulse (bpm)")}
        {field("spo2", "SpO₂ (%)")}
        {field("rr", "Resp. rate (/min)")}
        {field("sbp", "Systolic BP (mmHg)")}
        {field("temp_c", "Temp (°C)", "0.1")}
        <label className="field">
          Consciousness
          <select value={values.acvpu} onChange={set("acvpu")}>
            <option value="A">Alert</option>
            <option value="C">New confusion</option>
            <option value="V">Voice</option>
            <option value="P">Pain</option>
            <option value="U">Unresponsive</option>
          </select>
        </label>
      </div>
      <div className="form-actions">
        <label className="check">
          <input type="checkbox" checked={values.on_o2} onChange={set("on_o2")} /> On supplemental oxygen
        </label>
        <button className="button" type="submit" disabled={busy}>
          {busy ? "Saving…" : `Record for ${bed.id}`}
        </button>
        {result && (
          <span className="form-note" role="status">
            Saved to FluxDB in {milliseconds(result.writeMs)}. NEWS2 {result.score.total} ({result.score.risk.replace("-", "–")}).
          </span>
        )}
        {problem && (
          <span className="form-note" role="alert" style={{ color: "var(--down-ink)" }}>
            {problem}
          </span>
        )}
      </div>
      {result && (
        <pre className="lines" style={{ marginTop: 10 }} aria-label="Line protocol written">
          {result.line}
        </pre>
      )}
    </form>
  );
}

export function WardDashboard() {
  const [windowKey, setWindowKey] = useState<WindowKey>("6h");
  const [bedId, setBedId] = useState("B1");
  const picked = useRef(false);
  const live = useLiveData<VitalsResponse>(`/api/vitals?window=${windowKey}&bed=${bedId}`, 30_000);
  const data = live.data;

  // Open on the patient who most needs attention, once.
  useEffect(() => {
    if (picked.current || !data) return;
    picked.current = true;
    const worst = [...data.beds].sort((a, b) => (b.score?.total ?? -1) - (a.score?.total ?? -1))[0];
    if (worst?.score && worst.score.total >= 5 && worst.id !== bedId) setBedId(worst.id);
  }, [data, bedId]);

  const tz = data?.timeZone;
  const domain: [number, number] = data ? [data.window.from, data.window.to] : [0, 1];
  const selected = data?.beds.find((b) => b.id === (data.selected.id ?? bedId));
  const urgent = data?.beds.filter((b) => (b.score?.total ?? 0) >= 5).length ?? 0;
  const highest = data ? [...data.beds].sort((a, b) => (b.score?.total ?? -1) - (a.score?.total ?? -1))[0] : null;

  return (
    <>
      <div className="dash-head">
        <div>
          <p className="eyebrow">Healthcare · remote patient monitoring</p>
          <h1>Ward 4B vitals board</h1>
          <p>
            Eight patients on a step-down unit, each on a bedside monitor that charts pulse, SpO₂, breathing, blood pressure and
            temperature every minute. The board scores everyone with NEWS2, the early-warning score UK hospitals use to decide
            who needs review. Patients and readings are simulated; this is not medical software.
          </p>
        </div>
      </div>

      <div className="filters">
        <Segmented label="Time range" options={WINDOWS} value={windowKey} onChange={setWindowKey} />
        <LiveStatus live={live} />
      </div>
      <LoadState live={live} />

      {data && (
        <>
          <div className="kpis">
            <StatTile label="Patients monitored" value={whole(data.beds.length)} sub="One reading a minute each" />
            <StatTile label="Need urgent review" value={whole(urgent)} sub="NEWS2 of 5 or more" />
            <StatTile
              label="Highest score"
              value={highest?.score ? whole(highest.score.total) : "–"}
              sub={highest ? `${highest.id} · ${highest.reason}` : undefined}
            />
            <StatTile label="Observations stored" value={whole(data.observations)} sub="In this range, one COUNT query" />
          </div>

          <div className="beds" role="group" aria-label="Beds">
            {data.beds.map((bed) => (
              <button
                key={bed.id}
                type="button"
                className="bed"
                aria-pressed={bed.id === data.selected.id}
                onClick={() => {
                  picked.current = true;
                  setBedId(bed.id);
                }}
              >
                {bed.score && <span className="stripe" style={{ background: riskColor(bed.score.risk) }} />}
                <div className="bed-top">
                  <span className="bed-id">{bed.id}</span>
                  {bed.score && <RiskBadge risk={bed.score.risk} />}
                </div>
                <div className="who">
                  {bed.patient} · {bed.reason}
                </div>
                <div className="bed-top">
                  <div className="score">
                    <b>{bed.score?.total ?? "–"}</b>
                    <span>NEWS2</span>
                  </div>
                  <Sparkline points={bed.hrTrend} label={`Heart rate trend for ${bed.id}`} />
                </div>
                {bed.latest && (
                  <div className="vitals-row">
                    <span>
                      HR<b>{bed.latest.hr}</b>
                    </span>
                    <span>
                      SpO₂<b>{bed.latest.spo2}</b>
                    </span>
                    <span>
                      RR<b>{bed.latest.rr}</b>
                    </span>
                    <span>
                      BP<b>{bed.latest.sbp}</b>
                    </span>
                    <span>
                      T<b>{fixed(bed.latest.temp_c, 1)}</b>
                    </span>
                  </div>
                )}
              </button>
            ))}
          </div>

          {selected && (
            <section className="card" aria-label={`Bed ${selected.id}`}>
              <div className="card-head">
                <div>
                  <h2>
                    {selected.id} · {selected.patient} · {selected.reason}
                  </h2>
                  <p>
                    {selected.score?.response}. Line: mean per {data.window.interval}; wash: lowest to highest reading; grey band:
                    the range that scores 0; rings: observations recorded by hand.
                  </p>
                </div>
              </div>
              <div className="small-multiples">
                  {VITALS.map((vital) => {
                    const points = data.selected.series[vital.key];
                    const [lo, hi] = vital.normal(selected.spo2Scale);
                    return (
                      <div key={vital.key}>
                        <h3>{vital.label}</h3>
                        <div className="range-note">
                          Scores 0 between {lo} and {hi} {vital.unit}
                        </div>
                        <LineChart
                          series={[
                            {
                              id: vital.key,
                              label: "Mean",
                              color: "var(--series-1)",
                              points: points.map((p) => ({ t: p.t, v: p.mean })),
                              band: points.map((p) => ({ t: p.t, lo: p.lo, hi: p.hi })),
                            },
                          ]}
                          markers={data.selected.manual.flatMap((m) => (m[vital.key] === null ? [] : [{ t: m.t, v: m[vital.key]! }]))}
                          markerLabel="Recorded by hand"
                          refBand={{ lo, hi }}
                          domain={domain}
                          stepMs={data.window.intervalMs}
                          format={(v) => `${fixed(v, vital.digits)} ${vital.unit}`}
                          axisFormat={(v) => fixed(v, vital.key === "temp_c" ? 1 : 0)}
                          timeZone={tz}
                          height={150}
                          refreshing={live.refreshing}
                          label={`${vital.label} for ${selected.id} over the selected range`}
                        />
                      </div>
                    );
                  })}
                  <div>
                    <h3>How the score adds up</h3>
                    <div className="range-note">From the newest reading of each vital</div>
                    <ScoreBreakdown bed={selected} />
                  </div>
              </div>
              <div className="flux-step" style={{ marginTop: 16 }}>
                <h3 style={{ fontSize: 14, marginBottom: 4 }}>Record observations for {selected.id}</h3>
                <p className="form-note" style={{ margin: "0 0 10px" }}>
                  Writes one point to FluxDB with <code>source=manual</code>; it appears as a ring on the charts above. Pre-filled
                  with the monitor&apos;s latest values.
                </p>
                <ObservationForm key={selected.id} bed={selected} onSaved={() => live.refresh(true)} />
              </div>
            </section>
          )}

          <section className="card" style={{ marginTop: 16 }} aria-label="Out-of-range minutes">
            <div className="card-head">
              <div>
                <h2>Out-of-range minutes, whole ward</h2>
                <p>
                  Minutes per {data.alerts.interval} with pulse over 110, breathing over 24, systolic BP at or under 100 or a
                  temperature of 38.1 °C or more: one <code>COUNT … WHERE … GROUP BY time, bed</code> query.
                </p>
              </div>
            </div>
            <Heatmap
              rows={BEDS.map((b) => ({ id: b.id, label: b.id }))}
              cells={data.alerts.cells.map((c) => ({ row: c.bed, t: c.t, value: c.minutes }))}
              domain={domain}
              bucketMs={data.alerts.intervalMs}
              max={Math.round(data.alerts.intervalMs / 60_000)}
              legend={`Minutes out of range per ${data.alerts.interval}`}
              describe={(bed, t, value) =>
                `${bed}, ${clock(t, tz)}–${clock(t + data.alerts.intervalMs, tz)}: ${value} minute${value === 1 ? "" : "s"} out of range`
              }
              timeZone={tz}
              label="Heatmap of minutes with out-of-range vital signs per bed over time"
            />
          </section>
        </>
      )}

      <FluxPanel
        activity={data?.flux ?? null}
        code={CODE}
        intro={
          <p style={{ margin: 0 }}>
            Each minute is one point per bed in the <code>vitals</code> measurement, tagged with the bed and where it came from
            (<code>monitor</code> or <code>manual</code>). Integers stay integers (<code>hr=88i</code>), oxygen is a boolean and
            consciousness a string. The board is a single <code>LAST() … GROUP BY bed</code> query; NEWS2 is then scored on the
            server from those values.
          </p>
        }
      />
    </>
  );
}
