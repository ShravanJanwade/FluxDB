/**
 * Smart-building energy: a fictional four-building campus near Boston. Each
 * building's submeter reports a 5-minute reading split by end use, and the
 * campus main meter reports the totals plus the weather station's outdoor
 * temperature. Loads follow occupancy, daylight and weather the way real ones
 * do, and the lab's exhaust fan sometimes sticks on for a couple of hours.
 */
import type { Point } from "./lineProtocol";
import { bump, clamp, drift, gaussian, hashString, random01, round, smoothNoise, smoothstep } from "./noise";
import { DAY, HOUR, MINUTE, zoned, type ZonedTime } from "./time";

export const ENERGY_TIME_ZONE = "America/New_York";
export const ENERGY_STEP = 5 * MINUTE;

export type BuildingKind = "office" | "lab" | "residence" | "garage";

export interface Building {
  id: string;
  name: string;
  kind: BuildingKind;
  /** Rooftop or canopy solar, peak kW. */
  solarKwp: number;
}

export const BUILDINGS: Building[] = [
  { id: "admin", name: "Admin & offices", kind: "office", solarKwp: 60 },
  { id: "lab", name: "Science lab", kind: "lab", solarKwp: 0 },
  { id: "residence", name: "Residence hall", kind: "residence", solarKwp: 0 },
  { id: "garage", name: "Garage & EV hub", kind: "garage", solarKwp: 180 },
];

export const END_USES = [
  { id: "hvac_kw", label: "Heating & cooling" },
  { id: "lighting_kw", label: "Lighting" },
  { id: "plug_kw", label: "Plug loads" },
  { id: "ev_kw", label: "EV charging" },
] as const;

/** Time-of-use tariff, $/kWh: weekdays noon to 8 pm are peak. */
export const TARIFF = { peak: 0.28, offPeak: 0.14 };

export function isPeakHour(t: number): boolean {
  const z = zoned(t, ENERGY_TIME_ZONE);
  return z.weekday >= 1 && z.weekday <= 5 && z.hour >= 12 && z.hour < 20;
}

interface Profile {
  /** Always-on load: servers, freezers, fume hoods. */
  base: number;
  plugPeak: number;
  lightPeak: number;
  /** kW per degree outside the 16-22 °C comfort band. */
  hvacPerDegree: number;
  /** Air handlers. */
  fans: number;
}

const PROFILES: Record<BuildingKind, Profile> = {
  office: { base: 30, plugPeak: 70, lightPeak: 50, hvacPerDegree: 6.5, fans: 22 },
  lab: { base: 115, plugPeak: 55, lightPeak: 32, hvacPerDegree: 9.5, fans: 48 },
  residence: { base: 22, plugPeak: 60, lightPeak: 30, hvacPerDegree: 5.2, fans: 10 },
  garage: { base: 5, plugPeak: 4, lightPeak: 26, hvacPerDegree: 0, fans: 0 },
};

const WEATHER_SEED = hashString("campus-weather");
const CLOUD_SEED = hashString("campus-clouds");
const FAULT_SEED = hashString("lab-exhaust-fault");

/** Outdoor temperature, °C: season, time of day and passing weather systems. */
export function outdoorTemperature(t: number, z: ZonedTime = zoned(t, ENERGY_TIME_ZONE)): number {
  const seasonal = 10.5 - 12.5 * Math.cos((2 * Math.PI * (z.dayOfYear - 20)) / 365);
  const daily = 4.5 * Math.sin((2 * Math.PI * (z.hour - 9)) / 24);
  const weather = 4 * drift(WEATHER_SEED, t, 2 * DAY) + 1.2 * drift(WEATHER_SEED + 1, t, 6 * HOUR);
  return seasonal + daily + weather;
}

/** 0 for a clear sky, 1 for overcast. */
function cloudCover(t: number): number {
  return clamp(0.45 + 0.6 * drift(CLOUD_SEED, t, 1.5 * DAY) + 0.3 * smoothNoise(CLOUD_SEED + 1, t, 40 * MINUTE), 0, 1);
}

/** Share of rated solar output under a clear sky, 0 to about 0.85. */
function clearSky(t: number, z: ZonedTime): number {
  const season = Math.sin((2 * Math.PI * (z.dayOfYear - 80)) / 365);
  const dayLength = 12 + 3.1 * season;
  // Solar noon at 71° W is close to 16:45 UTC all year.
  const utcHour = (((t % DAY) + DAY) % DAY) / HOUR;
  const x = (utcHour - 16.75) / (dayLength / 2);
  if (Math.abs(x) >= 1) return 0;
  return Math.pow(Math.cos((x * Math.PI) / 2), 1.3) * (0.68 + 0.17 * season);
}

function occupancy(kind: BuildingKind, z: ZonedTime): number {
  const weekend = z.weekday === 0 || z.weekday === 6;
  const h = z.hour;
  switch (kind) {
    case "office": {
      const day = smoothstep(6.5, 9, h) * (1 - smoothstep(17, 19.5, h));
      const lunch = 1 - 0.18 * bump(h, 12.5, 0.6);
      return weekend ? 0.05 + 0.1 * day : 0.04 + 0.9 * day * lunch;
    }
    case "lab": {
      const day = smoothstep(7.5, 9.5, h) * (1 - smoothstep(18, 21.5, h));
      return weekend ? 0.1 + 0.25 * day : 0.12 + 0.78 * day;
    }
    case "residence": {
      const away = smoothstep(8, 10, h) * (1 - smoothstep(15.5, 18.5, h));
      const evening = bump(h, 21, 2.4);
      return weekend ? 0.5 + 0.35 * evening : 0.35 * (1 - away) + 0.15 + 0.45 * evening;
    }
    case "garage":
      return weekend ? 0.15 : 0.1 + 0.8 * smoothstep(7, 9, h) * (1 - smoothstep(16.5, 19, h));
  }
}

/** The lab's exhaust fan sticks at full speed for two hours on some weekdays. */
export function labFault(t: number, z: ZonedTime): number {
  const day = Math.floor((t + z.offset) / DAY);
  if (random01(FAULT_SEED, day) > 0.45) return 0;
  const start = 9 + 7 * random01(FAULT_SEED + 1, day);
  return smoothstep(start, start + 0.25, z.hour) * (1 - smoothstep(start + 2, start + 2.25, z.hour));
}

export interface Reading {
  hvac_kw: number;
  lighting_kw: number;
  plug_kw: number;
  ev_kw: number;
  solar_kw: number;
  /** Drawn from the grid; negative when solar exceeds the load. */
  net_kw: number;
  /** Net energy over the 5-minute interval. */
  kwh: number;
}

export function buildingReading(building: Building, t: number, z: ZonedTime = zoned(t, ENERGY_TIME_ZONE)): Reading {
  const seed = hashString(`building:${building.id}`);
  const profile = PROFILES[building.kind];
  const step = Math.floor(t / ENERGY_STEP);
  const occ = occupancy(building.kind, z);
  const temperature = outdoorTemperature(t, z);
  const sky = clearSky(t, z);
  const sun = sky * (1 - 0.75 * cloudCover(t));
  const wobble = (salt: number, size: number) => 1 + size * gaussian(seed + salt, step);

  const heating = Math.max(0, 16 - temperature) * (0.55 + 0.45 * occ);
  const cooling = Math.max(0, temperature - 22) * 1.3 * (0.5 + 0.5 * occ);
  let hvac = (profile.fans * (0.4 + 0.6 * occ) + profile.hvacPerDegree * (heating + cooling)) * wobble(1, 0.05);
  if (building.kind === "lab") hvac += 58 * labFault(t, z);

  const lighting =
    building.kind === "garage"
      ? profile.lightPeak * (0.3 + 0.7 * (1 - Math.min(1, sky * 4)))
      : profile.lightPeak * (0.12 + 0.88 * occ) * (1 - 0.35 * Math.min(1, sun * 1.6));

  const plug = (profile.base + profile.plugPeak * occ) * (1 + 0.06 * drift(seed + 2, t, 3 * HOUR)) * wobble(3, 0.03);

  let ev = 0;
  if (building.kind === "garage") {
    const weekday = z.weekday >= 1 && z.weekday <= 5;
    const sessions = 0.85 * bump(z.hour, 9.6, 1.7) + 0.35 * bump(z.hour, 18.2, 1.3) + 0.06;
    ev = Math.max(0, 132 * sessions * (weekday ? 1 : 0.3) * (1 + 0.25 * drift(seed + 4, t, 50 * MINUTE)));
  }

  const solar = building.solarKwp * sun * wobble(5, 0.02);
  const net = hvac + lighting + plug + ev - solar;
  return {
    hvac_kw: round(hvac, 1),
    lighting_kw: round(lighting, 1),
    plug_kw: round(plug, 1),
    ev_kw: round(ev, 1),
    solar_kw: round(Math.max(0, solar), 1),
    net_kw: round(net, 1),
    kwh: round((net * ENERGY_STEP) / HOUR, 3),
  };
}

/**
 * Every reading in [from, to]: one `power` point per building and one
 * `campus` point carrying the totals and the outdoor temperature.
 */
export function energyPoints(from: number, to: number): Point[] {
  const points: Point[] = [];
  for (let t = from; t <= to; t += ENERGY_STEP) {
    const z = zoned(t, ENERGY_TIME_ZONE);
    const campus: Reading = { hvac_kw: 0, lighting_kw: 0, plug_kw: 0, ev_kw: 0, solar_kw: 0, net_kw: 0, kwh: 0 };
    for (const building of BUILDINGS) {
      const reading = buildingReading(building, t, z);
      for (const key of Object.keys(campus) as (keyof Reading)[]) campus[key] += reading[key];
      points.push({ measurement: "power", tags: { building: building.id }, fields: { ...reading }, time: t });
    }
    points.push({
      measurement: "campus",
      fields: {
        hvac_kw: round(campus.hvac_kw, 1),
        lighting_kw: round(campus.lighting_kw, 1),
        plug_kw: round(campus.plug_kw, 1),
        ev_kw: round(campus.ev_kw, 1),
        solar_kw: round(campus.solar_kw, 1),
        net_kw: round(campus.net_kw, 1),
        kwh: round(campus.kwh, 3),
        outdoor_c: round(outdoorTemperature(t, z), 1),
      },
      time: t,
    });
  }
  return points;
}
