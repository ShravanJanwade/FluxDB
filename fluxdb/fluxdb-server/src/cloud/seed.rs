//! Sample fleet telemetry for the shared showcase workspace and for new guest
//! sandboxes.
//!
//! The dataset is a small production-shaped fleet with a deliberate incident in
//! it: around 90 minutes before "now", one service's payment dependency starts
//! timing out, its p99 latency and error rate climb, and the database host it
//! depends on saturates. Everything in the demo — the dashboard, the monitors
//! and the example queries — exists so that a visitor can find that incident,
//! which is the job a time-series database is actually bought to do.
//!
//! Values are generated from a seeded PRNG so the same shape appears on every
//! deployment, and every measurement is explicitly synthetic: nothing here is
//! presented as a real production system.

use super::model::{Comparison, Dashboard, Monitor, MonitorState, Panel, PanelKind, Severity};
use super::{auth, now_ms, now_ns};
use fluxdb_core::storage::Database;
use fluxdb_core::{DataPoint, FieldValue, Fields, Point, SeriesKey};

/// Name of the bucket the showcase project stores its fleet telemetry in.
pub const DEMO_BUCKET: &str = "production";
/// Name of the private, writable bucket each guest workspace receives.
pub const SANDBOX_BUCKET: &str = "sandbox";

const MINUTE_NS: i64 = 60 * 1_000_000_000;
/// Samples written for the showcase fleet: six hours at one-minute resolution.
const FLEET_SAMPLES: i64 = 360;
/// Samples written for a guest sandbox: one hour at one-minute resolution.
const SANDBOX_SAMPLES: i64 = 60;
/// Points per engine write call.
const CHUNK: usize = 2_500;

/// Deterministic xorshift64*, so a given deployment always produces the same
/// demo data and screenshots stay reproducible.
struct Noise(u64);

impl Noise {
    fn new(seed: u64) -> Self {
        Self(seed | 1)
    }

    fn next_unit(&mut self) -> f64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        let scrambled = self.0.wrapping_mul(0x2545_F491_4F6C_DD1D);
        (scrambled >> 11) as f64 / (1u64 << 53) as f64
    }

    /// Symmetric jitter in `[-amplitude, amplitude]`.
    fn jitter(&mut self, amplitude: f64) -> f64 {
        (self.next_unit() * 2.0 - 1.0) * amplitude
    }
}

struct Host {
    name: &'static str,
    role: &'static str,
    region: &'static str,
    cpu_base: f64,
    memory_base: f64,
}

const HOSTS: &[Host] = &[
    Host {
        name: "api-01",
        role: "api",
        region: "us-east-1",
        cpu_base: 34.0,
        memory_base: 52.0,
    },
    Host {
        name: "api-02",
        role: "api",
        region: "us-east-1",
        cpu_base: 31.0,
        memory_base: 49.0,
    },
    Host {
        name: "api-03",
        role: "api",
        region: "eu-west-1",
        cpu_base: 28.0,
        memory_base: 47.0,
    },
    Host {
        name: "api-04",
        role: "api",
        region: "eu-west-1",
        cpu_base: 36.0,
        memory_base: 55.0,
    },
    Host {
        name: "worker-01",
        role: "worker",
        region: "us-east-1",
        cpu_base: 58.0,
        memory_base: 66.0,
    },
    Host {
        name: "worker-02",
        role: "worker",
        region: "us-west-2",
        cpu_base: 54.0,
        memory_base: 63.0,
    },
    Host {
        name: "db-01",
        role: "database",
        region: "us-east-1",
        cpu_base: 44.0,
        memory_base: 71.0,
    },
    Host {
        name: "cache-01",
        role: "cache",
        region: "us-east-1",
        cpu_base: 19.0,
        memory_base: 78.0,
    },
];

struct Service {
    name: &'static str,
    region: &'static str,
    /// Requests per minute at the middle of the window.
    traffic: f64,
    latency_p50: f64,
    error_rate: f64,
}

const SERVICES: &[Service] = &[
    Service {
        name: "checkout",
        region: "us-east-1",
        traffic: 2400.0,
        latency_p50: 38.0,
        error_rate: 0.004,
    },
    Service {
        name: "catalog",
        region: "us-east-1",
        traffic: 9100.0,
        latency_p50: 12.0,
        error_rate: 0.001,
    },
    Service {
        name: "search",
        region: "us-east-1",
        traffic: 5300.0,
        latency_p50: 27.0,
        error_rate: 0.002,
    },
    Service {
        name: "payments",
        region: "us-east-1",
        traffic: 1800.0,
        latency_p50: 44.0,
        error_rate: 0.003,
    },
    Service {
        name: "auth",
        region: "eu-west-1",
        traffic: 3600.0,
        latency_p50: 16.0,
        error_rate: 0.002,
    },
    Service {
        name: "notifications",
        region: "us-west-2",
        traffic: 900.0,
        latency_p50: 21.0,
        error_rate: 0.006,
    },
];

/// The incident occupies the window from 100 to 55 minutes before the newest
/// sample, which puts it inside every default dashboard range without sitting
/// on the right edge of the chart.
const INCIDENT_START_MINUTES_AGO: i64 = 100;
const INCIDENT_END_MINUTES_AGO: i64 = 55;

/// Incident intensity at `minutes_ago`: zero outside the window, ramping up to
/// one in the middle of it.
fn incident(minutes_ago: i64) -> f64 {
    if !(INCIDENT_END_MINUTES_AGO..=INCIDENT_START_MINUTES_AGO).contains(&minutes_ago) {
        return 0.0;
    }
    let span = (INCIDENT_START_MINUTES_AGO - INCIDENT_END_MINUTES_AGO) as f64;
    let position = (INCIDENT_START_MINUTES_AGO - minutes_ago) as f64 / span;
    // Fast onset, slow recovery, which is what a dependency timeout looks like.
    if position < 0.25 {
        position / 0.25
    } else {
        ((1.0 - position) / 0.75).max(0.0)
    }
}

/// Smooth daily load curve in `[0, 1]`, driven by wall-clock hour so the demo
/// looks plausible whenever it is seeded.
fn diurnal(timestamp_ns: i64) -> f64 {
    let seconds_of_day = (timestamp_ns / 1_000_000_000).rem_euclid(86_400) as f64;
    let phase = (seconds_of_day / 86_400.0) * std::f64::consts::TAU;
    // Peak in the afternoon, trough overnight.
    0.5 + 0.5 * (phase - 1.9).sin()
}

fn float(value: f64) -> FieldValue {
    FieldValue::Float((value * 100.0).round() / 100.0)
}

fn point(
    measurement: &str,
    tags: &[(&str, &str)],
    timestamp: i64,
    fields: &[(&str, FieldValue)],
) -> Point {
    let mut key = SeriesKey::new(measurement);
    for (name, value) in tags {
        key.tags.insert((*name).to_string(), (*value).to_string());
    }
    let mut set = Fields::new();
    for (name, value) in fields {
        set.insert(*name, value.clone());
    }
    Point::new(
        key,
        DataPoint {
            timestamp,
            fields: set,
        },
    )
}

fn write_chunks(db: &Database, points: Vec<Point>) -> anyhow::Result<usize> {
    let total = points.len();
    for chunk in points.chunks(CHUNK) {
        db.write(chunk)?;
    }
    db.flush()?;
    Ok(total)
}

/// Newest sample timestamp, aligned to the minute so buckets line up.
fn latest_sample() -> i64 {
    (now_ns() / MINUTE_NS) * MINUTE_NS
}

/// Generate the showcase fleet: host metrics, service request metrics and a
/// disk series, with the incident woven through.
pub fn seed_fleet(db: &Database) -> anyhow::Result<usize> {
    let latest = latest_sample();
    let mut points = Vec::with_capacity(9_000);

    for (index, host) in HOSTS.iter().enumerate() {
        let mut noise = Noise::new(0x51ED_0001 + index as u64);
        for sample in 0..FLEET_SAMPLES {
            let minutes_ago = FLEET_SAMPLES - 1 - sample;
            let timestamp = latest - minutes_ago * MINUTE_NS;
            let load = diurnal(timestamp);
            let surge = incident(minutes_ago);
            // Only the database host and the workers feel the incident.
            let incident_cpu = match host.role {
                "database" => surge * 46.0,
                "worker" => surge * 14.0,
                _ => surge * 4.0,
            };
            let usage =
                (host.cpu_base + load * 18.0 + incident_cpu + noise.jitter(3.2)).clamp(1.0, 99.4);
            let memory =
                (host.memory_base + load * 7.0 + surge * 6.0 + noise.jitter(1.6)).clamp(5.0, 98.0);
            let total_bytes = 34_359_738_368i64; // 32 GiB
            points.push(point(
                "cpu",
                &[
                    ("host", host.name),
                    ("role", host.role),
                    ("region", host.region),
                    ("env", "production"),
                ],
                timestamp,
                &[
                    ("usage", float(usage)),
                    ("load1", float(usage / 100.0 * 8.0 + noise.jitter(0.25))),
                ],
            ));
            points.push(point(
                "mem",
                &[
                    ("host", host.name),
                    ("role", host.role),
                    ("region", host.region),
                    ("env", "production"),
                ],
                timestamp,
                &[
                    ("used_percent", float(memory)),
                    (
                        "used_bytes",
                        FieldValue::Integer((total_bytes as f64 * memory / 100.0) as i64),
                    ),
                ],
            ));
            // Disk fills slowly, so five-minute resolution is enough and keeps
            // the demo's working set small.
            if minutes_ago % 5 == 0 {
                let used = (58.0
                    + index as f64 * 3.4
                    + (FLEET_SAMPLES - minutes_ago) as f64 * 0.004
                    + noise.jitter(0.4))
                .clamp(5.0, 97.0);
                points.push(point(
                    "disk",
                    &[("host", host.name), ("mount", "/"), ("env", "production")],
                    timestamp,
                    &[("used_percent", float(used))],
                ));
            }
        }
    }

    for (index, service) in SERVICES.iter().enumerate() {
        let mut noise = Noise::new(0x51ED_1000 + index as u64);
        let affected = service.name == "payments" || service.name == "checkout";
        for sample in 0..FLEET_SAMPLES {
            let minutes_ago = FLEET_SAMPLES - 1 - sample;
            let timestamp = latest - minutes_ago * MINUTE_NS;
            let load = diurnal(timestamp);
            let surge = if affected { incident(minutes_ago) } else { 0.0 };
            let requests =
                (service.traffic * (0.55 + 0.45 * load) * (1.0 + noise.jitter(0.06))).max(1.0);
            // During the incident the dependency times out: errors climb by
            // two orders of magnitude and tail latency blows out.
            let error_rate = service.error_rate * (1.0 + noise.jitter(0.3)) + surge * 0.085;
            let p50 = service.latency_p50 * (1.0 + load * 0.25) + surge * 40.0 + noise.jitter(1.2);
            let p95 = p50 * 2.6 + surge * 320.0 + noise.jitter(6.0);
            let p99 = p50 * 4.1 + surge * 900.0 + noise.jitter(14.0);
            points.push(point(
                "http_requests",
                &[
                    ("service", service.name),
                    ("region", service.region),
                    ("env", "production"),
                ],
                timestamp,
                &[
                    ("requests", FieldValue::Integer(requests as i64)),
                    (
                        "errors",
                        FieldValue::Integer((requests * error_rate).round() as i64),
                    ),
                    ("error_rate", float(error_rate * 100.0)),
                    ("latency_p50", float(p50.max(1.0))),
                    ("latency_p95", float(p95.max(1.0))),
                    ("latency_p99", float(p99.max(1.0))),
                ],
            ));
        }
    }

    write_chunks(db, points)
}

/// Generate a small private dataset for a guest sandbox: enough to make the
/// explorer, the query workspace and the charts meaningful, small enough that
/// hundreds of concurrent guests stay affordable.
pub fn seed_sandbox(db: &Database) -> anyhow::Result<usize> {
    let latest = latest_sample();
    let mut points = Vec::with_capacity(400);
    for (index, host) in HOSTS.iter().take(2).enumerate() {
        let mut noise = Noise::new(0x5A0D_0001u64.wrapping_add(index as u64));
        for sample in 0..SANDBOX_SAMPLES {
            let minutes_ago = SANDBOX_SAMPLES - 1 - sample;
            let timestamp = latest - minutes_ago * MINUTE_NS;
            let load = diurnal(timestamp);
            points.push(point(
                "cpu",
                &[("host", host.name), ("env", "sandbox")],
                timestamp,
                &[(
                    "usage",
                    float((host.cpu_base + load * 16.0 + noise.jitter(4.0)).clamp(1.0, 99.0)),
                )],
            ));
            points.push(point(
                "mem",
                &[("host", host.name), ("env", "sandbox")],
                timestamp,
                &[(
                    "used_percent",
                    float((host.memory_base + load * 6.0 + noise.jitter(2.0)).clamp(5.0, 97.0)),
                )],
            ));
        }
    }
    for (index, service) in SERVICES.iter().take(2).enumerate() {
        let mut noise = Noise::new(0x5A0D_1000u64.wrapping_add(index as u64));
        for sample in 0..SANDBOX_SAMPLES {
            let minutes_ago = SANDBOX_SAMPLES - 1 - sample;
            let timestamp = latest - minutes_ago * MINUTE_NS;
            let requests = (service.traffic / 6.0 * (1.0 + noise.jitter(0.1))).max(1.0);
            points.push(point(
                "http_requests",
                &[("service", service.name), ("env", "sandbox")],
                timestamp,
                &[
                    ("requests", FieldValue::Integer(requests as i64)),
                    (
                        "errors",
                        FieldValue::Integer((requests * service.error_rate) as i64),
                    ),
                    (
                        "latency_p95",
                        float(service.latency_p50 * 2.4 + noise.jitter(3.0)),
                    ),
                ],
            ));
        }
    }
    write_chunks(db, points)
}

/// The dashboard that ships with the showcase project. Queries use the
/// `$timeFilter` and `$interval` macros the query API substitutes, so the same
/// panel works for any selected range.
pub fn demo_dashboard(project_id: &str, bucket_id: &str) -> Dashboard {
    let panel = |title: &str, kind: PanelKind, query: &str, unit: &str, span: u8| Panel {
        id: format!("pnl_{}", auth::random_id(10)),
        title: title.to_string(),
        kind,
        bucket_id: bucket_id.to_string(),
        query: query.to_string(),
        unit: unit.to_string(),
        span,
    };
    Dashboard {
        id: format!("dsh_{}", auth::random_id(12)),
        project_id: project_id.to_string(),
        name: "Fleet health".into(),
        panels: vec![
            panel(
                "Requests per minute",
                PanelKind::Stat,
                "SELECT MEAN(requests) AS requests FROM http_requests WHERE $timeFilter",
                "rpm",
                3,
            ),
            panel(
                "Error rate",
                PanelKind::Stat,
                "SELECT MEAN(error_rate) AS error_rate FROM http_requests WHERE $timeFilter",
                "%",
                3,
            ),
            panel(
                "p99 latency",
                PanelKind::Stat,
                "SELECT MAX(latency_p99) AS p99 FROM http_requests WHERE $timeFilter",
                "ms",
                3,
            ),
            panel(
                "Busiest host CPU",
                PanelKind::Stat,
                "SELECT MAX(usage) AS cpu FROM cpu WHERE $timeFilter",
                "%",
                3,
            ),
            panel(
                "p99 latency by service",
                PanelKind::Line,
                "SELECT MAX(latency_p99) AS p99 FROM http_requests WHERE $timeFilter GROUP BY time($interval), service",
                "ms",
                8,
            ),
            panel(
                "Errors by service",
                PanelKind::Bar,
                "SELECT SUM(errors) AS errors FROM http_requests WHERE $timeFilter GROUP BY service",
                "",
                4,
            ),
            panel(
                "CPU by host",
                PanelKind::Area,
                "SELECT MEAN(usage) AS cpu FROM cpu WHERE $timeFilter GROUP BY time($interval), host",
                "%",
                8,
            ),
            panel(
                "Memory pressure",
                PanelKind::Line,
                "SELECT MAX(used_percent) AS memory FROM mem WHERE $timeFilter GROUP BY time($interval), role",
                "%",
                4,
            ),
        ],
        created_by: "system".into(),
        created_at: now_ms(),
        updated_at: now_ms(),
    }
}

/// Monitors that ship with the showcase project. Their thresholds are set so
/// the seeded incident actually trips them, which is how a visitor sees the
/// alert feed populate rather than an empty state.
pub fn demo_monitors(project_id: &str, bucket_id: &str) -> Vec<Monitor> {
    let monitor =
        |name: &str, query: &str, comparison: Comparison, threshold: f64, severity: Severity| {
            Monitor {
                id: format!("mon_{}", auth::random_id(12)),
                project_id: project_id.to_string(),
                bucket_id: bucket_id.to_string(),
                name: name.to_string(),
                query: query.to_string(),
                comparison,
                threshold,
                severity,
                enabled: true,
                state: MonitorState::Unknown,
                last_value: None,
                last_checked_at: None,
                last_error: None,
                created_by: "system".into(),
                created_at: now_ms(),
            }
        };
    vec![
        monitor(
            "Tail latency budget",
            "SELECT MAX(latency_p99) AS p99 FROM http_requests",
            Comparison::Above,
            750.0,
            Severity::Critical,
        ),
        monitor(
            "Error rate SLO",
            "SELECT MEAN(error_rate) AS error_rate FROM http_requests",
            Comparison::Above,
            1.0,
            Severity::Critical,
        ),
        monitor(
            "Host CPU saturation",
            "SELECT MAX(usage) AS cpu FROM cpu",
            Comparison::Above,
            90.0,
            Severity::Warning,
        ),
        monitor(
            "Disk headroom",
            "SELECT MAX(used_percent) AS disk FROM disk",
            Comparison::Above,
            85.0,
            Severity::Warning,
        ),
    ]
}

/// Worked examples offered in the query workspace. Each one answers a question
/// an on-call engineer would actually ask of this dataset.
pub fn example_queries() -> Vec<(&'static str, &'static str)> {
    vec![
        (
            "Which service broke?",
            "SELECT MAX(latency_p99) AS p99, SUM(errors) AS errors FROM http_requests WHERE $timeFilter GROUP BY service ORDER BY p99 DESC",
        ),
        (
            "Latency over time, one series per service",
            "SELECT MAX(latency_p99) AS p99 FROM http_requests WHERE $timeFilter GROUP BY time($interval), service",
        ),
        (
            "Was it the database host?",
            "SELECT MEAN(usage) AS cpu FROM cpu WHERE host = 'db-01' AND $timeFilter GROUP BY time($interval)",
        ),
        (
            "Hosts above 90% CPU",
            "SELECT MAX(usage) AS peak FROM cpu WHERE usage > 90 AND $timeFilter GROUP BY host",
        ),
        (
            "Traffic mix by region",
            "SELECT SUM(requests) AS requests FROM http_requests WHERE $timeFilter GROUP BY region",
        ),
        (
            "Newest raw points",
            "SELECT * FROM http_requests WHERE $timeFilter ORDER BY time DESC LIMIT 50",
        ),
        (
            "Memory headroom by role",
            "SELECT MAX(used_percent) AS memory FROM mem WHERE $timeFilter GROUP BY role",
        ),
        (
            "Error budget burn per minute",
            "SELECT SUM(errors) AS errors, SUM(requests) AS requests FROM http_requests WHERE $timeFilter GROUP BY time($interval)",
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluxdb_core::storage::{StorageConfig, StorageEngine};

    fn database(name: &str) -> (tempfile::TempDir, std::sync::Arc<Database>) {
        let dir = tempfile::tempdir().unwrap();
        let engine = StorageEngine::new(StorageConfig {
            data_dir: dir.path().into(),
            ..Default::default()
        })
        .unwrap();
        let db = engine.create_database(name).unwrap();
        (dir, db)
    }

    #[test]
    fn incident_intensity_is_confined_to_its_window() {
        assert_eq!(incident(INCIDENT_START_MINUTES_AGO + 1), 0.0);
        assert_eq!(incident(INCIDENT_END_MINUTES_AGO - 1), 0.0);
        assert_eq!(incident(0), 0.0);
        let peak = incident(INCIDENT_START_MINUTES_AGO - 11);
        assert!(peak > 0.9, "onset should reach full intensity, got {peak}");
        // Recovery is monotonic after the peak.
        assert!(incident(70) > incident(60));
    }

    #[test]
    fn seeded_fleet_is_queryable_and_contains_the_incident() {
        let (_dir, db) = database(DEMO_BUCKET);
        let written = seed_fleet(&db).unwrap();
        assert!(
            written > 6_000,
            "expected a substantial fleet, got {written}"
        );
        assert_eq!(db.stats().total_entries, written);

        // Every dashboard panel and monitor query must parse and run against
        // the seeded shape, so the demo never opens onto a broken panel.
        for (_, query) in example_queries() {
            let concrete = query
                .replace("$timeFilter", "time > 0")
                .replace("$interval", "'5m'");
            crate::api::data::run_sql(&db, &concrete)
                .unwrap_or_else(|e| panic!("example query failed: {query}\n{e}"));
        }
        for panel in demo_dashboard("proj", "bkt").panels {
            let concrete = panel
                .query
                .replace("$timeFilter", "time > 0")
                .replace("$interval", "'5m'");
            crate::api::data::run_sql(&db, &concrete)
                .unwrap_or_else(|e| panic!("panel {} failed: {e}", panel.title));
        }

        // The incident is visible: tail latency crosses the monitor threshold
        // that ships enabled.
        let peak = crate::api::data::scalar(&db, "SELECT MAX(latency_p99) FROM http_requests")
            .unwrap()
            .unwrap();
        assert!(
            peak > 750.0,
            "incident should trip the latency monitor, peak {peak}"
        );
        let quiet = crate::api::data::scalar(
            &db,
            "SELECT MAX(latency_p99) FROM http_requests WHERE service = 'catalog'",
        )
        .unwrap()
        .unwrap();
        assert!(
            quiet < 200.0,
            "unaffected services stay healthy, got {quiet}"
        );
    }

    #[test]
    fn every_demo_monitor_query_evaluates_against_the_seeded_data() {
        let (_dir, db) = database(DEMO_BUCKET);
        seed_fleet(&db).unwrap();
        for monitor in demo_monitors("proj", "bkt") {
            let value = crate::api::data::scalar(&db, &monitor.query)
                .unwrap_or_else(|e| panic!("monitor {} failed: {e}", monitor.name))
                .unwrap_or_else(|| panic!("monitor {} returned no rows", monitor.name));
            assert!(value.is_finite());
        }
    }

    #[test]
    fn sandbox_seed_is_small_but_usable() {
        let (_dir, db) = database(SANDBOX_BUCKET);
        let written = seed_sandbox(&db).unwrap();
        assert!(
            (200..1_000).contains(&written),
            "sandbox should stay small, got {written}"
        );
        let schema = crate::api::data::schema_json(&db).unwrap();
        for measurement in ["cpu", "mem", "http_requests"] {
            assert!(
                schema["measurements"][measurement].is_object(),
                "sandbox is missing {measurement}"
            );
        }
    }

    #[test]
    fn generated_values_are_deterministic() {
        let mut first = Noise::new(7);
        let mut second = Noise::new(7);
        let sequence: Vec<f64> = (0..8).map(|_| first.next_unit()).collect();
        let repeat: Vec<f64> = (0..8).map(|_| second.next_unit()).collect();
        assert_eq!(sequence, repeat);
        assert!(sequence.iter().all(|v| (0.0..1.0).contains(v)));
        // A different seed must not produce the same stream.
        assert_ne!(sequence[0], Noise::new(8).next_unit());
    }
}
