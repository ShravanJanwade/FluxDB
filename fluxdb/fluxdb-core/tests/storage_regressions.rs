use fluxdb_core::{
    storage::{StorageConfig, StorageEngine},
    DataPoint, FieldValue, Fields, Point, SeriesKey, TimeRange,
};
use std::collections::BTreeMap;
use tempfile::TempDir;

fn config(dir: &TempDir) -> StorageConfig {
    StorageConfig {
        data_dir: dir.path().to_path_buf(),
        memtable_size_limit: 1024 * 1024,
        ..Default::default()
    }
}
fn point(ts: i64, host: &str, value: f64) -> Point {
    Point::new(
        SeriesKey::new("cpu").with_tag("host", host),
        DataPoint::new(ts, "usage", FieldValue::Float(value)),
    )
}

#[test]
fn types_tags_updates_deletes_survive_flush_checkpoint_and_restart() {
    let dir = TempDir::new().unwrap();
    let cfg = config(&dir);
    let engine = StorageEngine::new(cfg.clone()).unwrap();
    let db = engine.create_database("metrics").unwrap();
    let mut original = point(1789142400000000123, "a", 3.5);
    original
        .data
        .fields
        .insert("count", FieldValue::Integer(i64::MAX));
    original.data.fields.insert("ok", FieldValue::Boolean(true));
    original
        .data
        .fields
        .insert("message", FieldValue::String("hello, world".into()));
    db.write(&[original.clone(), point(10, "b", 1.0)]).unwrap();
    db.flush().unwrap();
    assert_eq!(db.points().unwrap().len(), 2);
    assert_eq!(
        db.query("SELECT * FROM cpu WHERE host = 'a'")
            .unwrap()
            .rows
            .len(),
        1
    );
    db.write(&[point(original.data.timestamp, "a", 9.0)])
        .unwrap();
    let updated = db
        .query_series(&original.key, &TimeRange::new(i64::MIN, i64::MAX))
        .unwrap();
    assert_eq!(
        updated[0].fields.get("count"),
        Some(&FieldValue::Integer(i64::MAX))
    );
    assert_eq!(
        updated[0].fields.get("usage"),
        Some(&FieldValue::Float(9.0))
    );
    assert_eq!(
        db.delete(
            "cpu",
            &BTreeMap::from([("host".into(), "b".into())]),
            TimeRange::new(10, 10)
        )
        .unwrap(),
        1
    );
    db.flush().unwrap();
    drop(db);
    drop(engine);
    let engine = StorageEngine::new(cfg.clone()).unwrap();
    let db = engine.get_database("metrics").unwrap();
    assert_eq!(db.points().unwrap().len(), 1);
    db.compact().unwrap();
    assert_eq!(db.stats().sstables, 1);
    db.write(&[point(-1, "out-of-order", 7.0)]).unwrap();
    db.flush().unwrap();
    drop(db);
    drop(engine);
    let engine = StorageEngine::new(cfg).unwrap();
    let db = engine.get_database("metrics").unwrap();
    assert_eq!(db.points().unwrap().len(), 2);
    assert_eq!(
        db.get_latest(&original.key).unwrap().unwrap().timestamp,
        original.data.timestamp
    );
    assert_eq!(
        db.points()
            .unwrap()
            .iter()
            .find(|p| p.key == original.key)
            .unwrap()
            .data
            .fields
            .get("message"),
        Some(&FieldValue::String("hello, world".into()))
    );
}

#[test]
fn sql_boolean_boundaries_nulls_and_aggregates() {
    let dir = TempDir::new().unwrap();
    let engine = StorageEngine::new(config(&dir)).unwrap();
    let db = engine.create_database("test").unwrap();
    db.write(&[point(0, "a", 10.), point(1, "b", 20.), point(2, "c", 30.)])
        .unwrap();
    assert_eq!(
        db.query("SELECT * FROM cpu WHERE host = 'a' OR host = 'c'")
            .unwrap()
            .rows
            .len(),
        2
    );
    assert_eq!(
        db.query("SELECT * FROM cpu WHERE NOT (host = 'a')")
            .unwrap()
            .rows
            .len(),
        2
    );
    assert_eq!(
        db.query("SELECT * FROM cpu WHERE time > 0 AND time < 2")
            .unwrap()
            .rows
            .len(),
        1
    );
    assert_eq!(
        db.query("SELECT * FROM cpu WHERE time = 1")
            .unwrap()
            .rows
            .len(),
        1
    );
    assert_eq!(
        db.query("SELECT * FROM cpu WHERE absent > 0")
            .unwrap()
            .rows
            .len(),
        0
    );
    assert_eq!(
        db.query("SELECT * FROM cpu WHERE NOT (absent > 0)")
            .unwrap()
            .rows
            .len(),
        0
    );
    assert_eq!(
        db.query("SELECT COUNT(*) FROM cpu").unwrap().rows[0].values[0].as_f64(),
        Some(3.)
    );
    assert_eq!(
        db.query("SELECT COUNT(*) FROM missing").unwrap().rows[0].values[0].as_f64(),
        Some(0.)
    );
    assert_eq!(
        db.query("SELECT FIRST(usage), LAST(usage) FROM cpu")
            .unwrap()
            .rows[0]
            .values
            .iter()
            .map(|v| v.as_f64().unwrap())
            .collect::<Vec<_>>(),
        vec![10., 30.]
    );
    assert_eq!(
        db.query("SELECT MEAN(usage) FROM cpu GROUP BY time('1m')")
            .unwrap()
            .rows[0]
            .values[0]
            .as_f64(),
        Some(20.)
    );
    assert!(db
        .query("SELECT MEAN(usage) FROM cpu GROUP BY time('0s')")
        .is_err());
    assert!(db.query("SELECT * FROM cpu; SELECT * FROM cpu").is_err());
    assert!(db
        .query("SELECT * FROM cpu JOIN other ON cpu.host=other.host")
        .is_err());
}

#[test]
fn retention_is_persistent_and_boundary_is_exclusive() {
    let dir = TempDir::new().unwrap();
    let cfg = config(&dir);
    let engine = StorageEngine::new(cfg.clone()).unwrap();
    let db = engine.create_database("retention").unwrap();
    db.write(&[point(1, "a", 1.), point(10_000_000_000, "a", 2.)])
        .unwrap();
    db.set_retention(10).unwrap();
    assert_eq!(db.enforce_retention(20_000_000_000).unwrap(), 1);
    db.compact().unwrap();
    drop(db);
    drop(engine);
    let engine = StorageEngine::new(cfg).unwrap();
    let db = engine.get_database("retention").unwrap();
    assert_eq!(db.retention_seconds(), 10);
    assert_eq!(db.points().unwrap().len(), 1);
    db.set_retention(0).unwrap();
    assert_eq!(db.enforce_retention(i64::MAX).unwrap(), 0);
}

#[test]
fn batch_validation_is_atomic_and_exact_delete_does_not_delete_superset_tags() {
    let dir = TempDir::new().unwrap();
    let engine = StorageEngine::new(config(&dir)).unwrap();
    let db = engine.create_database("test").unwrap();
    let mut invalid = point(2, "a", 2.);
    invalid.data.fields = Fields::new();
    assert!(db.write(&[point(1, "a", 1.), invalid]).is_err());
    assert_eq!(db.points().unwrap().len(), 0);
    let a = point(1, "a", 1.);
    let mut b = a.clone();
    b.key.tags.insert("region".into(), "east".into());
    db.write(&[a.clone(), b]).unwrap();
    assert_eq!(
        db.delete_matching("cpu", &a.key.tags, TimeRange::new(1, 1), true)
            .unwrap(),
        1
    );
    assert_eq!(db.points().unwrap().len(), 1);
}

#[test]
fn concurrent_writes_and_flushes_do_not_lose_points() {
    let dir = TempDir::new().unwrap();
    let engine = StorageEngine::new(config(&dir)).unwrap();
    let db = engine.create_database("test").unwrap();
    std::thread::scope(|scope| {
        for host in ["a", "b", "c", "d"] {
            let db = db.clone();
            scope.spawn(move || {
                for i in 0..25 {
                    db.write(&[point(i, host, i as f64)]).unwrap();
                    if i % 10 == 0 {
                        db.flush().unwrap();
                    }
                }
            });
        }
    });
    assert_eq!(db.points().unwrap().len(), 100);
    db.compact().unwrap();
    assert_eq!(db.points().unwrap().len(), 100);
}

#[test]
fn invalid_names_and_second_writer_are_rejected() {
    let dir = TempDir::new().unwrap();
    let cfg = config(&dir);
    let engine = StorageEngine::new(cfg.clone()).unwrap();
    for name in ["", "..", "../escape", "a/b", "a\\b"] {
        assert!(engine.create_database(name).is_err());
    }
    assert!(StorageEngine::new(cfg).is_err());
}

#[test]
fn recovery_repairs_torn_wal_tail_and_preserves_future_writes() {
    use std::io::Write;
    let dir = TempDir::new().unwrap();
    let cfg = config(&dir);
    {
        let engine = StorageEngine::new(cfg.clone()).unwrap();
        engine.write("test", &[point(1, "a", 1.)]).unwrap();
    }
    let wal = dir.path().join("test/wal/wal_00000000000000000000.log");
    {
        let mut f = std::fs::OpenOptions::new().append(true).open(&wal).unwrap();
        f.write_all(&[100, 0, 0, 0, 1, 2]).unwrap();
    }
    {
        let engine = StorageEngine::new(cfg.clone()).unwrap();
        engine.write("test", &[point(2, "b", 2.)]).unwrap();
    }
    let engine = StorageEngine::new(cfg).unwrap();
    assert_eq!(
        engine.get_database("test").unwrap().points().unwrap().len(),
        2
    );
}

#[test]
fn corrupted_wal_fails_closed() {
    let dir = TempDir::new().unwrap();
    let cfg = config(&dir);
    {
        let engine = StorageEngine::new(cfg.clone()).unwrap();
        engine.write("test", &[point(1, "a", 1.)]).unwrap();
    }
    let wal = dir.path().join("test/wal/wal_00000000000000000000.log");
    let mut data = std::fs::read(&wal).unwrap();
    data[15] ^= 1;
    std::fs::write(wal, data).unwrap();
    assert!(StorageEngine::new(cfg).is_err());
}

#[test]
fn unsupported_sql_modifiers_fail_instead_of_changing_results_silently() {
    let dir = TempDir::new().unwrap();
    let engine = StorageEngine::new(config(&dir)).unwrap();
    let db = engine.create_database("syntax").unwrap();
    db.write(&[point(1, "a", 10.0)]).unwrap();
    for sql in [
        "SELECT TOP 1 * FROM cpu",
        "SELECT * INTO copied FROM cpu",
        "WITH ignored AS (SELECT * FROM cpu) SELECT * FROM cpu",
        "SELECT * FROM cpu FETCH FIRST 1 ROW ONLY",
        "SELECT DISTINCT ON (host) * FROM cpu",
        "SELECT COUNT(*) OVER () FROM cpu",
        "SELECT * FROM cpu ORDER BY time NULLS FIRST",
    ] {
        assert!(db.query(sql).is_err(), "Accepted unsupported SQL: {sql}");
    }
    assert_eq!(
        db.query("SELECT COUNT(*) FROM \"cpu\"").unwrap().rows.len(),
        1
    );
}
