# Verification record

Verified locally on Windows 11 with Rust 1.96.0, Node.js 24.20.0, and Python 3.14. This records observed results, not a production certification.

| Check | Result |
| --- | --- |
| Rust workspace tests | 57 passed: 37 unit tests, 9 storage regression tests, 11 server tests |
| Rust formatting | `cargo fmt --all -- --check` passed |
| Release workspace build | Server and offline CLI built successfully |
| Authenticated HTTP smoke suite | Passed, including assistant configuration and missing-key responses |
| Python and JavaScript clients | CRUD, errors, and exact integer round trips passed |
| Snapshot restore | Restored into a new database and verified count and values |
| Process restart recovery | Writes and tombstone deletions survived restart |
| Offline CLI | Create, list/query, and compact exercised |
| Browser TypeScript + production bundle | Passed |
| Agent frontend tests | 7 passed: reviewed writes, destructive scope confirmation, SQL execution/editor integration, memory-only keys, model lookup, context changes, and old-server error handling |
| Gemini transport/tool loop | Local mock provider verified schema → bounded read → write proposal → final response, signature preservation, scope validation, key-safe errors, and model discovery; proposals never mutate storage |
| npm audit | Zero known vulnerabilities reported for the installed dependency graph |
| Browser interaction | Database creation, point write/upsert, SQL time buckets, SQL error state, data browsing, documentation search, and connection failure exercised |
| Responsive layout | Desktop and 390-pixel layouts inspected; documentation overflow and mobile navigation fixed |
| Production browser preview | Built assets rendered against the real API through the preview proxy |
| Development launcher | `node start-all.js --verify` passed server, web, and proxy readiness on isolated ports; both services exited |
| Docker / hosted CI | Configuration provided; not executed here |

## Small local HTTP workload

Command: `python scripts/benchmark.py --points 10000 --batch 500` against the release server on loopback, with immediate WAL synchronization and the browser console running.

| Measurement | Observed |
| --- | --- |
| Points / batch size | 10,000 / 500 |
| Write elapsed time | 0.224 seconds |
| HTTP point throughput | 44,712 points/second |
| Batch-write p95 | 19.772 ms |
| Aggregate-query p95 | 39.963 ms |
| Verified live points | 10,000 |

This is one small, warm local workload: twenty write batches and twenty aggregate queries. It is not a sustained-load, high-cardinality, disk-capacity, or multi-client benchmark. Results depend on hardware, filesystem caches, dataset, concurrency, and background activity. Re-run the included script on the deployment host.

## Remaining validation boundaries

The automated suite covers process interruption, normal persistence, invalid input, and selected corruption cases. It does not simulate all power failures or storage-device faults. There is no completed independent security assessment, distributed failover test, or production soak test. Docker is unavailable in this environment, so image builds, Linux container permissions, and reverse-proxy deployment must be checked on the target host. See ARCHITECTURE.md for memory and single-node constraints.

## Assistant validation

The original assistant failure was reproduced as HTTP 404 from an older running backend. The release backend was rebuilt with assistant routes, the development proxy checked, and the updated one-command launcher verified on isolated ports. Native Gemini request formatting was corrected to omit an empty parameter schema and normalize adjacent conversation roles. Models are now discoverable using the user's API key instead of relying on a hardcoded list.

Live Gemini 3.8 Flash calls returned HTTP 503. Using the same session key, Gemini 2.5 Flash successfully inspected the actual database schema and produced a SQL action. The action was executed through the browser and completed successfully. The default was changed to gemini-2.5-flash. Transient HTTP 502/503/504 responses now receive up to two retries with backoff within the turn deadline; a regression verifies recovery and a three-attempt ceiling. This successful call does not guarantee ongoing provider availability. No provider secret is included in this record.
