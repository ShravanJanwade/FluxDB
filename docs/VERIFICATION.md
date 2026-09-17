# Verification record

What was actually run, and what it reported. Observed results on one machine —
not a production certification, and not a substitute for load testing, fault
injection or independent security review.

Environment: Windows 11 (10.0.26200), Rust 1.96.0, Node.js 24.20.0,
Python 3.14.7.

## Automated

| Check | Result |
| --- | --- |
| `cargo fmt --all -- --check` | Passed |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | Passed with zero warnings |
| `cargo test --workspace --locked` | **105 passed**: 37 engine unit, 9 storage regression, 38 server unit, 21 control-plane integration |
| `cargo build --release --workspace --locked` | Server and offline CLI built |
| `python scripts/smoke.py` | Passed: authentication, CORS, CRUD, exact integer round trips, atomic batch validation, SQL, checkpoint, process restart, snapshot restore, durable deletion, OpenAPI, and both SDKs |
| `npm run typecheck` | Passed |
| `npm test` (vitest) | **75 passed**: result-shape classification, categorical slot stability and truncation, nanosecond precision, query macros, every field-value shape, formatting, colour contrast in both themes, the sign-in flows, and the console tour checklist's grid structure |
| `npm run build` | Passed. Landing page payload ≈ 255 kB raw / 85 kB gzipped; the 536 kB charting chunk loads only when a console screen mounts |
| `npm audit` | No known vulnerabilities in the installed graph |
| Categorical palette validator | Passed in both modes against the real surfaces (`#ffffff` and `#11141d`): lightness band, chroma floor, adjacent colour-vision separation (worst dE 9.1 light / 8.4 dark, target >= 8) and the normal-vision floor (worst 19.6 / 19.3, floor 15). Contrast is a documented relief case on three light slots, met by the legend and the table view. |
| Colour contrast | Every text token measures >= 4.5:1 on its surface in both themes, asserted by parsing the token file |
| `node start-all.js --verify` | Passed: server, browser proxy, and control plane (sqlite) readiness; both services exited |
| Container image build and boot | **Not executed here** — Docker is not installed on this machine. CI builds the image, boots it, and asserts it serves `/health`, the control plane, the console and a deep link while refusing anonymous `/api/v1`. |

### What the control-plane integration tests cover

These drive the real router over a temporary SQLite store, so they exercise
cookies, authorization and tenancy rather than mocking them.

- Sign-up provisions an organization, a project and a bucket, grants viewer
  access to the shared showcase, and never returns a password hash.
- Duplicate addresses are rejected case-insensitively; weak passwords and
  malformed addresses are rejected with the reason.
- A wrong password does not set a session, and an unknown address is
  indistinguishable from a wrong one.
- **One account cannot reach another's project by any route.** Nine endpoints
  are attempted with correct ids and all answer 404 rather than 403, so ids
  cannot be probed for existence. The owner's data is verified untouched
  afterwards.
- Roles gate behaviour: a viewer reads but cannot create buckets, list API keys
  or read the audit trail; promotion to member enables writes; a non-admin
  cannot change anyone's role; removal revokes access.
- The shared showcase project is readable and refuses every mutation — writes,
  deletes, bucket creation, project deletion and key creation — for every role.
- API keys ingest over line protocol and JSON, query back, cannot address a
  bucket outside their project, are rejected when malformed or unknown, and stop
  working the moment they are revoked. A read-only key is refused a write with a
  message that names the reason.
- Guest workspaces arrive pre-filled and writable, cannot invite members, and
  carry an expiry.
- Invitations to an address with no account are applied when that address
  registers.
- A monitor whose query cannot produce a number is rejected at creation; a
  monitor cannot be attached to a bucket outside its project.
- `/api/v1`, `/api/v1/stats`, `/api/v1/telemetry` and `/metrics` are all
  unreachable without the administration token whenever accounts exist, while
  `/health` stays public.
- Self-hosted proxy targets are refused for plain HTTP to a remote host,
  loopback, and link-local addresses.
- The console is served from the same process, and a deep link answers 200 with
  the application shell while an unknown `/api` path answers a JSON 404.

## Exercised in a browser

Against a local release server with the console on Vite, using a real guest
session throughout.

| Flow | Observed |
| --- | --- |
| Landing page | Renders with live figures read from the running instance: 8,496 showcase points, 0.14 ms API p95, uptime |
| One-click demo | Created a guest workspace and landed on its overview in about two seconds, with no form |
| Guest overview | 360 sandbox points, 5.59 KB on disk, chart of the seeded data |
| Showcase dashboard | Eight panels resolved: 2,204 rpm, 0.48 % error rate, 1,224 ms p99, 91 % peak host CPU, with the incident visible in the per-service latency chart |
| Query workspace | "Which service broke?" returned 6 rows in 11.0 ms and auto-selected the chart view; the table shows payments at 1,224 ms p99 against catalog at 68 ms |
| Data explorer | Schema tree with three measurements, per-field charting, paginated points |
| Write round trip | Wrote a point through the explorer dialog: the measurement's count went 120 → 121, the new `cores` integer field appeared in schema discovery, and the chart updated |
| API key → ingest | Issued a key in the console, then wrote line protocol and JSON with curl from outside the browser and queried the result back; the console showed the same three hosts |
| Tenancy from outside | A key was refused a bucket in another project (404); malformed and unknown keys were refused (401); revocation took effect on the next request |
| Instance health | 124 requests recorded, p50 10.4 ms, p95 460.6 ms, 0 % failed, with the latency and request charts populated |
| Route sweep | All 13 console routes rendered their expected heading with no error state |
| Built bundle, not just dev | The production build served by the release binary boots, renders the landing page with live figures, starts a guest session, and walks all 13 console routes with no console errors beyond the expected signed-out 401 |

Defects found this way and fixed:

- Charts drew into a stale canvas width, because the React ECharts binding
  resizes through echarts internals that moved in echarts 6. Panels measured
  134 px while their boxes were 589.
- The hosted deployment's reverse proxy injected the administration token into
  every request, which made the single-tenant `/api/v1` surface public.
- White button labels on the dark theme's accent fill measured 3.3:1, and the
  light theme's success green 3.8:1 on white. Both are below AA and neither
  looks obviously wrong.
- Chart colour was assigned by rank, so changing the time range repainted every
  surviving series — a reader who had learned "payments is pink" was misled by
  their own filter. Colour now follows the series name; ordering still follows
  the peak.
- Series past the palette's eight slots were drawn in recycled colours. They are
  now dropped with a count, because a ninth hue is indistinguishable from one
  already in use.
- The original indigo-to-cyan chart palette failed the dark lightness band, at
  OKLCH L 0.69-0.84 against a 0.48-0.67 target — bright saturated lines on a
  near-black surface that are harder to separate, not easier.
- **The first hosted deploy served a blank page.** Hand-splitting react away
  from the rest of `node_modules` created a chunk cycle: `react-router-dom`
  landed in the react chunk and pulled `@remix-run/router` into vendor, while
  vendor's `lucide-react` imported react back. Rollup printed
  `Circular chunk: vendor -> react -> vendor` and the bundle threw
  `Cannot read properties of undefined (reading 'forwardRef')` on load.

  Two process failures, not one. The build warned and the warning was missed
  because only the last few lines of its output were read. And the built bundle
  was verified with `curl` — status codes and content types, which were all
  correct — rather than by loading it in a browser, which is the only thing that
  would have caught it. Vite now throws on a circular chunk instead of warning,
  and that guard was tested in both directions: exit 1 with the bad split, exit 0
  with the good one. Booting the built bundle in a browser and walking all 13
  console routes is now part of the browser checks below.

## Small local HTTP workload

`python scripts/benchmark.py --points 10000 --batch 500` against the release
server on loopback, with immediate WAL synchronization and the seeded showcase
workspace also resident.

| Measurement | Observed |
| --- | --- |
| Points / batch size | 10,000 / 500 |
| Platform | Windows-11-10.0.26200-SP0 |
| Write elapsed time | 0.535 s |
| HTTP point throughput | 18,684 points/second |
| Batch-write p95 | 32.9 ms |
| Aggregate-query p95 | 53.5 ms |
| Verified live points | 10,000 |

Twenty write batches and twenty aggregate queries, warm, on one machine. It is
not a sustained-load, high-cardinality, disk-capacity or multi-client benchmark.

An earlier revision of this file recorded 44,712 points/second for the same
command. The difference is not a regression in the write path: that run had an
empty engine, while this one also holds the 8,496-point showcase workspace, so
every aggregate query scans more and the reader materialises more. That is the
memory-bound read behaviour described in the limits, showing up exactly where it
should. Re-run the script on your own host; the number is a property of the
hardware and the resident dataset, not of the project.

## Remaining validation boundaries

- **Container and hosted deployment.** The image build is exercised only in CI.
  Deploying to Render with managed Postgres has not been run from this machine.
- **Postgres control plane.** The Postgres backend compiles and shares its query
  path with SQLite, but the integration tests run against SQLite. A first
  deployment should check the startup log line naming the backend.
- **GitHub OAuth.** The flow is implemented and its failure paths are covered,
  but no round trip against GitHub's servers has been performed here; the
  provider reports itself disabled without credentials.
- **Sustained load and high cardinality.** Untested. The reader materialises
  SSTables and query snapshots in memory, so both are expected to be limits.
- **Power-loss durability.** The suite tests process interruption and recovery,
  not device-level failure. Guarantees still depend on filesystem and hardware
  behaviour, and Windows does not use the same directory-sync mechanism as Unix.
- **Security review.** Self-assessed. No independent review has been performed.
