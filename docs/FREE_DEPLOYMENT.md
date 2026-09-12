# Free resume demo deployment

Recommended: one Oracle Cloud Always Free Ubuntu VM, Docker Compose, a free DuckDNS hostname, and Caddy HTTPS. The included public gateway permits browsing and SQL queries and blocks database mutations. This is a deployment-level restriction, not a new user/RBAC system. The existing UI still displays write controls; those requests return a clear read-only error on the public URL.

Reviewed September 12, 2026. Cloud/container deployment has not been executed in this local Windows environment. Run the validation below on your VM before sharing the link.

## 1. Obtain a free VM

Create an Oracle Cloud account and choose your home region carefully. In Compute → Instances → Create instance, select:

- An Always Free eligible Ubuntu 24.04 image.
- VM.Standard.A1.Flex, 2 OCPUs and 12 GB RAM, provided your tenancy's free allocation is unused.
- A 50 GB boot disk within your total free storage allowance.
- A public subnet, internet gateway, and public IPv4 address.
- SSH authentication; save the private key securely.

Oracle currently documents an A1 allowance equivalent to 2 OCPUs / 12 GB, plus 200 GB combined boot/block storage. Confirm Always Free eligibility in your own console; avoid paid shapes and extra resources. Capacity can be unavailable and idle free instances can be reclaimed. This is not a guaranteed always-on free service. Keep an offline demo video and off-VM backups. [Oracle free resource limits](https://docs.oracle.com/en-us/iaas/Content/FreeTier/freetier_topic-Always_Free_Resources.htm).

In the VM's security list or network security group, allow inbound TCP 80 and 443 from the internet. Restrict TCP 22 to your own public IP. Keep 8080 and 8086 closed publicly. If the guest OS firewall is enabled, allow the same required traffic there; do not disable it wholesale.

## 2. Set up a free hostname

Register a subdomain at [DuckDNS](https://www.duckdns.org/), such as `your-fluxdb-demo.duckdns.org`, and point it to the VM's public IPv4 address. Use your actual unique subdomain throughout this guide. Update the DNS record if the VM's IP changes. [DuckDNS service description](https://www.duckdns.org/about.jsp).

## 3. Push the current project to GitHub

Commit your project changes, including the new deployment files, Dockerfiles, lockfiles, and docs. Do not include `.env`, database files, `.runtime`, `node_modules`, or Rust build output. The ignore files cover these paths. Clone this same version on the VM; an older backend can lack assistant routes.

## 4. Connect and install dependencies

From Windows PowerShell (replace key path and IP):

```powershell
ssh -i "C:\path\to\your-private-key" ubuntu@YOUR_VM_IP
```

The remaining commands run in the VM's Ubuntu shell unless stated otherwise:

```bash
sudo apt update
sudo apt install -y docker.io docker-compose-v2 git python3 openssl curl
sudo systemctl enable --now docker
sudo docker compose version
git clone https://github.com/YOUR_USERNAME/FluxDB.git
cd FluxDB
```

These package names target Ubuntu 24.04; `docker-compose-v2` is in its universe repository. For another distribution/version, follow the [official Docker installation instructions](https://docs.docker.com/engine/install/ubuntu/). Do not mix Ubuntu Docker packages with Docker's own package repository on the same host.

The images build natively on the VM, including ARM64 on A1. No local Windows binaries are uploaded. The first Rust release build may take several minutes.

## 5. Create the server environment

For a fresh clone with no `.env` yet:

```bash
umask 077
printf 'FLUXDB_TOKEN=%s\nGEMINI_API_KEY=\nGEMINI_MODEL=gemini-2.5-flash\nDEMO_HOST=your-fluxdb-demo.duckdns.org\nFLUXDB_CORS_ORIGINS=https://your-fluxdb-demo.duckdns.org\n' "$(openssl rand -hex 32)" > .env
nano .env
```

Replace both example hostnames. Keep the generated admin token private. The resume override deliberately disables a shared server Gemini key. Visitors can use their own key in Ask AI → Assistant settings. A public shared LLM key would expose your API quota to anonymous callers; a subscription does not guarantee unlimited or free API use. See [Gemini API pricing](https://ai.google.dev/gemini-api/docs/pricing).

If Render's Blueprint does not populate `FLUXDB_TOKEN`, the included Render wrapper generates a random ephemeral token so the public read/query demo can still start. Set your own 32+ character `FLUXDB_TOKEN` in Render's Environment tab if you need private administrator CRUD access; otherwise the generated token is intentionally not displayed or recoverable after a restart.

For a frictionless resume presentation, provide a short video showing your authenticated write/update/delete and AI workflows alongside the live browsing/query demo.

## 6. Validate and start

Always run these commands from the repository root. The first Compose file defines the base path for the override's relative mounts.

```bash
sudo docker compose -f compose.yaml -f deploy/compose.resume.yaml config --quiet
sudo docker compose -f compose.yaml -f deploy/compose.resume.yaml run --rm --no-deps gateway caddy validate --config /etc/caddy/Caddyfile --adapter caddyfile
sudo docker compose -f compose.yaml -f deploy/compose.resume.yaml up -d --build
sudo docker compose -f compose.yaml -f deploy/compose.resume.yaml ps
sudo docker compose -f compose.yaml -f deploy/compose.resume.yaml logs --tail=80 server studio gateway
```

Caddy obtains and renews HTTPS certificates when DNS resolves correctly and ports 80/443 are reachable. [Caddy automatic HTTPS](https://caddyserver.com/docs/automatic-https).

The Rust database uses the persistent `fluxdb-data` Docker volume. Both TLS state and database data survive ordinary container recreation. Never run `docker compose down -v` unless you intend to erase those volumes. This is still a single VM; a Docker volume is not an off-host backup.

## 7. Seed real stored sample data

On the VM, load your trusted `.env` into this shell and seed through the private loopback console port:

```bash
set -a
. ./.env
set +a
python3 scripts/demo.py --url http://127.0.0.1:8080 --seconds 60
```

This creates `demo_observability` containing explicitly synthetic sample measurements. Dashboard request latency remains measured server activity, not fabricated CPU/network metrics.

## 8. Verify the public experience

```bash
curl --fail https://your-fluxdb-demo.duckdns.org/api/v1/health
curl --fail https://your-fluxdb-demo.duckdns.org/api/v1/databases
curl --fail https://your-fluxdb-demo.duckdns.org/api/v1/databases/demo_observability/query \
  -H 'Content-Type: application/json' \
  -d '{"query":"SELECT * FROM cpu ORDER BY time DESC LIMIT 10"}'
curl -i -X POST https://your-fluxdb-demo.duckdns.org/api/v1/databases/public_write_should_fail
```

Expect successful health/list/query responses, and HTTP 403 for the last request. If it creates a database, stop publishing the URL and fix the gateway routing. Public mutation blocking relies on all traffic passing through Caddy; never expose the internal studio/backend ports externally.

Open the HTTPS URL in a private/incognito browser window:

- The console should load without an admin token prompt and show stored sample data.
- Select `demo_observability`; browse measurements and run the query above.
- Check Developer resources and the mobile layout.
- Optionally use a session Gemini key to inspect schema and prepare/run queries. Mutation proposals remain blocked by the public gateway. Model availability and API limits still apply.
- Public users share one disposable dataset; do not upload personal or production data. The gateway does not provide per-visitor databases, rate limiting, or protection against costly query scans.

## 9. Access full CRUD privately

From your own Windows terminal, keep this SSH tunnel open:

```powershell
ssh -i "C:\path\to\your-private-key" -L 18080:127.0.0.1:8080 ubuntu@YOUR_VM_IP
```

Open `http://127.0.0.1:18080`. Connect to that same URL with the `FLUXDB_TOKEN` from the VM's `.env`. This bypasses the public read-only gateway through SSH and provides the normal administrator experience. Enter your Gemini session key to demonstrate the full reviewed agent workflow. Never put your admin token on your resume or in a public README.

## 10. Back up and update

On the VM, with `FLUXDB_TOKEN` loaded as in step 7:

```bash
curl --fail -H "Authorization: Bearer $FLUXDB_TOKEN" \
  http://127.0.0.1:8080/api/v1/databases/demo_observability/export \
  -o "$HOME/fluxdb-demo-snapshot.json"
```

Copy the snapshot to your own computer using `scp`, and test `scripts/restore.py` into a separate database through the private endpoint. A backup kept only on the VM cannot survive losing the VM.

To update after pushing changes to GitHub:

```bash
git pull --ff-only
sudo docker compose -f compose.yaml -f deploy/compose.resume.yaml up -d --build
```

Recheck health, public mutation blocking, stored points, and the assistant after updates. Do not run `node start-all.js` or Vite's development server for this deployment.

## Resume presentation

Use three links: **Live demo**, **GitHub**, and **2-minute walkthrough**. Record schema browsing, a SQL time bucket, a reviewed agent write, an update, and recovery/compaction; disclose the public demo's read-only restrictions.

Suggested factual description:

> Built a Rust time-series database with checksummed WAL recovery, compressed SSTables, SQL aggregation, a browser console, and a Gemini tool-calling agent for schema-aware queries and reviewed CRUD operations.

Avoid claiming full InfluxDB compatibility, distributed scalability, production battle-testing, or benchmark throughput beyond your measured workload.

## Why not Render or Koyeb free web hosting?

[Render free web services](https://render.com/docs/free) have ephemeral filesystems and no persistent disks; [Koyeb free instances](https://www.koyeb.com/docs/reference/instances) cannot attach volumes. They can host a resettable demonstration, but do not preserve this disk-backed database correctly across restarts/redeployments. If Oracle capacity is unavailable, there is no guaranteed substitute in this guide: publish the repository and walkthrough while waiting, or choose a paid VM with persistent disk when budget permits.
