# Deploying FluxDB

FluxDB is one container: the engine, the control plane, the token API and the
browser console, on a single port. There is no reverse proxy in the deployment,
which means there is no proxy configuration that can accidentally expose an
endpoint — an earlier revision of this project shipped exactly that bug.

Three paths, in order of how quickly they get you a public URL.

1. [Render free tier + managed Postgres](#render-free-tier)
2. [Docker anywhere](#docker-anywhere)
3. [Your own VM with HTTPS](#your-own-vm)

---

## What every deployment needs

| Variable | Why it matters |
| --- | --- |
| `FLUXDB_TOKEN` | Administration credential for `/api/v1`. That surface can name any engine database, so it sits underneath project isolation. 32+ characters, required for any non-loopback bind. |
| `FLUXDB_SESSION_SECRET` | Signs OAuth state. Without a stable value, sign-in round trips started before a restart cannot complete after it. 32+ characters. |
| `PUBLIC_BASE_URL` | The absolute URL visitors reach. Used to build the OAuth callback and to decide whether cookies get `Secure`. |
| `DATABASE_URL` | Postgres for accounts, projects and API keys. **Read the next section before skipping this.** |
| `FLUXDB_STATIC_DIR` | Set to `/app/web` in the image; the server serves the console from it. |
| `FLUXDB_DATA_DIR` | Where time-series data lives. |

Generate both secrets with:

```sh
python -c "import secrets; print(secrets.token_hex(32))"
```

### Why control-plane metadata needs a real database

Time-series data and control-plane metadata have different durability needs.

Losing the time-series data on a free hosted plan is survivable: the showcase
workspace is re-seeded on every start, so the demo works again within seconds of
a redeploy.

Losing the **accounts** is not survivable in any useful sense. Someone who signs
up, builds a dashboard and comes back next week to find no such account exists
has watched the product fail. Free hosted containers generally have an ephemeral
filesystem, so a SQLite file inside one is wiped by every redeploy.

Set `DATABASE_URL` to a managed Postgres instance and that problem goes away.
[Neon](https://neon.tech) and [Supabase](https://supabase.com) both have
permanent free tiers that need no card. Include `?sslmode=require`:

```
postgresql://user:password@ep-example-123456.eu-central-1.aws.neon.tech/fluxdb?sslmode=require
```

Without it, FluxDB falls back to a SQLite file beside the time-series data and
logs which backend it chose at startup. That is the right choice for a laptop or
a VM with a real disk, and the wrong one for an ephemeral container.

---

## Render free tier

Roughly ten minutes, no card.

**1. Create the Postgres instance.** Sign up at Neon or Supabase, create a
database, and copy the connection string. Append `?sslmode=require` if it is not
already there.

**2. Deploy the blueprint.** Push this repository to GitHub, then in Render
choose **New → Blueprint** and point it at your fork. `render.yaml` describes one
Docker web service on the free plan and generates `FLUXDB_TOKEN` and
`FLUXDB_SESSION_SECRET` for you.

**3. Fill in the two values Render cannot guess**, under the service's
Environment tab:

| Key | Value |
| --- | --- |
| `DATABASE_URL` | the connection string from step 1 |
| `PUBLIC_BASE_URL` | `https://your-service.onrender.com` |

Deploy. The first build compiles the Rust workspace in release mode and builds
the console, which takes several minutes; later builds reuse Docker layers.

**4. Check it.**

```sh
BASE=https://your-service.onrender.com
curl --fail $BASE/health
curl --fail $BASE/api/cloud/config                       # control_plane should read "postgres"
curl -o /dev/null -w '%{http_code}\n' $BASE/api/v1/databases   # must be 401
```

Then open the URL, click **Explore the demo**, and confirm the seeded fleet and
its dashboard appear.

**5. Optional — GitHub sign-in.** Create an OAuth app at
<https://github.com/settings/developers>:

- **Homepage URL**: `https://your-service.onrender.com`
- **Authorization callback URL**:
  `https://your-service.onrender.com/api/cloud/auth/github/callback`

Set `GITHUB_CLIENT_ID` and `GITHUB_CLIENT_SECRET` on the service. Both are
required; the provider stays disabled and the button explains itself if either
is missing.

### What the free plan costs you

- **The instance sleeps** after about 15 minutes of inactivity, and the next
  request waits for a cold start. If you are sending someone a link, open it
  yourself first.
- **The filesystem is ephemeral.** Time-series data does not survive a redeploy;
  the showcase workspace re-seeds itself, and account data is safe in Postgres.
- **Memory is capped** at 512 MB. The seeded showcase is about 8,500 points;
  guest workspaces are bounded and reclaimed after 24 hours, and anonymous
  workspace creation is rate limited and capped.

Both limits are stated on the deployed site rather than hidden.

---

## Docker anywhere

```sh
cp .env.example .env
# Replace FLUXDB_TOKEN and FLUXDB_SESSION_SECRET with generated values.
# Set PUBLIC_BASE_URL. Set DATABASE_URL if the filesystem is ephemeral.
docker compose up --build -d
```

The console is on <http://localhost:8080>, bound to loopback. Time-series data
and — unless `DATABASE_URL` is set — control-plane metadata live on the
`fluxdb-data` volume.

```sh
docker compose logs -f
docker compose ps
docker compose down            # keeps the volume
docker compose down --volumes  # deletes the data
```

Put an HTTPS reverse proxy in front for anything reachable from outside the
host, and set `PUBLIC_BASE_URL` to the public address so cookies get `Secure`
and the OAuth callback is built correctly.

To run the image directly instead:

```sh
docker build -t fluxdb .
docker run -d --name fluxdb -p 8086:8086 \
  -e FLUXDB_TOKEN="$(python -c 'import secrets; print(secrets.token_hex(32))')" \
  -e FLUXDB_SESSION_SECRET="$(python -c 'import secrets; print(secrets.token_hex(32))')" \
  -e PUBLIC_BASE_URL="http://localhost:8086" \
  -v fluxdb-data:/app/data \
  fluxdb
```

---

## Your own VM

The path worth taking if you want the data to persist and the instance not to
sleep. Any small VM with a disk works; a 1 GB instance is plenty for a demo.

**1. Install Docker and clone the repository.**

```sh
sudo apt update && sudo apt install -y docker.io docker-compose-v2 git
sudo usermod -aG docker "$USER" && newgrp docker
git clone https://github.com/ShravanJanwade/FluxDB.git && cd FluxDB
```

**2. Point a hostname at the VM's public IPv4 address** and open ports 80 and
443. A free subdomain from a dynamic-DNS provider is fine.

**3. Configure.**

```sh
printf 'FLUXDB_TOKEN=%s\nFLUXDB_SESSION_SECRET=%s\nPUBLIC_BASE_URL=https://%s\nDATABASE_URL=\n' \
  "$(openssl rand -hex 32)" "$(openssl rand -hex 32)" "fluxdb.example.com" > .env
docker compose up --build -d
```

`DATABASE_URL` is left empty on purpose: a VM has a real disk, so SQLite beside
the data directory is the right choice.

**4. Terminate TLS.** Run Caddy in front — it obtains and renews certificates
automatically once DNS resolves and 80/443 are reachable:

```sh
docker run -d --name caddy --network host \
  -v caddy-data:/data -v caddy-config:/config \
  caddy:2 caddy reverse-proxy \
    --from fluxdb.example.com --to 127.0.0.1:8080
```

**5. Check it.**

```sh
BASE=https://fluxdb.example.com
curl --fail $BASE/health
curl --fail $BASE/api/cloud/config
curl -o /dev/null -w '%{http_code}\n' $BASE/api/v1/databases   # 401
```

**6. Back it up.** The container's volume holds everything. A logical snapshot
per bucket is the portable option:

```sh
curl -H "Authorization: Bearer $FLUXDB_TOKEN" \
  http://127.0.0.1:8080/api/v1/databases/YOUR_DB/export > snapshot-$(date +%F).json
```

Store snapshots off the VM. Never copy live WAL or SSTable files as a backup.

---

## Single-tenant deployments

If you want a plain time-series database with no accounts — for an internal
service, or as a backend behind your own application:

```sh
docker run -d -p 8086:8086 \
  -e FLUXDB_CLOUD=off \
  -e FLUXDB_TOKEN="$(openssl rand -hex 32)" \
  -e FLUXDB_CORS_ORIGINS="https://your-app.example.com" \
  -v fluxdb-data:/app/data \
  fluxdb
```

`FLUXDB_CLOUD=off` disables the control plane entirely: no accounts, no
`/api/cloud`, no console sign-in. What remains is `/api/v1` behind the shared
token, line-protocol ingestion, `/metrics` and the OpenAPI document. Visitors
can still point the hosted console at it as a self-hosted connection.

---

## Troubleshooting

**`error: invalid local: resolve : lstat .../deploy: no such file or directory`**
— the service is still configured with a Dockerfile path that no longer exists.
Render matches Blueprint services *by name*: if `name:` in `render.yaml` differs
from the existing service's name, Render leaves that service's settings
untouched and treats the definition as a new service. Either set **Dockerfile
Path** to `./Dockerfile` and **Docker Build Context** to `.` in the service's
settings, or make `name:` match the existing service and re-sync the Blueprint.

**"Control plane could not start"** — `DATABASE_URL` is wrong or unreachable.
The message includes the underlying error. This is a hard startup failure on
purpose: serving a signed-out product would look like data loss to every account
holder. To run without accounts, set `FLUXDB_CLOUD=off`.

**"Non-loopback binding requires FLUXDB_TOKEN with at least 32 characters"** —
the instance is reachable from outside the host, so it needs a real
administration credential. Generate one.

**GitHub sign-in returns to the login page with an error** — the callback URL on
the OAuth app must match `PUBLIC_BASE_URL` exactly, including scheme and any
trailing path. The error text on the sign-in page names which check failed.

**The console loads but every request is 401** — the session cookie is being
dropped. Over HTTPS this usually means `PUBLIC_BASE_URL` is `http://`, so the
cookie is sent without `Secure` and the browser refuses it on a secure page.

**"FLUXDB_STATIC_DIR … does not contain index.html"** — the console was not
built, or the path is wrong. The server fails fast rather than serving the API
while every browser request answers 404.

**Charts are empty but the explorer shows points** — check the time range. The
axis is pinned to the range you selected, so data outside it is genuinely not
shown.
