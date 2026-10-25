# Deploying FluxDB to Azure Container Apps

FluxDB runs as one container on Azure Container Apps that only runs while it is
being used, which keeps a year of hosting well inside the $100 Azure for
Students credit, shared with CodeLens.

- **Stays on for the first 48 hours,** visitors or not, so it can collect data
  and you can try it without waiting for it to wake.
- **Then sleeps after 30 idle minutes.** Zero replicas cost nothing. The next
  visit starts it again in a few seconds, and it goes back to sleep 30 minutes
  after the last request.
- **Keeps its data.** The engine works on the container's fast local disk, and
  `deploy/azure/persist-datadir.sh` archives the data directory to an Azure
  Files share every five minutes when something changed and again on every
  stop. The next start restores the newest archive.
- **Deploys itself.** Every push to `master` builds the image, publishes it to
  GitHub Container Registry and updates the app.

## What it costs

| Resource | Why | Cost |
| --- | --- | --- |
| Container Apps environment `cae-portfolio` | Shared with CodeLens. No dedicated plan and no Log Analytics workspace. | $0 |
| Container app `fluxdb` (0.25 vCPU, 0.5 GiB) | Billed per second, only while running. | $0 for the first ~200 running hours a month (free grant), then about $0.03/hour |
| The 48-hour warm-up | One replica, once. | About a quarter of one month's free grant, so normally $0 |
| Storage account + 5 GiB `fluxdb-data` share | Data archives. Billed for the few MB used. | a few cents a month |
| GitHub Container Registry | Holds the image. Free for public images. | $0 (Azure Container Registry would be ~$5/month) |
| Managed identity, budget alert | Deploy sign-in and spend emails. | $0 |

The monthly free grant (180,000 vCPU-seconds and 360,000 GiB-seconds per
subscription) is shared by both apps. A few visits a day keeps both inside it;
even both apps awake eight hours every day would cost roughly $4–14 a month.
Running both around the clock would be about $16 a month while idle and $50
while busy, which is why nothing here is always on. (Prices are East US list
prices and vary a little by region.)

`fluxdb-studio/public/robots.txt` asks search crawlers not to crawl the demo,
since every crawler visit would wake it for half an hour.

## How the data survives sleeping

The server's own disk is empty every time the app wakes, so the wrapper that
starts it restores the data directory first:

- **Every five minutes**, if anything changed, the server is paused for the
  fraction of a second it takes to `tar` the directory to local disk, then
  resumed while the archive is compressed and uploaded. A paused copy is what
  a crash at that instant would leave, and the engine is built to recover from
  exactly that: the WAL tail is repaired on open, and SSTables and the
  manifest are written to `.pending` files and renamed into place.
- **On every stop** the server flushes its memtables and exits, and the final
  archive is taken.
- **The showcase is never archived.** The server seeds an empty showcase on
  start, so its incident is always about 90 minutes old instead of days.

Accounts, projects and API keys live in SQLite inside the same directory, so
they are archived too. Setting `DATABASE_URL` to a free Postgres instance (the
Neon or Supabase one from the Render deployment works unchanged) moves them out
of the archive: existing accounts carry over, and a crash cannot lose them. A
crash can lose at most the last five minutes of points.

If the server ever fails to open a restored archive, the wrapper keeps it as
`fluxdb-data.tar.gz.quarantined` on the share and starts with an empty
directory rather than keeping the demo down.

## Step 1 — Get this on `master`

The deploy workflow only runs from `master`. Merge the pull request that added
`deploy/azure/` and `.github/workflows/deploy-azure.yml`.

## Step 2 — Run the setup script in Azure Cloud Shell

1. Sign in at <https://portal.azure.com> with the account that has Azure for
   Students, and open **Cloud Shell** (the `>_` icon in the top bar). Choose
   **Bash**. If it asks about storage, pick **No storage account required**.
2. Optional but convenient: run `gh auth login` and follow the prompts. The
   script then sets the GitHub variables and starts the first deploy itself.
3. Run:

   ```bash
   git clone https://github.com/ShravanJanwade/FluxDB.git
   bash FluxDB/deploy/azure/setup.sh
   ```

It takes about five minutes and is safe to run again. It creates (or reuses,
if you already ran the CodeLens one) the resource group `rg-portfolio`, the
environment, the storage account and share, the managed identity GitHub
Actions signs in as, and a monthly budget that emails you. The last lines print
the address FluxDB will have, of the form
`https://fluxdb.<random-name>.<region>.azurecontainerapps.io`.

Northeastern's Azure tenant refuses a resource group without three tags, so the
script puts them on everything it creates: `Owner` (your sign-in email),
`Cost Center` (`Azure for Students`, since the rule only checks that the tag
exists) and `IaC managed` (this repository). To use other values, prefix the
command, for example `TAG_COST_CENTER=12345 bash FluxDB/deploy/azure/setup.sh`.
If a policy asks for a tag the script doesn't set, add it with
`EXTRA_TAGS='Name=value'`, separating several with `;`.

## Step 3 — Repository variables

Skip this if the script said it set them. Otherwise open
**Settings → Secrets and variables → Actions → Variables** on the repository and
add the six values the script printed:

| Variable | Example |
| --- | --- |
| `AZURE_CLIENT_ID` | `00000000-0000-0000-0000-000000000000` |
| `AZURE_TENANT_ID` | `00000000-0000-0000-0000-000000000000` |
| `AZURE_SUBSCRIPTION_ID` | `00000000-0000-0000-0000-000000000000` |
| `AZURE_RESOURCE_GROUP` | `rg-portfolio` |
| `AZURE_CONTAINERAPPS_ENVIRONMENT` | `cae-portfolio` |
| `AZURE_TAGS` | `{"Owner":"you@northeastern.edu","Cost Center":"Azure for Students","IaC managed":"ShravanJanwade/FluxDB"}` |

None of these are secrets. GitHub Actions signs in with OpenID Connect, so no
Azure password or key is stored in GitHub.

If you want accounts in Postgres, add the `DATABASE_URL` **secret** now, before
the first deploy (see optional settings).

## Step 4 — Deploy

Open **Actions → Deploy to Azure → Run workflow** (skip if the script started
it). The first run compiles the Rust workspace in release mode and takes about
fifteen minutes; later runs reuse the layer cache.

**The first run will probably stop at "Check that Azure can pull the image".**
New GitHub packages are private, and Azure pulls the image without credentials.
Open the link in the error (or **your profile → Packages → fluxdb → Package
settings**), choose **Change visibility → Public**, then **Re-run jobs**. This
is a one-time step.

`FLUXDB_TOKEN` and `FLUXDB_SESSION_SECRET` are generated on the first deploy and
kept in the container app, so later deploys reuse them.

## Step 5 — Check it

```bash
BASE=https://fluxdb.<your-domain>
curl --fail $BASE/health
curl --fail $BASE/api/cloud/config                              # demo_project_id is there
curl -o /dev/null -w '%{http_code}\n' $BASE/api/v1/databases    # must be 401
```

Then open the address and click **Explore the demo**.

Logs are streamed rather than stored (storing them in Log Analytics is billed
per GB). While the app is running:

```bash
az containerapp logs show -n fluxdb -g rg-portfolio --follow --tail 200
```

Lines starting with `[persist]` come from the archive wrapper. To read the
generated administration token for the `/api/v1` SDKs:

```bash
az containerapp secret show -n fluxdb -g rg-portfolio --secret-name fluxdb-token --query value -o tsv
```

## The warm-up window

The first deploy records a 48-hour window in tags on the app (`warmup-from`,
`warmup-until`) and adds a KEDA cron rule that holds one replica during it.
Later deploys keep the same window, and the first deploy after it closes drops
the rule. From then on the app only runs while it is being visited.

- **Different length for the first deploy:** set the repository variable
  `FLUXDB_WARMUP_HOURS` before it runs (`0` turns the warm-up off).
- **Keep it on for an interview or a demo day:** **Actions → Deploy to Azure →
  Run workflow**, and enter a number of hours in `warmup_hours`. The new window
  starts immediately.

## Optional settings

Add these under **Settings → Secrets and variables → Actions**, then re-run the
workflow.

| Name | Kind | Effect |
| --- | --- | --- |
| `DATABASE_URL` | secret | Postgres for accounts, projects and API keys, for example the Render deployment's Neon URL with `?sslmode=require`. |
| `GH_OAUTH_CLIENT_ID`, `GH_OAUTH_CLIENT_SECRET` | secrets | Enable GitHub sign-in. Set the OAuth app's callback to `https://fluxdb.<your-domain>/api/cloud/auth/github/callback`. |
| `GEMINI_API_KEY` | secret | Enables the assistant and agent for registered accounts. |
| `GEMINI_MODEL` | variable | Default `gemini-2.5-flash`. |
| `IDLE_SHUTDOWN_SECONDS` | variable | Idle time before scaling to zero, default `1800`, minimum `300`. |
| `FLUXDB_WARMUP_HOURS` | variable | Length of the first deploy's warm-up, default `48`. |
| `FLUXDB_TOKEN`, `FLUXDB_SESSION_SECRET` | secrets | Override the generated values, for example to keep Render's token. |

GitHub secret names cannot start with `GITHUB_`, hence `GH_OAUTH_…`.

A Neon database suspends its own compute a few minutes after the last query,
and FluxDB only queries it while the app is awake, so it sleeps along with it.

## Step 6 — Retire the Render service

Once the Azure address works, update the links that point at
`fluxdb-demo.onrender.com` (this README, your resume, your portfolio), point the
GitHub OAuth app at the new callback, and delete the Render service.

## Keeping an eye on the credit

- Remaining credit: <https://www.microsoftazuresponsorships.com/Balance>, or the
  **Education** page in the portal.
- Spend by resource: **Cost Management → Cost analysis**, scoped to
  `rg-portfolio`.
- The budget created by the setup script emails you at 50% and 100% of $8 a
  month, and when the forecast passes $8. Twelve months of $8 is the whole
  credit; normal use is far below it.
- To stop FluxDB completely: `az containerapp delete -n fluxdb -g rg-portfolio`.
  The archives stay in the file share, and the next deploy restores them.
- To remove everything for both projects: `az group delete -n rg-portfolio`.

## Troubleshooting

**`RequestDisallowedByPolicy` … "A tag named … is required".** A policy in your
organization wants tags the script didn't set or different values. Set the
values with `TAG_OWNER`, `TAG_COST_CENTER` or `TAG_IAC_MANAGED`, add missing tags
with `EXTRA_TAGS='Name=value'`, and re-run.

**`RequestDisallowedByAzure` during setup.** Azure for Students only allows a
few regions. The script reads the allowed list and tries them in order; if it
still fails, find the list under **Policy → Assignments → Allowed resource
deployment regions** and run `LOCATION=<region> bash FluxDB/deploy/azure/setup.sh`.

**`AADSTS700213` / "No matching federated identity record" at Azure login.**
The identity only trusts workflows running on `master` of this repository. Run
the workflow from `master`, or re-run the setup script if the repository was
renamed.

**"The image is private".** See step 4.

**"Control plane could not start".** `DATABASE_URL` is wrong or unreachable;
the log line includes the underlying error.

**The app never becomes healthy.** Look at the logs (step 5).
`az containerapp revision list -n fluxdb -g rg-portfolio -o table` shows whether
the newest revision started.

**Points went back in time.** Archives are taken every five minutes while data
changes and on every graceful stop, so only a crash loses writes, and at most
the last five minutes of them. During a deploy, writes that reach the old
version in its final seconds can also be lost.
