# FluxDB use cases

Three small apps that keep their data in FluxDB, the way you would use Postgres,
MongoDB or Supabase for an ordinary app. Each one is a Next.js backend (route
handlers) and a frontend (dashboards), deployed on Vercel. The backend writes to
FluxDB and queries it over HTTPS with a project API key, and the frontend draws
what comes back.

**Live:** <https://fluxdb-use-cases.vercel.app>

| Sector | App | What FluxDB does for it |
| --- | --- | --- |
| Energy | [Campus energy monitor](https://fluxdb-use-cases.vercel.app/energy) | Stores 5-minute submeter readings for four buildings. `MEAN … GROUP BY time($interval), building` draws demand per building, `SUM` gives today's kWh and the bill. |
| Healthcare | [Ward vitals board](https://fluxdb-use-cases.vercel.app/healthcare) | Stores a minute of vital signs per bed. `LAST() … GROUP BY bed` feeds the live board and its NEWS2 early-warning scores; `COUNT … WHERE hr > 110 OR …` counts out-of-range minutes. Nurses can record observations, which are written straight to FluxDB. |
| Finance | [Crypto market board](https://fluxdb-use-cases.vercel.app/markets) | Stores real one-minute candles from Coinbase. `FIRST(open), MAX(high), MIN(low), LAST(close), SUM(volume)` rolls them up into 5- and 15-minute candlesticks at query time; `STDDEV` measures volatility. |

At the bottom of every dashboard, **How this page uses FluxDB** lists what the
last request wrote (with the line protocol it sent), every SQL query it ran, and
how long FluxDB took to answer each one.

The energy and ward data are simulated. Market prices are real. None of it is
medical or financial advice.

## How it works

```
browser ──GET /api/energy──▶ Next.js route handler (Vercel)
                               │ 1. read the watermark          SELECT MAX(until_ms) FROM sync_state …
                               │ 2. write what is new           POST /api/ingest/v1/write  (line protocol)
                               │ 3. query, in parallel          POST /api/ingest/v1/query  (SQL)
                               ▼
                             FluxDB (Azure Container Apps), project "Use cases"
                               buckets: energy · vitals · markets, 7-day retention
```

There is no cron job. When someone opens a dashboard, its route handler fills in
every reading since the last visit, then queries. The simulated meters and
monitors are pure functions of time, and FluxDB replaces a point that has the same
measurement, tags and timestamp, so two visitors filling the same gap leave one
copy. The market stream asks Coinbase (or Kraken, if Coinbase does not answer)
only for the candles that closed since its watermark.

FluxDB scales to zero when nobody uses it, so the first request after a quiet
half hour waits for it to start. The pages say so and retry by themselves.

### Data model

| Bucket | Measurement | Tags | Fields | Every |
| --- | --- | --- | --- | --- |
| `energy` | `power` | `building` | `hvac_kw`, `lighting_kw`, `plug_kw`, `ev_kw`, `solar_kw`, `net_kw`, `kwh` | 5 min, per building |
| `energy` | `campus` | | the same totals, plus `outdoor_c` | 5 min |
| `vitals` | `vitals` | `bed`, `source` (`monitor` or `manual`) | `hr`, `spo2`, `rr`, `sbp` (integers), `temp_c`, `on_o2` (boolean), `acvpu` (string) | 1 min, per bed |
| `markets` | `candles` | `product` | `open`, `high`, `low`, `close`, `volume` | 1 min, per product |
| each | `sync_state` | `stream` | `until_ms` | each write |

At seven days of retention the three buckets hold about 120,000 points, well
under the hosted plan's 2,000,000 per project.

## Set up FluxDB

On the hosted FluxDB (or your own):

1. Sign in to the [FluxDB console](https://fluxdb.icygrass-95174406.westus2.azurecontainerapps.io)
   with GitHub or email, then choose **New project** and name it `Use cases`.
2. Under **Buckets**, choose **New bucket** three times: `energy`, `vitals` and
   `markets`, each with **Retention: 7 days**.
3. Under **API keys**, choose **New API key**, name it `use-cases app`, keep the
   **read** and **write** scopes, and **Create key**. Copy the key (it starts with
   `fdbk_`); the console shows it once.

Use a signed-in account, not the guest workspace: guest workspaces expire after a
day.

## Deploy to Vercel

1. On <https://vercel.com/new>, import the `ShravanJanwade/FluxDB` repository.
2. Name the project `fluxdb-use-cases` (this gives the address used above) and set
   **Root Directory** to `examples/use-cases`. Vercel detects Next.js.
3. Under **Environment Variables**, add:

   | Name | Value |
   | --- | --- |
   | `FLUXDB_URL` | `https://fluxdb.icygrass-95174406.westus2.azurecontainerapps.io` |
   | `FLUXDB_API_KEY` | the `fdbk_…` key from step 3 above |

   Add `FLUXDB_ENERGY_BUCKET`, `FLUXDB_VITALS_BUCKET` or `FLUXDB_MARKETS_BUCKET`
   only if you named the buckets differently.
4. **Deploy**, then open the address Vercel gives you. The home page says
   *Connected to FluxDB* once the key and buckets are right.

`vercel.json` runs the functions in `pdx1` (Portland), next to the Azure region
FluxDB is in, and skips builds for commits that do not touch this folder.

## Run it locally

You need Node.js 20.9 or later and a FluxDB server with the three buckets and a
key. To run FluxDB on your machine too, start it from the repository root with
`node start-all.js` (API on <http://127.0.0.1:8086>, console on
<http://127.0.0.1:5173>), sign up in the console and follow
[Set up FluxDB](#set-up-fluxdb) there.

```bash
cd examples/use-cases
cp .env.example .env.local   # set FLUXDB_URL (http://127.0.0.1:8086 for a local server) and FLUXDB_API_KEY
npm install
npm run dev
```

`npm test` runs the unit tests (line protocol, NEWS2 scoring, catch-up ranges)
and `npm run build` makes a production build.

## Keeping the hosted FluxDB cheap

FluxDB runs on Azure Container Apps and stops 30 minutes after its last request.
This app tries not to keep it awake for nothing:

- Dashboards refresh every 30 or 60 seconds, but stop while the tab is hidden and
  after 10 minutes without a mouse, key or scroll event.
- API answers are cached by Vercel's CDN for 10 seconds, so a crowd of visitors
  costs one set of queries.
- `robots.txt` keeps crawlers out of `/api`.

## Where things are

| Path | What it is |
| --- | --- |
| `lib/fluxdb.ts` | A small typed client: `write`, `query`, `whoami` |
| `lib/lineProtocol.ts` | Points to line protocol, with escaping |
| `lib/sync.ts` | Watermarks and catch-up ingestion |
| `lib/energy.ts`, `lib/vitals.ts`, `lib/markets.ts` | The three data sources, and NEWS2 |
| `lib/server/*Dashboard.ts` | The queries behind each page |
| `app/api/*` | Route handlers |
| `app/energy`, `app/healthcare`, `app/markets` | The dashboards |
| `components/charts` | Line, stacked-column, candlestick, heatmap and sparkline charts in plain SVG |
