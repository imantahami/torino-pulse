# Torino Pulse

An end-to-end data pipeline that ingests live public transit and weather data for Turin, Italy, and transforms it into a queryable analytics layer.

Runs 24/7 on a single Azure VM. Six Airflow DAGs, four data sources, Postgres storage, dbt transformations with data quality tests.

---

## Why this exists

Turin's transit operator (GTT) publishes GTFS-Realtime feeds — vehicle positions, stop-level delay predictions, and service alerts — but nothing persists them. Each request returns a snapshot that is overwritten seconds later.

This project captures those snapshots on a schedule, stores them, and builds a historical record you can actually query: how the fleet size changes across the day, which routes deviate most from schedule, how weather correlates with service.

The problems here are the ones that make real pipelines annoying: multi-source scheduling, protobuf parsing, deduplicating a feed that reports the same observation repeatedly, and running orchestration on a machine that barely has enough RAM for it.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  SOURCES                                                    │
│                                                             │
│  GTT GTFS-RT          GTT GTFS-RT        GTT GTFS-RT        │
│  vehicle_position     trip_update        alerts             │
│  (protobuf)           (protobuf)         (protobuf)         │
│                                                             │
│  GTT Static GTFS      Open-Meteo                            │
│  (zip, 13 MB)         (JSON)                                │
└──────────────────────────┬──────────────────────────────────┘
                           │
              ┌────────────▼────────────┐
              │  AIRFLOW 2.10.4         │
              │  CeleryExecutor         │
              │                         │
              │  6 DAGs:                │
              │  · positions    5 min   │
              │  · trip_updates 5 min   │
              │  · alerts      15 min   │
              │  · weather     15 min   │
              │  · static_gtfs  daily   │
              │  · dbt_transform 15 min │
              └────────────┬────────────┘
                           │
              ┌────────────▼────────────┐
              │  POSTGRES 13            │
              │                         │
              │  raw_vehicle_positions  │
              │  raw_trip_updates       │
              │  raw_alerts             │
              │  raw_weather            │
              │  gtfs_routes / stops /  │
              │  trips / agency /       │
              │  calendar               │
              └────────────┬────────────┘
                           │
              ┌────────────▼────────────┐
              │  DBT 1.8.2              │
              │                         │
              │  staging/               │
              │  · stg_vehicle_positions│
              │  · stg_weather          │
              │                         │
              │  8 data quality tests   │
              └─────────────────────────┘
```

Everything runs in Docker Compose on one Azure B2ls_v2 VM (2 vCPU, 4 GB RAM) in Spain Central.

---

## Data sources

| Source | Format | Interval | What it gives you |
|---|---|---|---|
| `vehicle_position.aspx` | GTFS-RT protobuf | 5 min | Vehicle ID, route, trip, lat/lon, bearing, GPS timestamp |
| `trip_update.aspx` | GTFS-RT protobuf | 5 min | Per-stop arrival/departure delay in seconds, license plate, wheelchair accessibility |
| `alerts.aspx` | GTFS-RT protobuf | 15 min | Service disruptions: cause, effect, severity, affected routes and stops, Italian description text |
| `gtt_gtfs.zip` | GTFS static | daily | 204 routes with human-readable names, 6,894 stops, 38,824 trips, service calendar |
| Open-Meteo | JSON | 15 min | Temperature, precipitation, wind speed, weather code for Turin (45.0703, 7.6869) |

No API keys required for any of them.

---

## Schema

### `raw_vehicle_positions`

One row per vehicle per snapshot. ~300 rows every 5 minutes during service hours.

| Column | Type | Note |
|---|---|---|
| `id` | serial | PK |
| `vehicle_id` | text | e.g. `16316U` |
| `vehicle_label` | text | Human-facing number |
| `route_id` | text | Joins to `gtfs_routes.route_id` |
| `trip_id` | text | Joins to `gtfs_trips.trip_id` |
| `trip_start_time` | text | Scheduled start, as published |
| `trip_start_date` | text | `YYYYMMDD` |
| `latitude` / `longitude` | float8 | `0,0` when GPS is unavailable — see below |
| `bearing` | float8 | Degrees |
| `gps_timestamp` | bigint | Unix seconds, from the vehicle |
| `fetched_at` | timestamp | When *we* pulled it |

### `raw_trip_updates`

One row per **stop-time update**, not per trip. A single trip with 20 upcoming stops produces 20 rows.

| Column | Type | Note |
|---|---|---|
| `trip_id`, `route_id` | text | |
| `vehicle_id`, `vehicle_label`, `license_plate` | text | |
| `wheelchair_accessible` | text | Enum name, e.g. `WHEELCHAIR_ACCESSIBLE` |
| `stop_sequence` | int | Position in the trip |
| `arrival_delay_seconds` | int | **Negative means running early** |
| `departure_delay_seconds` | int | Often null in this feed |
| `gps_timestamp` | bigint | |
| `fetched_at` | timestamp | |

### `raw_alerts`

One row per **(alert × informed entity)**. An alert affecting 23 stops produces 23 rows.

| Column | Type | Note |
|---|---|---|
| `alert_id` | text | Stable across fetches |
| `cause`, `effect`, `severity_level` | text | Enum names, e.g. `DETOUR`, `INFO` |
| `header_text`, `description_text` | text | Italian |
| `active_period_start` / `_end` | bigint | Unix seconds |
| `informed_route_id` / `_stop_id` / `_trip_id` | text | Nullable — only one is usually set |
| `fetched_at` | timestamp | |

### `gtfs_*`

Reference tables, fully rebuilt daily (`DROP TABLE` + recreate). All columns are `TEXT` — the loader reads the CSV header and creates columns dynamically, so the schema follows whatever GTT publishes.

`stop_times.txt` (70 MB) and `shapes.txt` (8 MB) are deliberately skipped. Row-by-row `INSERT` on those would take too long; loading them properly needs `COPY`.

---

## Data quality notes

Things the raw feeds actually do, which the transformation layer has to handle:

**GPS dropouts.** About 1.3% of vehicle positions report `latitude = 0, longitude = 0`. These are not in the Gulf of Guinea — the GPS is unavailable. `stg_vehicle_positions` filters them out.

**Duplicate weather observations.** Open-Meteo updates on a 15-minute grid, but nothing guarantees our fetch aligns with it. Fetching twice inside one window returns the same `observed_at` with a different `fetched_at`. `stg_weather` deduplicates with a window function, keeping the most recent fetch per observation time:

```sql
row_number() over (
    partition by observed_at
    order by fetched_at desc
) as rn
```

The `unique` test on `stg_weather.observed_at` is what proves this works — it fails immediately if the dedup logic breaks.

**Alerts grow fast.** Because alerts are re-fetched every 15 minutes and fan out across affected entities, `raw_alerts` accumulates roughly 25k rows per fetch cycle in a busy period. Deduplication by `alert_id` belongs in staging (not yet implemented — see Roadmap).

**Vehicles run early, not late.** Average `arrival_delay_seconds` sits around **−67 seconds**. Not a bug: the published schedule appears to be padded, so vehicles routinely reach stops ahead of it.

---

## Findings so far

From roughly one day of collection:

**Fleet size follows a clear daily curve.** Distinct active vehicles per hour (UTC; Turin is UTC+2 in summer):

```
12:00  327    ┃━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
13:00  354    ┃━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  peak
14:00  327    ┃━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
15:00  319    ┃━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
16:00  327    ┃━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
17:00  334    ┃━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
18:00  315    ┃━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
19:00  249    ┃━━━━━━━━━━━━━━━━━━━━━━━
20:00  160    ┃━━━━━━━━━━━━━━
21:00  144    ┃━━━━━━━━━━━━
22:00  137    ┃━━━━━━━━━━━━
23:00   90    ┃━━━━━━━
00:00   20    ┃━
01:00    6    ┃
02:00  156    ┃━━━━━━━━━━━━━     service resumes
03:00  238    ┃━━━━━━━━━━━━━━━━━━━━━━
04:00  293    ┃━━━━━━━━━━━━━━━━━━━━━━━━━━━
05:00  303    ┃━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

The overnight floor is 6 vehicles at 03:00 local. Ramp-up begins around 04:00 local and reaches full fleet by 07:00.

**Detours dominate service alerts.** Of 154 active alerts:

| Effect | Alerts |
|---|---|
| `DETOUR` | 95 |
| `OTHER_EFFECT` | 53 |
| `UNKNOWN_EFFECT` | 4 |
| `MODIFIED_SERVICE` | 2 |

62% are detours, mostly tied to summer roadworks. The alert descriptions name the specific streets and the contractor.

---

## Running it

### Requirements

- Docker + Docker Compose
- 4 GB RAM minimum. On exactly 4 GB you will need swap — see below.

### Setup

```bash
git clone https://github.com/imantahami/torino-pulse.git
cd torino-pulse

mkdir -p logs plugins config
echo "AIRFLOW_UID=50000" > .env

docker compose up airflow-init
docker compose up -d
```

Wait 3–5 minutes for the containers to install their extra Python dependencies, then:

```bash
docker compose ps          # all services should read (healthy)
docker exec -it torino-pulse-postgres-1 psql -U airflow -c "CREATE DATABASE torino_pulse;"
```

Airflow UI is at `http://localhost:8080`, credentials `airflow` / `airflow`.

Unpause the six DAGs. They will start on their schedules; trigger them manually if you don't want to wait.

### Verifying data flow

```bash
docker exec -it torino-pulse-postgres-1 psql -U airflow -d torino_pulse -P pager=off -c "
SELECT 'positions' AS t, COUNT(*) FROM raw_vehicle_positions
UNION ALL SELECT 'trip_updates', COUNT(*) FROM raw_trip_updates
UNION ALL SELECT 'alerts', COUNT(*) FROM raw_alerts
UNION ALL SELECT 'weather', COUNT(*) FROM raw_weather
UNION ALL SELECT 'routes', COUNT(*) FROM gtfs_routes;"
```

### Running dbt manually

From inside the scheduler container, using the in-project profile:

```bash
docker compose exec airflow-scheduler bash -c \
  "cd /opt/airflow/dbt/torino_pulse && dbt run --profiles-dir . && dbt test --profiles-dir ."
```

---

## Implementation notes

### Two dbt profiles, deliberately

`dbt/torino_pulse/profiles.yml` uses `host: postgres` — the Docker service name. This is the profile the `dbt_transform` DAG uses, via `--profiles-dir .`.

`~/.dbt/profiles.yml` uses `host: localhost`. This is for running dbt from your own machine against the exposed port.

They are not interchangeable. Running dbt locally while sitting in the project directory picks up the in-project profile and fails with `could not translate host name "postgres"`. Pass `--profiles-dir` explicitly.

### BashOperator instead of Astronomer Cosmos

Cosmos is the idiomatic way to run dbt from Airflow, and it was the first thing tried. It does not install cleanly alongside Airflow 2.10.4 — pip enters dependency backtracking, walking through hundreds of `dbt-core` and `dbt-adapters` versions without converging, and the containers eventually come up `unhealthy`.

Pinning versions didn't resolve it. What did was dropping Cosmos entirely and running dbt through two `BashOperator` tasks:

```python
dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --profiles-dir .',
)
dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command=f'cd {DBT_PROJECT_DIR} && dbt test --profiles-dir .',
)
dbt_run >> dbt_test
```

This loses Cosmos's per-model task granularity in the Airflow graph. In exchange it installs in seconds and does not break. For a project this size that is the right trade.

### Non-destructive schema evolution

`raw_vehicle_positions` originally stored only ID, coordinates, and timestamp. Adding route and trip fields mid-collection would normally mean recreating the table and losing accumulated rows. Instead the DAG issues idempotent `ALTER TABLE` statements on every run:

```python
for col, coltype in [("route_id", "TEXT"), ("bearing", "FLOAT"), ...]:
    cur.execute(f"ALTER TABLE raw_vehicle_positions ADD COLUMN IF NOT EXISTS {col} {coltype}")
```

Cheap, safe to re-run, and history survives. Rows written before the change simply have nulls in the new columns.

### Dependencies via `_PIP_ADDITIONAL_REQUIREMENTS`

`docker-compose.override.yaml` injects `gtfs-realtime-bindings`, `psycopg2-binary`, `requests`, `dbt-core`, and `dbt-postgres` at container start.

Airflow's own docs call this a development-only feature, and they're right — it re-installs on every container start, which costs minutes and depends on PyPI being reachable. A custom image extending `apache/airflow:2.10.4` is the correct fix and is on the roadmap.

### Swap is mandatory on a 4 GB VM

Airflow's Compose stack asks for 4 GB. A B2ls_v2 reports 3.8 GB usable, so `airflow-init` warns and then the system dies under load — SSH itself becomes unresponsive once available memory drops below ~90 MB.

Four GB of swap fixes it:

```bash
sudo fallocate -l 4G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
```

Swap is slower than RAM, but this workload is bursty — a few seconds of work every five minutes — so paging is not on the hot path.

### Azure for Students region restrictions

Student subscriptions are limited by policy to about five regions, and the set differs per subscription. Deployments to unlisted regions fail with `RequestDisallowedByAzure` regardless of quota or SKU availability.

Don't guess. Read the allowed list:

```
Azure Portal → Policy → Assignments → "Allowed resource deployment regions" → Parameters
```

---

## Security

`profiles.yml` currently contains a plaintext password. It's the Compose default (`airflow`/`airflow`) on a database that isn't publicly routable, so the exposure is limited — but it is still the wrong pattern. The fix is `password: "{{ env_var('DBT_PASSWORD') }}"`.

The Airflow webserver runs with default credentials. If you expose port 8080, scope the NSG rule to your own IP rather than `0.0.0.0/0`.

`*.pem` is gitignored. Verify that before your first push.

---

## Roadmap

- [ ] Staging models for `raw_trip_updates` and `raw_alerts`, including dedup by `alert_id`
- [ ] Marts: delay by route, hourly fleet profile, weather-vs-service correlation
- [ ] Load `stop_times.txt` via `COPY` so `stop_sequence` resolves to real stop names
- [ ] Custom Airflow image, replacing `_PIP_ADDITIONAL_REQUIREMENTS`
- [ ] Secrets via environment variables
- [ ] Dashboard
- [ ] GitHub Actions running `dbt test` on PRs

---

## Stack

Airflow 2.10.4 · dbt-core 1.8.2 / dbt-postgres 1.8.2 · Postgres 13 · Redis 7.2 · Docker Compose · Python 3.12 · Azure VM (Ubuntu 24.04)

## Data attribution

Transit data © [GTT Torino](https://www.gtt.to.it/), published as open data.
Weather data from [Open-Meteo](https://open-meteo.com/), CC BY 4.0.
