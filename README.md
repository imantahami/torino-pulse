# Torino Pulse

An end-to-end data pipeline that ingests live public transit and weather data for Turin, Italy, transforms it into a queryable analytics layer, and exposes it through three live interfaces.

Runs 24/7 on Oracle Cloud Always Free (ARM64, Turin region). Six Airflow DAGs, four data sources, Postgres storage, nine dbt models with 39 data quality tests, and an AI-powered chart builder.

**Live:**
- AI Chart Builder: http://84.8.253.68:8501 — describe what you want to see in plain English
---

## Why this exists

Turin's transit operator (GTT) publishes GTFS-Realtime feeds — vehicle positions, stop-level delay predictions, and service alerts — but nothing persists them. Each request returns a snapshot that is overwritten seconds later.

This project captures those snapshots on a schedule, stores them, and builds a historical record you can actually query: how the fleet size changes across the day, which routes deviate most from schedule, how weather correlates with service.

The interesting problems here are not the happy path. They are the ones documented under [Implementation notes](#implementation-notes): a dependency conflict that left every task silently failing while the containers reported healthy, a UID mismatch that made dbt die without printing anything, and a feed that emits `0` where it means `null`.

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
              ┌────────────▼─────────────┐
              │  POSTGRES 13             │
              │                          │
              │  raw_vehicle_positions   │
              │  raw_trip_updates        │
              │  raw_alerts              │
              │  raw_weather             │
              │  gtfs_* (7 tables, incl. │
              │    1.1M stop_times rows) │
              └────────────┬─────────────┘
                           │
              ┌────────────▼────────────┐
              │  DBT 1.8.2              │
              │                         │
              │  staging/  (6 models)   │
              │  marts/    (3 models)   │
              │                         │
              │  39 data quality tests  │
              └────────────┬────────────┘
                           │
         ┌─────────────────┼─────────────────┐
         ▼                 ▼                 ▼
   ┌───────────┐    ┌───────────┐    ┌───────────────┐
   │  Metabase │    │  Power BI │    │  Streamlit    │
   │  :3000    │    │ (SSH      │    │  AI Chart     │
   │           │    │  tunnel)  │    │  Builder :8501│
   └───────────┘    └───────────┘    └───────────────┘
```

Everything runs in Docker Compose on one Oracle Cloud VM.Standard.A1.Flex (2 OCPU ARM64, 12 GB RAM, 100 GB disk) in the Italy North (Turin) region — permanently free.

---

## Data sources

| Source | Format | Interval | What it gives you |
|---|---|---|---|
| `vehicle_position.aspx` | GTFS-RT protobuf | 5 min | Vehicle ID, route, trip, lat/lon, bearing, GPS timestamp |
| `trip_update.aspx` | GTFS-RT protobuf | 5 min | Per-stop arrival delay in seconds, license plate, wheelchair accessibility |
| `alerts.aspx` | GTFS-RT protobuf | 15 min | Service disruptions: cause, effect, severity, affected routes and stops, Italian description |
| `gtt_gtfs.zip` | GTFS static | daily | 204 routes, 6,894 stops, 38,824 trips, 1.1M stop_times, service calendar |
| Open-Meteo | JSON | 15 min | Temperature, precipitation, wind speed, WMO weather code |

No API keys required. The realtime feeds carry fewer fields than the GTFS-RT spec allows — `current_status`, `occupancy_status`, `congestion_level`, and `stop_id` on trip updates are all absent from GTT's output. Everything GTT does publish is stored.

---

## Model layers

### staging

| Model | Purpose |
|---|---|
| `stg_vehicle_positions` | Drops `(0,0)` GPS readings. Adds local time and `has_assigned_trip`. |
| `stg_trip_updates` | Resolves `route_id` via static GTFS. Adds prediction drift columns and `is_en_route`. |
| `stg_alerts` | Collapses alert/entity pairs, carries the observation window. |
| `stg_weather` | Deduplicates per observation time. Translates WMO codes to conditions. |
| `stg_routes` | Human-readable names, transport mode, urban/extra-urban, night flag. |
| `stg_stops` | Coordinates and wheelchair boarding, filtered to valid geometry. |

### marts

| Model | Grain | Purpose |
|---|---|---|
| `fct_route_performance` | one row per route | Median, p90, stddev, punctuality bands |
| `fct_hourly_service` | one row per local hour | Fleet size + delay + weather, joined |
| `fct_alert_impact` | one row per (route, alert) | Which lines are disrupted and how |

The three filters that make delay numbers meaningful live in `fct_route_performance`, not in the BI tool. Power BI and Metabase read the marts and need no filters of their own.

---

### AI Chart Builder (Streamlit)
Live at `http://84.8.253.68:8501`. Describe what you want to see in plain English; the app uses NVIDIA Llama 3.1-8B to write the SQL and creates the chart directly in Metabase via API.

Source: `streamlit-agent/app.py`. Runs as a systemd service (`streamlit.service`) so it restarts automatically.

### Metabase
Dashboards connected directly to Postgres inside the Docker network. Not publicly exposed — access is available on request.

### Power BI Desktop
DirectQuery mode via SSH tunnel from a local machine:

```bash
ssh -i <key> -L 5432:localhost:5432 ubuntu@84.8.253.68
```

Point Power BI at `localhost:5432`, database `torino_pulse`. Better than an NSG rule scoped to your IP: mobile and home connections change address regularly.

---

## Data quality notes

### GPS dropouts
About 2.3% of vehicle positions report `latitude = 0, longitude = 0`. Filtered in staging. All surviving coordinates fall inside a plausible bounding box for the Turin metro area (lat 44.92–45.19, lon 7.50–7.84), with zero outliers.

### Vehicles without a trip
7.4% of position reports carry a `route_id` but no `trip_id`. This peaks at end of service — 18% at 19:00 local against 3.5% mid-afternoon. Consistent with vehicles returning to depot, but not confirmed. The column is named `has_assigned_trip` rather than `is_in_service`.

### One third of trips report no delay data
33% of stop-time updates report `arrival_delay_seconds = 0`. Three checks decided how to treat them:

**Uniformity.** The zero rate is 31–34% at every stop position, from stop 2 through stop 15.

**Trip-level clustering.** 2,229 of 6,717 trips report zero at every single stop.

**Vehicle-level consistency.** 80% of vehicles sit firmly in one camp (always zero or never zero), which points at the vehicle rather than the moment.

A competing hypothesis — trips caught early, before GTT computed a delay — was tested and rejected: zero-trips and data-trips have identical observation counts (2.2 each). The evidence fits missing data encoded as `0`. `fct_route_performance` excludes them.

### First stop always reads zero
The first stop reports zero delay because the vehicle has not departed. 96% of `stop_sequence = 1` rows are zero. The `is_en_route` flag excludes it.

### Delay drifts negative along a trip
Median delay falls steadily with stop position: −25s at stop 2, −59s at stop 5, −98s at stop 15. Vehicles get further ahead of schedule the further they go — suggesting padded timetables.

---

## Findings

Collected across approximately three days. Patterns are reported as hypotheses to re-test once a fuller dataset exists.

**Fleet follows a clean daily cycle.**

```
03:00    2   ┃                          overnight floor
04:00  151   ┃━━━━━━━━━━━━━             service resumes
06:00  301   ┃━━━━━━━━━━━━━━━━━━━━━━━━━
10:00  346   ┃━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  daytime peak
18:00  322   ┃━━━━━━━━━━━━━━━━━━━━━━━━━━━
21:00  253   ┃━━━━━━━━━━━━━━━━━━━━━
00:00  133   ┃━━━━━━━━━━━            night routes still running
```

**Trams appear more punctual than buses.** Median-of-medians −160s for trams against −112s for buses, and 4.1% versus 11.1% of observations more than a minute late.

**One route stands out.** Route 64 (via Napoli, Grugliasco → corso Vittorio Emanuele II) is the only line with a positive median delay (+15s) and 44% of observations running late. It carried no active service alert at the time of writing.

**Median delay and late frequency correlate at r = 0.75.** Routes that run late on average also run late often.

**Detours dominate service alerts.** Of 154 active alerts, 95 were detours, mostly tied to summer roadworks.

---

## Running it

### Requirements
- Docker + Docker Compose
- 4 GB RAM minimum. On exactly 4 GB you need swap (see below). 12 GB is comfortable.

### Setup

```bash
git clone https://github.com/imantahami/torino-pulse.git
cd torino-pulse

mkdir -p logs plugins config
echo "AIRFLOW_UID=$(id -u)" > .env

docker compose up airflow-init
docker compose up -d
```

Wait 3–5 minutes, then:

```bash
docker compose ps          # all services should read (healthy)
docker exec -it torino-pulse-postgres-1 psql -U airflow -c "CREATE DATABASE torino_pulse;"
```

Airflow UI at `http://localhost:8080`, credentials `airflow` / `airflow`. Unpause the six DAGs.

### Metabase

```bash
docker exec -it torino-pulse-postgres-1 psql -U airflow -c "CREATE DATABASE metabase;"
docker compose -f docker-compose.metabase.yaml up -d
```

First startup takes 3–5 minutes. UI at `http://localhost:3000`.

### AI Chart Builder

```bash
cd streamlit-agent
python3 -m venv venv && source venv/bin/activate
pip install streamlit openai requests python-dotenv
cp .env.example .env   # fill in your values
streamlit run app.py
```

Needs a free NVIDIA API key from `https://build.nvidia.com` and Metabase credentials.

### Verifying data flow

```bash
docker exec -it torino-pulse-postgres-1 psql -U airflow -d torino_pulse -P pager=off -c "
SELECT 'positions' AS t, COUNT(*), NOW()-MAX(fetched_at) AS since_last FROM raw_vehicle_positions
UNION ALL SELECT 'trip_updates', COUNT(*), NOW()-MAX(fetched_at) FROM raw_trip_updates
UNION ALL SELECT 'alerts', COUNT(*), NOW()-MAX(last_seen_at) FROM raw_alerts
UNION ALL SELECT 'weather', COUNT(*), NOW()-MAX(fetched_at) FROM raw_weather;"
```

`since_last` should stay under the DAG interval.

### Running dbt manually

```bash
docker compose exec airflow-scheduler bash -c \
  "cd /opt/airflow/dbt/torino_pulse && dbt run --profiles-dir . && dbt test --profiles-dir ."
```

---

## Implementation notes

### A dependency conflict that reported itself as healthy

After adding `dbt-core` to `_PIP_ADDITIONAL_REQUIREMENTS`, every DAG stopped producing data. All six containers showed `healthy`. The UI showed all DAGs unpaused. The worker logged `celery@… ready`. Nothing said broken.

The scheduler log had it:

```
AttributeError: module 'redis' has no attribute 'client'
```

Installing dbt had pulled in a `redis` version incompatible with the `kombu` build Celery uses. Fixed by pinning `redis==4.6.0`. The lesson: green health checks describe the process, not the work.

### UID mismatch makes dbt exit silently

`dbt --version` worked. `dbt run` printed nothing and exited with code 2.

The project directory was owned by UID 1000 (`ubuntu` on the host); the container runs as UID 50000. dbt could read the models but could not create `logs/` or `target/`, and died before it had anywhere to write the error.

Fixed by aligning the two:

```bash
echo "AIRFLOW_UID=1000" > .env
sudo chown -R 1000:1000 ~/torino-pulse
```

### Deduplicating alerts on ingest

`raw_alerts` grew at roughly 38 MB/hour. Each 15-minute fetch re-inserted all 154 active alerts across every affected entity. Around 99% was a byte-identical copy of something already there.

The fix hashes the content and upserts:

```sql
ON CONFLICT (alert_id, content_hash,
             COALESCE(informed_route_id, ''),
             COALESCE(informed_stop_id, ''),
             COALESCE(informed_trip_id, ''))
DO UPDATE SET last_seen_at = EXCLUDED.last_seen_at,
              times_seen   = raw_alerts.times_seen + 1
```

Result: 245,307 rows collapsed to 8,634, and 193 MB to 9 MB.

### COPY for the large GTFS file

`stop_times.txt` is 70 MB and 1.1M rows. Row-by-row INSERT takes tens of minutes. Streaming into COPY takes 24 seconds:

```python
cur.copy_expert(f"COPY {table} ({cols}) FROM STDIN WITH (FORMAT csv, QUOTE '\"')", text)
```

### CASCADE when reloading reference tables

The static GTFS loader recreates tables daily. Once dbt staging views existed on top of them, `DROP TABLE` started failing. `DROP TABLE ... CASCADE` is correct here: the views are rebuilt by the next dbt run.

### Migrating from Azure to Oracle Cloud

Data was migrated via `pg_dump` on the source, `scp` through a laptop as intermediary, and `pg_restore` on the destination:

```bash
# Source (Azure)
docker exec torino-pulse-postgres-1 pg_dump -U airflow -Fc torino_pulse > ~/torino_pulse.dump

# Transfer via laptop
scp azureuser@<azure-ip>:~/torino_pulse.dump ./
scp ./torino_pulse.dump ubuntu@<oracle-ip>:~/

# Destination (Oracle)
docker cp ~/torino_pulse.dump torino-pulse-postgres-1:/tmp/
docker exec torino-pulse-postgres-1 pg_restore -U airflow -d torino_pulse --clean --if-exists /tmp/torino_pulse.dump
```

137,297 positions, 911,532 trip_updates, 10,020 alerts, and 224 weather records transferred without loss.

### Oracle Cloud ARM and region capacity

Oracle Always Free provides 2 OCPU ARM64 and 12 GB RAM permanently. ARM images for Airflow, Postgres, and Redis are all multi-arch and work without modification.

The Italy North (Turin) region (`eu-turin-1`) was selected — one availability domain, lower demand than Frankfurt or US East, and capacity was available immediately.

Oracle's Always Free ARM allocation was halved from 4 OCPU / 24 GB to 2 OCPU / 12 GB in June 2026 with no public announcement. The new limits are sufficient for this stack with room to spare (swap usage: 0).

### Swap is mandatory on a 4 GB VM

Airflow's Compose stack asks for 4 GB. A B2ls_v2 (Azure) reports 3.8 GB usable, so the system dies under load. Four GB of swap fixes it. Setting `AIRFLOW__CORE__LOAD_EXAMPLES: 'false'` cut swap usage from 2.6 GB to 800 MB by stopping the scheduler from parsing 74 example DAGs on every loop.

### Azure for Students region restrictions

Student subscriptions are limited by policy to roughly five regions. Read the allowed list before attempting:

```
Azure Portal → Policy → Assignments → "Allowed resource deployment regions" → Parameters
```

### Two dbt profiles, deliberately

`dbt/torino_pulse/profiles.yml` uses `host: postgres` for the Docker service name. `~/.dbt/profiles.yml` uses `host: localhost` for local development. They are not interchangeable.

### BashOperator instead of Astronomer Cosmos

Cosmos does not install cleanly alongside Airflow 2.10.4 — pip enters dependency backtracking without converging. Two BashOperator tasks work fine:

```python
dbt_run >> dbt_test
```

---

## Known gaps

**`_PIP_ADDITIONAL_REQUIREMENTS` is development-only.** It reinstalls on every container start. A custom image is the correct fix.

**`profiles.yml` contains a plaintext password.** The fix is `password: "{{ env_var('DBT_PASSWORD') }}"`.

**`shapes.txt` is not loaded** — route geometry for mapping is available but unused.

**Collection gaps.** Two outages are visible in the data from the redis incident and a container restart. Hours affected are flagged by `is_sparse_hour` in `fct_hourly_service`.

---

## Roadmap

- [ ] Re-run analysis on a full week, including a weekend and a rain event
- [ ] Custom Airflow image, replacing `_PIP_ADDITIONAL_REQUIREMENTS`
- [ ] Secrets via environment variables
- [ ] Load `shapes.txt` for route map visualisation
- [ ] GitHub Actions running `dbt test` on PRs

---

## Stack

Airflow 2.10.4 · dbt-core 1.8.2 / dbt-postgres 1.8.2 · Postgres 13 · Redis 7.2 · Docker Compose · Python 3.12 · Metabase · Streamlit · NVIDIA Llama 3.1-8B · Power BI (DirectQuery) · Oracle Cloud ARM (Ubuntu 24.04)

## Data attribution

Transit data © [GTT Torino](https://www.gtt.to.it/), published as open data.
Weather data from [Open-Meteo](https://open-meteo.com/), CC BY 4.0.
