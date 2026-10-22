# Torino Pulse
![Airflow](https://img.shields.io/badge/Airflow-2.10.4-017CEE?logo=apache-airflow)
![dbt](https://img.shields.io/badge/dbt-1.8.2-FF694B?logo=dbt)
![Postgres](https://img.shields.io/badge/Postgres-13-336791?logo=postgresql)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)
![Oracle Cloud](https://img.shields.io/badge/Oracle%20Cloud-ARM%20Free-F80000?logo=oracle)
![Live](https://img.shields.io/badge/Dashboard-Live-brightgreen)

A production data pipeline that continuously ingests live public transit and weather data for Turin, Italy, transforms it through a layered dbt model, and exposes findings through live dashboards.

Data has been collecting since **19 August 2026** and updates every 5–15 minutes around the clock — roughly 437,000 vehicle-position records a week (see [Ingestion volume](#ingestion-volume-by-day-of-week) in Findings).


**Live dashboard:**
[Turin Transit Dashboard](http://84.8.253.68:3000/public/dashboard/c893e417-08cc-4a87-b405-8a33fba2a81a)
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
              ┌────────────▼────────────┐
              │  Metabase               │
              │  Live dashboards        │
              │  :3000                  │
              └─────────────────────────┘
```

Everything runs in Docker Compose on one Oracle Cloud VM.Standard.A1.Flex (2 OCPU ARM64, 12 GB RAM, 100 GB disk) in the Italy North (Turin) region — permanently free.

---

## Data sources

| Source | Format | Interval | What it gives you |
|---|---|---|---|
| `vehicle_position.aspx` | GTFS-RT protobuf | 5 min | Vehicle ID, route, trip, lat/lon, bearing, GPS timestamp |
| `trip_update.aspx` | GTFS-RT protobuf | 5 min | Per-stop arrival delay in seconds, license plate, wheelchair accessibility |
| `alerts.aspx` | GTFS-RT protobuf | 15 min | Service disruptions: cause, effect, severity, affected routes and stops |
| `gtt_gtfs.zip` | GTFS static | daily | 204 routes, 6,894 stops, 38,824 trips, 1.1M stop_times |
| Open-Meteo | JSON | 15 min | Temperature, precipitation, wind speed, WMO weather code |

No API keys required. The realtime feeds carry fewer fields than the GTFS-RT spec allows — `current_status`, `occupancy_status`, and `congestion_level` are all absent from GTT's output. Everything GTT does publish is stored.

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
| `fct_weather_delay_impact` | one row per (condition, precipitation, local hour) | Delay grouped into dry / rainy / foggy buckets |

---

## Data quality notes

### GPS dropouts
About 2.3% of vehicle positions report `latitude = 0, longitude = 0`. Filtered in staging.

### One third of trips report no delay data
33% of stop-time updates report `arrival_delay_seconds = 0`. Three checks decided how to treat them:

**Uniformity.** The zero rate is 31–34% at every stop position, from stop 2 through stop 15.

**Trip-level clustering.** 2,229 of 6,717 trips report zero at every single stop.

**Vehicle-level consistency.** 80% of vehicles sit firmly in one camp (always zero or never zero).

The evidence fits missing data encoded as `0`. `fct_route_performance` excludes them. This cannot be proven without documentation from GTT, so the model header states the reasoning rather than asserting a fact.

### First stop always reads zero
The first stop reports zero delay because the vehicle has not departed. The `is_en_route` flag (`stop_sequence > 1`) excludes it.

### Delay drifts negative along a trip
Median delay falls steadily with stop position: −25s at stop 2, −59s at stop 5, −98s at stop 15. Vehicles get progressively further ahead of schedule — suggesting padded timetables toward the end of routes.

### `fct_route_performance`'s window is shorter than the raw history
`is_latest_observation` in `stg_trip_updates` keeps one row per `(trip_id, stop_sequence)` — the most recent fetch, discarding earlier ones. GTT reuses the same `trip_id` for a given scheduled trip on every service day, so this collapses each trip's day-over-day history down to its latest occurrence. In practice, `fct_route_performance`'s `first_observed_at` currently reads 1 October 2026, even though `raw_trip_updates` holds data back to 19 August 2026 (see [Ingestion volume](#ingestion-volume-by-day-of-week)). The route-level findings should be read as "most recent ~3 weeks," not "the full collection window."

---

## Findings

Raw ingestion has run continuously since **19 August 2026** (26M+ `raw_trip_updates` rows to date). The route-level marts below (`fct_route_performance` and friends) keep only the *latest* observation per `(trip_id, stop_sequence)` — see [Data quality notes](#data-quality-notes) — and since GTT reuses the same `trip_id` on every service day, that collapses the punctuality numbers down to roughly the last 3 weeks of data (first observation in the current marts: **1 October 2026**). Findings below are current **as of 22 October 2026**; the queries behind every number are in [`analysis/findings.sql`](analysis/findings.sql) and are meant to be re-run as the window moves.

### Network-level punctuality vs European benchmarks

GTT's on-time performance (OTP), defined as arriving within ±60 seconds of schedule, sits well below European standards:

| Network | OTP |
|---|---|
| Hamburg HVV (best EU) | 93% |
| European average | ~75% |
| London TfL | 76% |
| **GTT Turin — tram** | **25.4%** |
| **GTT Turin — bus** | **28.7%** |

The low OTP reflects a structural pattern, not random variation: across 478,117 observations on 44 routes, the network runs systematically ahead of schedule (network median delay = −52s), which means timetables are padded and vehicles arrive early rather than on time. A rider who arrives at the scheduled departure time risks missing a vehicle that left early.

### Buses and trams are now statistically close

| Mode | Routes | Observations | OTP | pct_late | Median delay |
|---|---|---|---|---|---|
| Tram | 8 | 109,851 | 25.4% | 24.5% | −54s |
| Bus | 36 | 368,266 | 28.7% | 24.8% | −51s |

An earlier read of this data (13 days, ending 1 September) showed trams meaningfully behind buses. In the current window the gap has narrowed to a few points of OTP, and `pct_late` is essentially tied (24.5% vs 24.8%). Trams still have dedicated right-of-way, but that is no longer showing up as a clear punctuality advantage — worth re-checking again once more data accumulates rather than treating either reading as the final word.

### Most problematic routes

Routes with the highest rate of late arrivals (observations ≥ 200):

| Route | pct_late | p90 delay | stddev | observations |
|---|---|---|---|---|
| 17 | 48.5% | +561s | 1,647 | 17,051 |
| 17/ | 43.8% | +475s | 301 | 3,439 |
| 4 | 42.9% | +544s | 3,925 | 31,294 |
| 6 | 41.2% | +469s | 314 | 3,936 |
| 64 | 37.9% | +406s | 1,585 | 9,179 |
| 68+ | 36.2% | +393s | 353 | 1,146 |

Route 64 was already a known outlier in the first reading of this data (September) and still is — a persistent pattern rather than a one-off.

### Most unpredictable routes

Routes with the highest delay variance (stddev), meaning the schedule is least useful as a predictor:

| Route | stddev (seconds) | pct_late | observations |
|---|---|---|---|
| 36 | 4,415 | 25.0% | 10,520 |
| 4 | 3,925 | 42.9% | 31,294 |
| 5 | 3,488 | 29.2% | 15,202 |
| 35 | 3,249 | 12.9% | 7,994 |
| 3 | 3,069 | 10.6% | 10,062 |
| 60 | 2,913 | 32.3% | 6,124 |

A high stddev means the vehicle sometimes arrives very early and sometimes very late — the schedule gives riders almost no useful information. Route 36 topped this list in the earlier reading too.

### Most reliable routes

| Route | OTP | pct_late | stddev | observations |
|---|---|---|---|---|
| 12 | 40.0% | 23.6% | 207 | 7,165 |
| 58/ | 37.5% | 22.9% | 168 | 7,195 |
| 16 CD | 35.6% | 23.6% | 248 | 8,305 |
| 33 | 35.5% | 25.9% | 202 | 14,809 |
| 74 | 35.3% | 24.6% | 189 | 9,361 |
| 13+ | 35.2% | 20.5% | 195 | 1,579 |

### Fleet cycle

Average active vehicles by hour of day (hours with fewer than 1,500 position reports are excluded as partial/unreliable):

```
00:00  146   ┃━━━━━━━━━━                overnight low
05:00  268   ┃━━━━━━━━━━━━━━━━━━         service resumes
07:00  449   ┃━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
08:00  454   ┃━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  daily peak
10:00  413   ┃━━━━━━━━━━━━━━━━━━━━━━━━━━━━
14:00  385   ┃━━━━━━━━━━━━━━━━━━━━━━━━━━
18:00  406   ┃━━━━━━━━━━━━━━━━━━━━━━━━━━━━
20:00  368   ┃━━━━━━━━━━━━━━━━━━━━━━━━━
22:00  162   ┃━━━━━━━━━━━               night routes winding down
```

### Alerts

Detours account for 60% of every alert raised since collection began (591 of 982 distinct alerts), with 153 active as of this snapshot. The rest split across `OTHER_EFFECT` (256), `NO_EFFECT` (53), `MODIFIED_SERVICE` (49), and a long tail of rarer effects.

### Weather vs delay

| Condition | Hours observed | avg pct_late | avg median delay |
|---|---|---|---|
| Dry | 849 | 21.7% | −60s |
| Rainy | 73 | 23.1% | −63s |
| Foggy | 16 | 18.0% | −75s |

The differences here are small (a few points of `pct_late`, tens of seconds of median delay) and the rainy/foggy samples are thin (73 and 16 hours respectively) next to 849 dry hours. This reads as a weak or inconclusive weather effect, not a confirmed one — there isn't enough data yet to say weather meaningfully moves GTT's delays either way.

### Ingestion volume by day of week

Averaged across the full collection window (19 August – 22 October 2026), vehicle-position volume is lower on weekends, when fewer vehicles run:

| Day | Avg. position reports/day |
|---|---|
| Tuesday | ~75,700 |
| Wednesday | ~70,200 |
| Friday | ~66,900 |
| Monday | ~66,800 |
| Thursday | ~66,300 |
| Saturday | ~55,800 |
| Sunday | ~35,900 |

That's roughly **437,000 vehicle-position records a week**, ingested automatically on a 5-minute schedule with no manual intervention.

---

## Running it

### Requirements
- Docker + Docker Compose
- 4 GB RAM minimum (12 GB recommended — see [Swap note](#swap-is-mandatory-on-a-4-gb-vm))

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

### Connecting Power BI

Postgres is not exposed to the internet. Open an SSH tunnel instead:

```bash
ssh -i <key> -L 5432:localhost:5432 ubuntu@<server-ip>
```

Then point Power BI at `localhost:5432`, database `torino_pulse`, mode `DirectQuery`.

---

## Implementation notes

### A dependency conflict that reported itself as healthy

After adding `dbt-core` to `_PIP_ADDITIONAL_REQUIREMENTS`, every DAG stopped producing data. All six containers showed `healthy`. The UI showed all DAGs unpaused. Nothing said broken.

The scheduler log had it:

```
AttributeError: module 'redis' has no attribute 'client'
```

Installing dbt had pulled in a `redis` version incompatible with the `kombu` build Celery uses. Fixed by pinning `redis==4.6.0`. The lesson: green health checks describe the process, not the work.

### UID mismatch makes dbt exit silently

`dbt --version` worked. `dbt run` printed nothing and exited with code 2 — no traceback, no log file.

The project directory was owned by UID 1000 (host); the container runs as UID 50000. dbt died before it had anywhere to write the error.

Fixed by:
```bash
echo "AIRFLOW_UID=1000" > .env
sudo chown -R 1000:1000 ~/torino-pulse
```

### Deduplicating alerts on ingest

`raw_alerts` grew at roughly 38 MB/hour. Each 15-minute fetch re-inserted all active alerts across every affected entity. The fix hashes content and upserts:

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

`stop_times.txt` is 70 MB and 1.1M rows. Row-by-row INSERT takes tens of minutes. Streaming into COPY takes 24 seconds.

### CASCADE when reloading reference tables

The static GTFS loader recreates tables daily. Once dbt staging views existed on top of them, `DROP TABLE` started failing with dependency errors. `DROP TABLE ... CASCADE` is correct here — the views are rebuilt by the next dbt run.

### Migrating from Azure to Oracle Cloud

Data was migrated via `pg_dump` on the source, `scp` through a laptop as intermediary, and `pg_restore` on the destination. 842,864 positions and 5.2M trip_updates transferred without loss.

### Oracle Cloud ARM

Oracle Always Free provides 2 OCPU ARM64 and 12 GB RAM permanently. The Italy North (Turin) region was selected — the datacenter is in the same city as the transit network it monitors.

Oracle's Always Free ARM allocation was reduced from 4 OCPU / 24 GB to 2 OCPU / 12 GB in June 2026 with no public announcement.

### Swap is mandatory on a 4 GB VM

Airflow's Compose stack asks for 4 GB. A 3.8 GB usable VM dies under load. Four GB of swap fixes it. Setting `AIRFLOW__CORE__LOAD_EXAMPLES: 'false'` cut swap usage from 2.6 GB to 800 MB.

### BashOperator instead of Astronomer Cosmos

Cosmos does not install cleanly alongside Airflow 2.10.4 — pip enters dependency backtracking without converging. Two BashOperator tasks work fine and install in seconds.

---

## Known gaps

**`_PIP_ADDITIONAL_REQUIREMENTS` is development-only.** A custom image is the correct fix.

**`shapes.txt` is not loaded** — route geometry for mapping is available but unused.

---

## Roadmap

- [x] Re-run analysis after 30 days of collection
- [ ] Custom Airflow image, replacing `_PIP_ADDITIONAL_REQUIREMENTS`
- [x] Secrets via environment variables
- [ ] Load `shapes.txt` for route map visualisation
- [ ] GitHub Actions running `dbt test` on PRs

---

## Stack

Airflow 2.10.4 · dbt-core 1.8.2 · Postgres 13 · Redis 7.2 · Docker Compose · Python 3.12 · Metabase · Oracle Cloud ARM (Ubuntu 24.04)

## Data attribution

Transit data © [GTT Torino](https://www.gtt.to.it/), published as open data.
Weather data from [Open-Meteo](https://open-meteo.com/), CC BY 4.0.
