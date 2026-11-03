from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import csv
import io
import os
import zipfile

import psycopg2
import requests


# Small reference files loaded row-by-row; stop_times is handled separately
# because it is ~70 MB and needs COPY.
FILES_TO_LOAD = {
    'routes.txt': 'gtfs_routes',
    'stops.txt': 'gtfs_stops',
    'trips.txt': 'gtfs_trips',
    'agency.txt': 'gtfs_agency',
    'calendar.txt': 'gtfs_calendar',
    'calendar_dates.txt': 'gtfs_calendar_dates',
}

LARGE_FILES = {
    'stop_times.txt': 'gtfs_stop_times',
}

GTFS_URL = "https://www.gtt.to.it/open_data/gtt_gtfs.zip"


def _create_table_from_header(cur, table_name, columns):
    """
    Recreate a table with one TEXT column per CSV header field.

    CASCADE is required because dbt staging views (stg_routes, stg_stops, ...)
    are built on top of these tables. Those views are recreated by the next
    dbt run, so dropping them here is safe.
    """
    cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
    col_defs = ", ".join([f'"{c}" TEXT' for c in columns])
    cur.execute(f"CREATE TABLE {table_name} ({col_defs}, loaded_at TIMESTAMP)")


def load_static_gtfs():
    response = requests.get(GTFS_URL, timeout=300)
    archive = zipfile.ZipFile(io.BytesIO(response.content))

    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        dbname="torino_pulse",
        user="airflow",
        password=os.environ["POSTGRES_PASSWORD"]
    )
    cur = conn.cursor()
    loaded_at = datetime.now()

    # --- Small files: row-by-row is fine at this size ---
    for filename, table_name in FILES_TO_LOAD.items():
        if filename not in archive.namelist():
            print(f"Skipping {filename} (not in archive)")
            continue

        with archive.open(filename) as f:
            text = io.TextIOWrapper(f, encoding='utf-8', errors='replace')
            reader = csv.DictReader(text)
            columns = reader.fieldnames
            if not columns:
                continue

            _create_table_from_header(cur, table_name, columns)

            placeholders = ", ".join(["%s"] * (len(columns) + 1))
            col_names = ", ".join([f'"{c}"' for c in columns]) + ", loaded_at"

            rows = 0
            for row in reader:
                cur.execute(
                    f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})",
                    [row.get(c) for c in columns] + [loaded_at]
                )
                rows += 1

            conn.commit()
            print(f"Loaded {rows} rows into {table_name}")

    # --- Large files: COPY straight from the zip stream ---
    # Row-by-row INSERT on stop_times (~1.5M rows) takes tens of minutes.
    # COPY finishes in seconds because it bypasses the statement planner.
    for filename, table_name in LARGE_FILES.items():
        if filename not in archive.namelist():
            print(f"Skipping {filename} (not in archive)")
            continue

        # First pass: read only the header so we know the schema
        with archive.open(filename) as f:
            header_line = io.TextIOWrapper(f, encoding='utf-8', errors='replace').readline()
        columns = next(csv.reader([header_line]))

        _create_table_from_header(cur, table_name, columns)
        conn.commit()

        # Second pass: stream the body into COPY
        with archive.open(filename) as f:
            text = io.TextIOWrapper(f, encoding='utf-8', errors='replace')
            next(text)  # discard header
            col_list = ", ".join([f'"{c}"' for c in columns])
            cur.copy_expert(
                f"COPY {table_name} ({col_list}) FROM STDIN WITH (FORMAT csv, QUOTE '\"')",
                text
            )
        conn.commit()

        cur.execute(f"UPDATE {table_name} SET loaded_at = %s", (loaded_at,))
        conn.commit()

        cur.execute(f"SELECT count(*) FROM {table_name}")
        print(f"Loaded {cur.fetchone()[0]} rows into {table_name} via COPY")

    # Indexes that make the downstream joins usable
    cur.execute("CREATE INDEX IF NOT EXISTS gtfs_stop_times_trip_idx ON gtfs_stop_times (trip_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS gtfs_stop_times_stop_idx ON gtfs_stop_times (stop_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS gtfs_trips_trip_idx ON gtfs_trips (trip_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS gtfs_stops_stop_idx ON gtfs_stops (stop_id)")
    conn.commit()
    print("Indexes ensured.")

    cur.close()
    conn.close()


# --- History tables ---------------------------------------------------------
#
# load_static_gtfs() drops and recreates gtfs_* every day, so those tables only
# ever describe the feed GTT published today. GTT rotates trip_ids when it
# publishes a new feed, which means realtime observations recorded under an
# older feed can no longer be resolved to a route. The history tables below are
# never dropped: each daily run upserts today's feed into them, so every
# trip_id / route_id that has ever been published stays resolvable.
#
# They only accumulate from the first run onward. Observations recorded before
# that, under feeds that were not captured, cannot be resolved retroactively.

HISTORY_DDL = """
CREATE TABLE IF NOT EXISTS gtfs_trips_history (
    trip_id        TEXT      NOT NULL,
    route_id       TEXT      NOT NULL,
    direction_id   TEXT,
    trip_headsign  TEXT,
    first_seen_at  TIMESTAMP NOT NULL,
    last_seen_at   TIMESTAMP NOT NULL,
    PRIMARY KEY (trip_id, route_id)
);

CREATE TABLE IF NOT EXISTS gtfs_routes_history (
    route_id          TEXT      NOT NULL PRIMARY KEY,
    agency_id         TEXT,
    route_short_name  TEXT,
    route_long_name   TEXT,
    route_desc        TEXT,
    route_type        TEXT,
    route_url         TEXT,
    route_color       TEXT,
    route_text_color  TEXT,
    first_seen_at     TIMESTAMP NOT NULL,
    last_seen_at      TIMESTAMP NOT NULL
);
"""

# DISTINCT ON guards against duplicate keys within one feed, which would make
# ON CONFLICT fail with "cannot affect row a second time".
UPSERT_TRIPS_SQL = """
INSERT INTO gtfs_trips_history
    (trip_id, route_id, direction_id, trip_headsign, first_seen_at, last_seen_at)
SELECT DISTINCT ON (trip_id, route_id)
    trip_id, route_id, direction_id, trip_headsign, %(ts)s, %(ts)s
FROM gtfs_trips
WHERE trip_id IS NOT NULL AND route_id IS NOT NULL
ORDER BY trip_id, route_id
ON CONFLICT (trip_id, route_id) DO UPDATE SET
    direction_id  = EXCLUDED.direction_id,
    trip_headsign = EXCLUDED.trip_headsign,
    last_seen_at  = EXCLUDED.last_seen_at
"""

# Latest feed wins for descriptive attributes; first_seen_at is kept.
UPSERT_ROUTES_SQL = """
INSERT INTO gtfs_routes_history
    (route_id, agency_id, route_short_name, route_long_name, route_desc,
     route_type, route_url, route_color, route_text_color, first_seen_at, last_seen_at)
SELECT DISTINCT ON (route_id)
    route_id, agency_id, route_short_name, route_long_name, route_desc,
    route_type, route_url, route_color, route_text_color, %(ts)s, %(ts)s
FROM gtfs_routes
WHERE route_id IS NOT NULL
ORDER BY route_id
ON CONFLICT (route_id) DO UPDATE SET
    agency_id        = EXCLUDED.agency_id,
    route_short_name = EXCLUDED.route_short_name,
    route_long_name  = EXCLUDED.route_long_name,
    route_desc       = EXCLUDED.route_desc,
    route_type       = EXCLUDED.route_type,
    route_url        = EXCLUDED.route_url,
    route_color      = EXCLUDED.route_color,
    route_text_color = EXCLUDED.route_text_color,
    last_seen_at     = EXCLUDED.last_seen_at
"""


def update_gtfs_history():
    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        dbname="torino_pulse",
        user="airflow",
        password=os.environ["POSTGRES_PASSWORD"]
    )
    cur = conn.cursor()

    # Refuse to run against an empty feed, so a failed load can't silently
    # leave the history looking up to date.
    for table in ("gtfs_trips", "gtfs_routes"):
        cur.execute(f"SELECT count(*) FROM {table}")
        if cur.fetchone()[0] == 0:
            raise RuntimeError(f"{table} is empty; refusing to update history")

    cur.execute(HISTORY_DDL)
    params = {"ts": datetime.now()}
    cur.execute(UPSERT_TRIPS_SQL, params)
    cur.execute(UPSERT_ROUTES_SQL, params)
    conn.commit()

    cur.execute("SELECT count(*) FROM gtfs_trips_history")
    print(f"gtfs_trips_history: {cur.fetchone()[0]} rows")
    cur.execute("SELECT count(*) FROM gtfs_routes_history")
    print(f"gtfs_routes_history: {cur.fetchone()[0]} rows")

    # stg_gtfs_trips resolves each trip_id to its most recently seen route.
    # That is only safe if GTT does not reuse a trip_id for a different route,
    # so report it if it ever happens.
    cur.execute("""
        SELECT count(*) FROM (
            SELECT trip_id FROM gtfs_trips_history
            GROUP BY trip_id HAVING count(*) > 1
        ) t
    """)
    reused = cur.fetchone()[0]
    if reused:
        print(f"WARNING: {reused} trip_id(s) map to more than one route_id in history")
    else:
        print("No trip_id maps to more than one route_id.")

    cur.close()
    conn.close()


default_args = {
    'owner': 'iman',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='gtt_static_gtfs_ingestion',
    default_args=default_args,
    start_date=datetime(2026, 8, 18),
    schedule_interval='@daily',
    catchup=False,
    tags=['torino-pulse', 'gtt', 'static'],
) as dag:

    load_task = PythonOperator(
        task_id='load_static_gtfs',
        python_callable=load_static_gtfs,
    )

    history_task = PythonOperator(
        task_id='update_gtfs_history',
        python_callable=update_gtfs_history,
    )

    load_task >> history_task