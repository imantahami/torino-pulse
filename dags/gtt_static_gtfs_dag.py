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

# Everything else in GTT's zip. Loaded after the core tables, and a failure
# here is logged but never stops the core tables or the history update:
# these files are not needed by the dbt models yet, they are kept so the
# data exists later (vehicle catalogue, shapes, timetables, fares, feed_info).
EXTRA_FILES = {
    'feed_info.txt': 'gtfs_feed_info',
    'stop_attributes.txt': 'gtfs_stop_attributes',
    'vehicles.txt': 'gtfs_vehicles',
    'vehicle_categories.txt': 'gtfs_vehicle_categories',
    'trips_booking.txt': 'gtfs_trips_booking',
    'timetables.txt': 'gtfs_timetables',
    'timetable_pages.txt': 'gtfs_timetable_pages',
    'timetable_stop_order.txt': 'gtfs_timetable_stop_order',
    'fare_attributes.txt': 'gtfs_fare_attributes',
    'fare_rules.txt': 'gtfs_fare_rules',
}

EXTRA_LARGE_FILES = {
    'shapes.txt': 'gtfs_shapes',
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


def _load_small_file(cur, conn, archive, filename, table_name, loaded_at):
    """Row-by-row load of a small CSV from the zip. Returns the row count."""
    with archive.open(filename) as f:
        # utf-8-sig drops a byte-order mark if GTT ever adds one
        text = io.TextIOWrapper(f, encoding='utf-8-sig', errors='replace')
        reader = csv.DictReader(text)
        columns = reader.fieldnames
        if not columns:
            return 0

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
    return rows


def _load_large_file(cur, conn, archive, filename, table_name, loaded_at):
    """COPY a large CSV straight from the zip stream.

    Row-by-row INSERT on stop_times (~1.5M rows) takes tens of minutes.
    COPY finishes in seconds because it bypasses the statement planner.
    """
    # First pass: read only the header so we know the schema
    with archive.open(filename) as f:
        header_line = io.TextIOWrapper(f, encoding='utf-8-sig', errors='replace').readline()
    columns = next(csv.reader([header_line]))

    _create_table_from_header(cur, table_name, columns)
    conn.commit()

    # Second pass: stream the body into COPY
    with archive.open(filename) as f:
        text = io.TextIOWrapper(f, encoding='utf-8-sig', errors='replace')
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


def load_static_gtfs():
    response = requests.get(GTFS_URL, timeout=300)
    response.raise_for_status()
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
    names = archive.namelist()

    # --- Core tables: a failure here fails the task ---
    for filename, table_name in FILES_TO_LOAD.items():
        if filename not in names:
            print(f"Skipping {filename} (not in archive)")
            continue
        _load_small_file(cur, conn, archive, filename, table_name, loaded_at)

    for filename, table_name in LARGE_FILES.items():
        if filename not in names:
            print(f"Skipping {filename} (not in archive)")
            continue
        _load_large_file(cur, conn, archive, filename, table_name, loaded_at)

    # Indexes that make the downstream joins usable
    cur.execute("CREATE INDEX IF NOT EXISTS gtfs_stop_times_trip_idx ON gtfs_stop_times (trip_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS gtfs_stop_times_stop_idx ON gtfs_stop_times (stop_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS gtfs_trips_trip_idx ON gtfs_trips (trip_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS gtfs_stops_stop_idx ON gtfs_stops (stop_id)")
    conn.commit()
    print("Indexes ensured.")

    # --- Extra files: best effort, never fatal ---
    for filename, table_name in EXTRA_FILES.items():
        if filename not in names:
            print(f"Skipping {filename} (not in archive)")
            continue
        try:
            _load_small_file(cur, conn, archive, filename, table_name, loaded_at)
        except Exception as exc:
            conn.rollback()
            print(f"WARNING: could not load {filename}: {exc}")

    for filename, table_name in EXTRA_LARGE_FILES.items():
        if filename not in names:
            print(f"Skipping {filename} (not in archive)")
            continue
        try:
            _load_large_file(cur, conn, archive, filename, table_name, loaded_at)
            cur.execute("CREATE INDEX IF NOT EXISTS gtfs_shapes_shape_idx ON gtfs_shapes (shape_id)")
            conn.commit()
        except Exception as exc:
            conn.rollback()
            print(f"WARNING: could not load {filename}: {exc}")

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

-- Every stop time ever published. GTT rotates trip_ids, so this is the only way
-- to look up the scheduled time of a stop for an observation recorded under an
-- older feed. Rows are only ever added (a changed time is a new row).
CREATE TABLE IF NOT EXISTS gtfs_stop_times_history (
    trip_id         TEXT      NOT NULL,
    stop_sequence   INT       NOT NULL,
    stop_id         TEXT,
    arrival_time    TEXT,
    departure_time  TEXT,
    first_seen_at   TIMESTAMP NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS gtfs_stop_times_history_uq
    ON gtfs_stop_times_history (trip_id, stop_sequence, COALESCE(arrival_time, ''), COALESCE(departure_time, ''));

-- Route geometry, same idea.
CREATE TABLE IF NOT EXISTS gtfs_shapes_history (
    shape_id           TEXT      NOT NULL,
    shape_pt_sequence  INT       NOT NULL,
    shape_pt_lat       TEXT,
    shape_pt_lon       TEXT,
    first_seen_at      TIMESTAMP NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS gtfs_shapes_history_uq
    ON gtfs_shapes_history (shape_id, shape_pt_sequence, COALESCE(shape_pt_lat, ''), COALESCE(shape_pt_lon, ''));

-- Every distinct row of the small reference tables (stops, vehicles, vehicle
-- categories, calendar, feed_info, ...), so a renamed stop or a changed vehicle
-- record keeps its past versions. One row per (table, content hash).
CREATE TABLE IF NOT EXISTS gtfs_snapshot_history (
    source_table   TEXT      NOT NULL,
    row_hash       TEXT      NOT NULL,
    row_data       JSONB     NOT NULL,
    first_seen_at  TIMESTAMP NOT NULL,
    last_seen_at   TIMESTAMP NOT NULL,
    PRIMARY KEY (source_table, row_hash)
);
"""

# Columns added to the history tables after they were first created.
# Only ALTER when a column is missing (ALTER TABLE takes an ACCESS EXCLUSIVE lock
# even with IF NOT EXISTS).
HISTORY_EXTRA_COLUMNS = {
    "gtfs_trips_history": [
        ("service_id", "TEXT"), ("trip_short_name", "TEXT"), ("block_id", "TEXT"),
        ("shape_id", "TEXT"), ("wheelchair_accessible", "TEXT"),
        ("bikes_allowed", "TEXT"), ("limited_route", "TEXT"),
    ],
    "gtfs_routes_history": [("route_sort_order", "TEXT")],
}

# Reference tables copied into gtfs_snapshot_history (when they exist and are not empty).
SNAPSHOT_TABLES = [
    "gtfs_agency", "gtfs_stops", "gtfs_stop_attributes", "gtfs_calendar",
    "gtfs_calendar_dates", "gtfs_feed_info", "gtfs_vehicles",
    "gtfs_vehicle_categories", "gtfs_trips_booking", "gtfs_timetables",
    "gtfs_timetable_pages", "gtfs_timetable_stop_order",
    "gtfs_fare_attributes", "gtfs_fare_rules",
]

# DISTINCT ON guards against duplicate keys within one feed, which would make
# ON CONFLICT fail with "cannot affect row a second time".
UPSERT_TRIPS_SQL = """
INSERT INTO gtfs_trips_history
    (trip_id, route_id, direction_id, trip_headsign,
     service_id, trip_short_name, block_id, shape_id,
     wheelchair_accessible, bikes_allowed, limited_route,
     first_seen_at, last_seen_at)
SELECT DISTINCT ON (trip_id, route_id)
    trip_id, route_id, direction_id, trip_headsign,
    service_id, trip_short_name, block_id, shape_id,
    wheelchair_accessible, bikes_allowed, limited_route,
    %(ts)s, %(ts)s
FROM gtfs_trips
WHERE trip_id IS NOT NULL AND route_id IS NOT NULL
ORDER BY trip_id, route_id
ON CONFLICT (trip_id, route_id) DO UPDATE SET
    direction_id          = EXCLUDED.direction_id,
    trip_headsign         = EXCLUDED.trip_headsign,
    service_id            = EXCLUDED.service_id,
    trip_short_name       = EXCLUDED.trip_short_name,
    block_id              = EXCLUDED.block_id,
    shape_id              = EXCLUDED.shape_id,
    wheelchair_accessible = EXCLUDED.wheelchair_accessible,
    bikes_allowed         = EXCLUDED.bikes_allowed,
    limited_route         = EXCLUDED.limited_route,
    last_seen_at          = EXCLUDED.last_seen_at
"""

# Latest feed wins for descriptive attributes; first_seen_at is kept.
UPSERT_ROUTES_SQL = """
INSERT INTO gtfs_routes_history
    (route_id, agency_id, route_short_name, route_long_name, route_desc,
     route_type, route_url, route_color, route_text_color, route_sort_order,
     first_seen_at, last_seen_at)
SELECT DISTINCT ON (route_id)
    route_id, agency_id, route_short_name, route_long_name, route_desc,
    route_type, route_url, route_color, route_text_color, route_sort_order,
    %(ts)s, %(ts)s
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
    route_sort_order = EXCLUDED.route_sort_order,
    last_seen_at     = EXCLUDED.last_seen_at
"""

# Rows only ever get added; DO NOTHING also tolerates duplicates inside one feed.
INSERT_STOP_TIMES_HISTORY_SQL = """
INSERT INTO gtfs_stop_times_history
    (trip_id, stop_sequence, stop_id, arrival_time, departure_time, first_seen_at)
SELECT trip_id, stop_sequence::int, stop_id, arrival_time, departure_time, %(ts)s
FROM gtfs_stop_times
WHERE trip_id IS NOT NULL AND stop_sequence ~ '^[0-9]+$'
ON CONFLICT (trip_id, stop_sequence, (COALESCE(arrival_time, '')), (COALESCE(departure_time, '')))
DO NOTHING
"""

INSERT_SHAPES_HISTORY_SQL = """
INSERT INTO gtfs_shapes_history
    (shape_id, shape_pt_sequence, shape_pt_lat, shape_pt_lon, first_seen_at)
SELECT shape_id, shape_pt_sequence::int, shape_pt_lat, shape_pt_lon, %(ts)s
FROM gtfs_shapes
WHERE shape_id IS NOT NULL AND shape_pt_sequence ~ '^[0-9]+$'
ON CONFLICT (shape_id, shape_pt_sequence, (COALESCE(shape_pt_lat, '')), (COALESCE(shape_pt_lon, '')))
DO NOTHING
"""

# Table names come from SNAPSHOT_TABLES above, never from input.
SNAPSHOT_SQL = """
INSERT INTO gtfs_snapshot_history
    (source_table, row_hash, row_data, first_seen_at, last_seen_at)
SELECT %(src)s, md5(j::text), j, %(ts)s, %(ts)s
FROM (SELECT DISTINCT to_jsonb(t) - 'loaded_at' AS j FROM {table} t) x
ON CONFLICT (source_table, row_hash) DO UPDATE SET last_seen_at = EXCLUDED.last_seen_at
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
    conn.commit()

    # Add newer columns only when missing, so the usual run takes no table lock.
    for table, columns in HISTORY_EXTRA_COLUMNS.items():
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = current_schema() AND table_name = %s", (table,))
        existing = {row[0] for row in cur.fetchall()}
        for name, coltype in columns:
            if name not in existing:
                cur.execute(f"ALTER TABLE {table} ADD COLUMN {name} {coltype}")
    conn.commit()

    params = {"ts": datetime.now()}
    cur.execute(UPSERT_TRIPS_SQL, params)
    cur.execute(UPSERT_ROUTES_SQL, params)
    conn.commit()

    # Extra history is best effort: it must not undo the trips/routes update above.
    def table_has_rows(table):
        cur.execute("SELECT to_regclass(%s)", (table,))
        if cur.fetchone()[0] is None:
            return False
        cur.execute(f"SELECT count(*) FROM {table}")
        return cur.fetchone()[0] > 0

    try:
        if table_has_rows("gtfs_stop_times"):
            cur.execute(INSERT_STOP_TIMES_HISTORY_SQL, params)
            print(f"gtfs_stop_times_history: {cur.rowcount} new rows")
        conn.commit()
    except Exception as exc:
        conn.rollback()
        print(f"WARNING: stop_times history failed: {exc}")

    try:
        if table_has_rows("gtfs_shapes"):
            cur.execute(INSERT_SHAPES_HISTORY_SQL, params)
            print(f"gtfs_shapes_history: {cur.rowcount} new rows")
        conn.commit()
    except Exception as exc:
        conn.rollback()
        print(f"WARNING: shapes history failed: {exc}")

    for table in SNAPSHOT_TABLES:
        try:
            if table_has_rows(table):
                cur.execute(SNAPSHOT_SQL.format(table=table), {**params, "src": table})
            conn.commit()
        except Exception as exc:
            conn.rollback()
            print(f"WARNING: snapshot of {table} failed: {exc}")

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