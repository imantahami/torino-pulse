from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import csv
import io
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
    """Recreate a table with one TEXT column per CSV header field."""
    cur.execute(f"DROP TABLE IF EXISTS {table_name}")
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
        password="airflow"
    )
    cur = conn.cursor()
    loaded_at = datetime.now()

    # --- Small files: readable, row-by-row, fine at this size ---
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

    # --- Large files: COPY straight from the stream ---
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