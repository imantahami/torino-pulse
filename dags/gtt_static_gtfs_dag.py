from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import zipfile
import io
import csv
import psycopg2


# Files we actually need (skipping huge stop_times.txt and shapes.txt)
FILES_TO_LOAD = {
    'routes.txt': 'gtfs_routes',
    'stops.txt': 'gtfs_stops',
    'trips.txt': 'gtfs_trips',
    'agency.txt': 'gtfs_agency',
    'calendar.txt': 'gtfs_calendar',
}


def load_static_gtfs():
    url = "https://www.gtt.to.it/open_data/gtt_gtfs.zip"
    response = requests.get(url, timeout=120)
    z = zipfile.ZipFile(io.BytesIO(response.content))

    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        dbname="torino_pulse",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()

    loaded_at = datetime.now()

    for filename, table_name in FILES_TO_LOAD.items():
        if filename not in z.namelist():
            print(f"Skipping {filename} (not in archive)")
            continue

        with z.open(filename) as f:
            text = io.TextIOWrapper(f, encoding='utf-8', errors='replace')
            reader = csv.DictReader(text)
            columns = reader.fieldnames

            if not columns:
                continue

            # Recreate table each run (static data, full refresh)
            cur.execute(f"DROP TABLE IF EXISTS {table_name}")
            col_defs = ", ".join([f'"{c}" TEXT' for c in columns])
            cur.execute(f"CREATE TABLE {table_name} ({col_defs}, loaded_at TIMESTAMP)")

            rows = 0
            placeholders = ", ".join(["%s"] * (len(columns) + 1))
            col_names = ", ".join([f'"{c}"' for c in columns]) + ", loaded_at"

            for row in reader:
                values = [row.get(c) for c in columns] + [loaded_at]
                cur.execute(
                    f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})",
                    values
                )
                rows += 1

            conn.commit()
            print(f"Loaded {rows} rows into {table_name}")

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