from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import requests
import psycopg2
from google.transit import gtfs_realtime_pb2
from gtt_common import store_feed_snapshot

TRIP_REL = gtfs_realtime_pb2.TripDescriptor.ScheduleRelationship


def fetch_and_store_vehicle_positions():
    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        dbname="torino_pulse",
        user="airflow",
        password=os.environ["POSTGRES_PASSWORD"]
    )
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw_vehicle_positions (
            id SERIAL PRIMARY KEY,
            vehicle_id TEXT,
            vehicle_label TEXT,
            route_id TEXT,
            trip_id TEXT,
            trip_start_time TEXT,
            trip_start_date TEXT,
            latitude FLOAT,
            longitude FLOAT,
            bearing FLOAT,
            gps_timestamp BIGINT,
            fetched_at TIMESTAMP
        )
    """)

    # Add new columns if table already exists from previous version.
    # Only ALTER when a column is actually missing: even with IF NOT EXISTS,
    # ALTER TABLE takes an ACCESS EXCLUSIVE lock, which queues behind long
    # dbt queries and blocks every other reader and writer of the table.
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = current_schema() AND table_name = 'raw_vehicle_positions'"
    )
    existing_columns = {row[0] for row in cur.fetchall()}
    for col, coltype in [
        ("vehicle_label", "TEXT"),
        ("route_id", "TEXT"),
        ("trip_id", "TEXT"),
        ("trip_start_time", "TEXT"),
        ("trip_start_date", "TEXT"),
        ("bearing", "FLOAT"),
        ("gps_timestamp", "BIGINT"),
        ("entity_id", "TEXT"),
        ("trip_schedule_relationship", "TEXT"),
        ("feed_timestamp", "BIGINT"),
    ]:
        if col not in existing_columns:
            # Waiting for this lock behind a long dbt query would block every other
            # query on the table (lock convoy), so fail fast and let the retry
            # handle it. Normally the columns are added once by the migration script.
            cur.execute("SET LOCAL lock_timeout = '5s'")
            cur.execute(f"ALTER TABLE raw_vehicle_positions ADD COLUMN {col} {coltype}")

    conn.commit()

    url = "https://percorsieorari.gtt.to.it/das_gtfsrt/vehicle_position.aspx"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)

    fetched_at = datetime.now()
    feed_timestamp = feed.header.timestamp if feed.header.HasField('timestamp') else None
    store_feed_snapshot(cur, 'vehicle_position', fetched_at, response.content, feed_timestamp)
    count = 0

    for entity in feed.entity:
        if entity.HasField('vehicle'):
            v = entity.vehicle

            cur.execute("""
                INSERT INTO raw_vehicle_positions
                (vehicle_id, vehicle_label, route_id, trip_id, trip_start_time,
                 trip_start_date, latitude, longitude, bearing, gps_timestamp, fetched_at,
                 entity_id, trip_schedule_relationship, feed_timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                v.vehicle.id,
                v.vehicle.label,
                v.trip.route_id,
                v.trip.trip_id,
                v.trip.start_time,
                v.trip.start_date,
                v.position.latitude,
                v.position.longitude,
                v.position.bearing,
                v.timestamp,
                fetched_at,
                entity.id,
                TRIP_REL.Name(v.trip.schedule_relationship),
                feed_timestamp
            ))
            count += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"Inserted {count} rows")


default_args = {
    'owner': 'iman',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='gtt_vehicle_positions_ingestion',
    default_args=default_args,
    start_date=datetime(2026, 8, 18),
    schedule_interval=timedelta(minutes=5),
    catchup=False,
    tags=['torino-pulse', 'gtt'],
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_and_store_vehicle_positions',
        python_callable=fetch_and_store_vehicle_positions,
    )