from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import os
import requests
import psycopg2
from google.transit import gtfs_realtime_pb2 as gtfs

TRIP_REL = gtfs.TripDescriptor.ScheduleRelationship
STOP_REL = gtfs.TripUpdate.StopTimeUpdate.ScheduleRelationship


def event_field(stu, event_name, field):
    """Return a field of arrival/departure only if GTT actually set it.
    Protobuf returns 0 for unset ints, which would look like 'on time'."""
    if not stu.HasField(event_name):
        return None
    event = getattr(stu, event_name)
    return getattr(event, field) if event.HasField(field) else None


def fetch_and_store_trip_updates():
    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        dbname="torino_pulse",
        user="airflow",
        password=os.environ["POSTGRES_PASSWORD"]
    )
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw_trip_updates (
            id SERIAL PRIMARY KEY,
            trip_id TEXT,
            route_id TEXT,
            trip_start_time TEXT,
            trip_start_date TEXT,
            vehicle_id TEXT,
            vehicle_label TEXT,
            license_plate TEXT,
            wheelchair_accessible TEXT,
            stop_sequence INT,
            arrival_delay_seconds INT,
            departure_delay_seconds INT,
            gps_timestamp BIGINT,
            fetched_at TIMESTAMP
        )
    """)
    cur.execute("""
        ALTER TABLE raw_trip_updates
            ADD COLUMN IF NOT EXISTS trip_schedule_relationship TEXT,
            ADD COLUMN IF NOT EXISTS stop_schedule_relationship TEXT,
            ADD COLUMN IF NOT EXISTS arrival_time BIGINT,
            ADD COLUMN IF NOT EXISTS departure_time BIGINT,
            ADD COLUMN IF NOT EXISTS feed_timestamp BIGINT
    """)
    conn.commit()

    url = "https://percorsieorari.gtt.to.it/das_gtfsrt/trip_update.aspx"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    feed = gtfs.FeedMessage()
    feed.ParseFromString(response.content)

    fetched_at = datetime.now(timezone.utc).replace(tzinfo=None)
    feed_timestamp = feed.header.timestamp if feed.header.HasField('timestamp') else None
    count = 0
    trips_without_stops = 0

    insert_sql = """
        INSERT INTO raw_trip_updates
        (trip_id, route_id, trip_start_time, trip_start_date,
         vehicle_id, vehicle_label, license_plate, wheelchair_accessible,
         stop_sequence, arrival_delay_seconds, departure_delay_seconds,
         gps_timestamp, fetched_at,
         trip_schedule_relationship, stop_schedule_relationship,
         arrival_time, departure_time, feed_timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    for entity in feed.entity:
        if not entity.HasField('trip_update'):
            continue
        tu = entity.trip_update

        trip_fields = (
            tu.trip.trip_id,
            tu.trip.route_id,
            tu.trip.start_time,
            tu.trip.start_date,
            tu.vehicle.id,
            tu.vehicle.label,
            tu.vehicle.license_plate,
            str(tu.vehicle.wheelchair_accessible),
        )
        trip_rel = TRIP_REL.Name(tu.trip.schedule_relationship)

        # Cancelled trips usually carry no stop_time_update at all.
        # Store one trip-level row so they are not silently lost.
        if len(tu.stop_time_update) == 0:
            cur.execute(insert_sql, trip_fields + (
                None, None, None,
                tu.timestamp, fetched_at,
                trip_rel, None,
                None, None, feed_timestamp,
            ))
            count += 1
            trips_without_stops += 1
            continue

        for stu in tu.stop_time_update:
            cur.execute(insert_sql, trip_fields + (
                stu.stop_sequence,
                event_field(stu, 'arrival', 'delay'),
                event_field(stu, 'departure', 'delay'),
                tu.timestamp,
                fetched_at,
                trip_rel,
                STOP_REL.Name(stu.schedule_relationship),
                event_field(stu, 'arrival', 'time'),
                event_field(stu, 'departure', 'time'),
                feed_timestamp,
            ))
            count += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"Inserted {count} rows ({trips_without_stops} trip-level rows without stop updates)")


default_args = {
    'owner': 'iman',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='gtt_trip_updates_ingestion',
    default_args=default_args,
    start_date=datetime(2026, 8, 18),
    schedule_interval=timedelta(minutes=5),
    catchup=False,
    tags=['torino-pulse', 'gtt', 'delays'],
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_and_store_trip_updates',
        python_callable=fetch_and_store_trip_updates,
    )
