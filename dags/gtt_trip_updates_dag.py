from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import psycopg2
from google.transit import gtfs_realtime_pb2


def fetch_and_store_trip_updates():
    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        dbname="torino_pulse",
        user="airflow",
        password="airflow"
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
    conn.commit()

    url = "https://percorsieorari.gtt.to.it/das_gtfsrt/trip_update.aspx"
    response = requests.get(url)
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)

    fetched_at = datetime.now()
    count = 0

    for entity in feed.entity:
        if entity.HasField('trip_update'):
            tu = entity.trip_update

            for stu in tu.stop_time_update:
                arrival_delay = stu.arrival.delay if stu.HasField('arrival') else None
                departure_delay = stu.departure.delay if stu.HasField('departure') else None

                cur.execute("""
                    INSERT INTO raw_trip_updates
                    (trip_id, route_id, trip_start_time, trip_start_date,
                     vehicle_id, vehicle_label, license_plate, wheelchair_accessible,
                     stop_sequence, arrival_delay_seconds, departure_delay_seconds,
                     gps_timestamp, fetched_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    tu.trip.trip_id,
                    tu.trip.route_id,
                    tu.trip.start_time,
                    tu.trip.start_date,
                    tu.vehicle.id,
                    tu.vehicle.label,
                    tu.vehicle.license_plate,
                    str(tu.vehicle.wheelchair_accessible),
                    stu.stop_sequence,
                    arrival_delay,
                    departure_delay,
                    tu.timestamp,
                    fetched_at
                ))
                count += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"Inserted {count} stop-time updates")


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