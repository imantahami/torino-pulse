from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import psycopg2
from google.transit import gtfs_realtime_pb2


def fetch_and_store_vehicle_positions():
    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        dbname="torino_pulse",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw_vehicle_positions (
            id SERIAL PRIMARY KEY,
            vehicle_id TEXT,
            latitude FLOAT,
            longitude FLOAT,
            fetched_at TIMESTAMP
        )
    """)
    conn.commit()

    url = "https://percorsieorari.gtt.to.it/das_gtfsrt/vehicle_position.aspx"
    response = requests.get(url)
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)

    fetched_at = datetime.now()
    count = 0

    for entity in feed.entity:
        if entity.HasField('vehicle'):
            vehicle_id = entity.vehicle.vehicle.id
            lat = entity.vehicle.position.latitude
            lon = entity.vehicle.position.longitude

            cur.execute("""
                INSERT INTO raw_vehicle_positions (vehicle_id, latitude, longitude, fetched_at)
                VALUES (%s, %s, %s, %s)
            """, (vehicle_id, lat, lon, fetched_at))
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