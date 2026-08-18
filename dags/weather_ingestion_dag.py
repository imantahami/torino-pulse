from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import psycopg2


def fetch_and_store_weather():
    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        dbname="torino_pulse",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw_weather (
            id SERIAL PRIMARY KEY,
            observed_at TIMESTAMP,
            temperature_c FLOAT,
            precipitation_mm FLOAT,
            wind_speed_kmh FLOAT,
            weather_code INT,
            fetched_at TIMESTAMP
        )
    """)
    conn.commit()

    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 45.0703,
        "longitude": 7.6869,
        "current": "temperature_2m,precipitation,wind_speed_10m,weather_code"
    }

    response = requests.get(url, params=params)
    data = response.json()["current"]

    cur.execute("""
        INSERT INTO raw_weather
        (observed_at, temperature_c, precipitation_mm, wind_speed_kmh, weather_code, fetched_at)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        data["time"],
        data["temperature_2m"],
        data["precipitation"],
        data["wind_speed_10m"],
        data["weather_code"],
        datetime.now()
    ))

    conn.commit()
    cur.close()
    conn.close()
    print(f"Inserted weather record: {data['temperature_2m']}C")


default_args = {
    'owner': 'iman',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='weather_ingestion',
    default_args=default_args,
    start_date=datetime(2026, 8, 18),
    schedule_interval=timedelta(minutes=15),
    catchup=False,
    tags=['torino-pulse', 'weather'],
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_and_store_weather',
        python_callable=fetch_and_store_weather,
    )