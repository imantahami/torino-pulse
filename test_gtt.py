import requests
import psycopg2
from datetime import datetime
from google.transit import gtfs_realtime_pb2

# Connect to Postgres
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="torino_pulse",
    user="airflow",
    password="airflow"
)
cur = conn.cursor()

# Create table if it doesn't exist
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

# Fetch live data from GTT
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
print(f"Inserted {count} rows into raw_vehicle_positions")

cur.close()
conn.close()