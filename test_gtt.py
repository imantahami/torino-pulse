import requests
from google.transit import gtfs_realtime_pb2

# --- Vehicle Positions ---
print("=" * 50)
print("VEHICLE POSITIONS")
print("=" * 50)

url_vp = "https://percorsieorari.gtt.to.it/das_gtfsrt/vehicle_position.aspx"
response = requests.get(url_vp)
feed = gtfs_realtime_pb2.FeedMessage()
feed.ParseFromString(response.content)

print(f"Total entities: {len(feed.entity)}\n")

for entity in feed.entity[:3]:
    print("--- Entity ---")
    print(entity)

# --- Trip Updates ---
print("\n" + "=" * 50)
print("TRIP UPDATES")
print("=" * 50)

url_tu = "https://percorsieorari.gtt.to.it/das_gtfsrt/trip_update.aspx"
response_tu = requests.get(url_tu)
feed_tu = gtfs_realtime_pb2.FeedMessage()
feed_tu.ParseFromString(response_tu.content)

print(f"Total entities: {len(feed_tu.entity)}\n")

for entity in feed_tu.entity[:2]:
    print("--- Entity ---")
    print(entity)