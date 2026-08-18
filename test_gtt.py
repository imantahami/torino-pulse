import requests
from google.transit import gtfs_realtime_pb2

url = "https://percorsieorari.gtt.to.it/das_gtfsrt/alerts.aspx"
response = requests.get(url)
print("Status:", response.status_code)

feed = gtfs_realtime_pb2.FeedMessage()
feed.ParseFromString(response.content)

print(f"Total alerts: {len(feed.entity)}\n")

for entity in feed.entity[:3]:
    print("--- Alert ---")
    print(entity)