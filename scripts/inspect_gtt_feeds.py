"""List every field GTT actually publishes, to compare with what we store.

Fetches each GTFS-Realtime feed once and counts every populated field path
(including header and any fields the protobuf schema does not know about),
then lists the files and columns in the static GTFS zip.

Run inside the Airflow container (it has requests and the protobuf bindings):
  docker compose exec -T airflow-worker python - < scripts/inspect_gtt_feeds.py
Read-only: it writes nothing to the database.
"""
import io
import zipfile
from collections import defaultdict

import requests
from google.protobuf.descriptor import FieldDescriptor
from google.transit import gtfs_realtime_pb2 as gtfs

BASE = "https://percorsieorari.gtt.to.it/das_gtfsrt/"
FEEDS = ["vehicle_position", "trip_update", "alerts"]
STATIC_URL = "https://www.gtt.to.it/open_data/gtt_gtfs.zip"
# Files the static DAG loads (dags/gtt_static_gtfs_dag.py)
LOADED = {"routes.txt", "stops.txt", "trips.txt", "agency.txt",
          "calendar.txt", "calendar_dates.txt", "stop_times.txt"}


def is_repeated(fd):
    # protobuf >= 5 has is_repeated; older versions only have label
    flag = getattr(fd, "is_repeated", None)
    if flag is not None:
        return flag
    return fd.label == FieldDescriptor.LABEL_REPEATED


def walk(msg, path, counts, samples, unknown):
    try:
        uf = msg.UnknownFields()
        if len(uf):
            for f in uf:
                unknown[f"{path}#field{f.field_number}"] += 1
    except Exception:
        pass
    for fd, value in msg.ListFields():
        name = f"{path}.{fd.name}"
        items = value if is_repeated(fd) else [value]
        for item in items:
            counts[name] += 1
            if fd.type == FieldDescriptor.TYPE_MESSAGE:
                walk(item, name, counts, samples, unknown)
            else:
                if fd.type == FieldDescriptor.TYPE_ENUM:
                    item = fd.enum_type.values_by_number[item].name
                if len(samples[name]) < 3:
                    samples[name].add(str(item)[:40])


for feed_name in FEEDS:
    print(f"\n===== {feed_name} =====")
    try:
        r = requests.get(BASE + feed_name + ".aspx", timeout=30)
        r.raise_for_status()
    except Exception as e:
        print("fetch failed:", e)
        continue
    feed = gtfs.FeedMessage()
    feed.ParseFromString(r.content)
    counts, samples, unknown = defaultdict(int), defaultdict(set), defaultdict(int)
    walk(feed, "feed", counts, samples, unknown)
    print(f"{len(r.content)} bytes, {len(feed.entity)} entities")
    for path in sorted(counts):
        print(f"{path:70s} {counts[path]:>7}  {sorted(samples[path])}")
    if unknown:
        print("-- fields not in the standard schema (extensions):")
        for path, n in sorted(unknown.items()):
            print(f"{path:70s} {n:>7}")

print("\n===== static GTFS zip =====")
try:
    z = zipfile.ZipFile(io.BytesIO(requests.get(STATIC_URL, timeout=120).content))
    for info in z.infolist():
        with z.open(info) as fh:
            header = fh.readline().decode("utf-8-sig").strip()
        flag = "loaded" if info.filename in LOADED else "NOT LOADED"
        print(f"{info.filename:22s} {info.file_size/1e6:8.2f} MB  {flag:10s} {header}")
except Exception as e:
    print("fetch failed:", e)
