"""Generate the small fixture datasets that CI loads as dbt seeds.

The seeds stand in for the raw tables the DAGs write in production. They are
tiny and deterministic so a CI run takes seconds, but they are shaped to
exercise the parts of the models that have gone wrong before: trip_id reuse
across service days, trips that no GTFS feed ever contained, (0,0) GPS
readings, exact-zero delays, and alerts naming many stops.

Run from the repository root:  python scripts/generate_ci_seeds.py
"""
import csv
import os
from datetime import datetime, timedelta

OUT = os.path.join("dbt", "torino_pulse", "seeds")
os.makedirs(OUT, exist_ok=True)


def write(name, header, rows):
    with open(os.path.join(OUT, f"{name}.csv"), "w", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(header)
        w.writerows(rows)


def ts(d):
    return d.strftime("%Y-%m-%d %H:%M:%S")


def epoch(d):
    return int((d - datetime(1970, 1, 1)).total_seconds())


DAYS = [datetime(2026, 11, 2), datetime(2026, 11, 3)]
FEED_SEEN = datetime(2026, 11, 3, 3, 0)

# ---- static GTFS history --------------------------------------------------
routes = [
    # route_id, short name, long name, route_type
    ("4U", "4", "Falchera - Mirafiori", "0"),   # tram
    ("10U", "10", "Vallette - Lingotto", "0"),  # tram
    ("55U", "55", "Cimitero - Stazione", "3"),  # bus
    ("17U", "17", "Corso Sicilia", "3"),        # bus
    ("N1U", "N1", "Night line", "3"),           # night bus
    ("M1U", "M1", "Metro Fermi - Lingotto", "1"),
    ("12E", "12E", "Extra-urban", "3"),
]
write(
    "gtfs_routes_history",
    ["route_id", "agency_id", "route_short_name", "route_long_name", "route_desc",
     "route_type", "route_url", "route_color", "route_text_color", "first_seen_at", "last_seen_at"],
    [(rid, "GTT", sn, ln, "", rt, "", "", "", ts(datetime(2026, 11, 3, 3, 0)), ts(FEED_SEEN))
     for rid, sn, ln, rt in routes],
)

# trip_id -> route_id. T_ROT is on two routes to exercise the "latest wins" rule.
trips = [("T4A", "4U"), ("T4B", "4U"), ("T10A", "10U"), ("T55A", "55U"), ("T55B", "55U"),
         ("T17A", "17U"), ("TM1A", "M1U"), ("TN1A", "N1U"), ("T12E", "12E")]
rows = [(t, r, "0", "Headsign", ts(datetime(2026, 11, 3, 3, 0)), ts(FEED_SEEN)) for t, r in trips]
rows.append(("T_ROT", "55U", "0", "Headsign", ts(datetime(2026, 11, 2, 3, 0)), ts(datetime(2026, 11, 2, 3, 0))))
rows.append(("T_ROT", "17U", "0", "Headsign", ts(datetime(2026, 11, 3, 3, 0)), ts(FEED_SEEN)))
write("gtfs_trips_history", ["trip_id", "route_id", "direction_id", "trip_headsign", "first_seen_at", "last_seen_at"], rows)

# ---- static stops ---------------------------------------------------------
stops = [(f"S{i}", f"{i}", f"Stop {i}", "", f"45.0{i}", f"7.6{i}", "", "", "0", "", "", "1" if i % 2 else "2", ts(FEED_SEEN))
         for i in range(1, 6)]
stops.append(("S_BAD", "9", "No coordinates", "", "", "", "", "", "0", "", "", "0", ts(FEED_SEEN)))  # filtered out
write("gtfs_stops",
      ["stop_id", "stop_code", "stop_name", "stop_desc", "stop_lat", "stop_lon", "zone_id", "stop_url",
       "location_type", "parent_station", "stop_timezone", "wheelchair_boarding", "loaded_at"], stops)

# ---- weather: hourly, with dry, cloudy, fog and rain hours ----------------
codes = {6: 0, 7: 0, 8: 3, 9: 3, 10: 45, 11: 61, 12: 61, 13: 3, 14: 0, 15: 0, 16: 3, 17: 0, 18: 0}
rows = []
for i, (h, code) in enumerate(codes.items(), start=1):
    obs = datetime(2026, 11, 3, h, 0)
    rows.append((i, ts(obs), 8 + h / 4, 1.2 if code == 61 else 0.0, 10 + h % 5, code, ts(obs + timedelta(minutes=5))))
write("raw_weather", ["id", "observed_at", "temperature_c", "precipitation_mm", "wind_speed_kmh", "weather_code", "fetched_at"], rows)

# ---- vehicle positions ----------------------------------------------------
rows, pid = [], 1
vehicles = [("V1", "4U", "T4A"), ("V2", "55U", "T55A"), ("V3", "17U", "T17A"), ("V4", "M1U", "")]
for h in range(6, 19):
    for k in range(3):
        for vid, rid, trip in vehicles:
            t = datetime(2026, 11, 3, h, 10 * k)
            lat, lon = (45.07 + k / 100, 7.68 + k / 100)
            rows.append((pid, vid, vid, rid, trip, "08:00:00", "20261103", lat, lon, 90.0, epoch(t), ts(t)))
            pid += 1
rows.append((pid, "V_BAD", "V_BAD", "4U", "T4A", "08:00:00", "20261103", 0.0, 0.0, 0.0, epoch(datetime(2026, 11, 3, 9, 0)),
             ts(datetime(2026, 11, 3, 9, 0))))  # (0,0) reading, dropped in staging
write("raw_vehicle_positions",
      ["id", "vehicle_id", "vehicle_label", "route_id", "trip_id", "trip_start_time", "trip_start_date",
       "latitude", "longitude", "bearing", "gps_timestamp", "fetched_at"], rows)

# ---- trip updates ---------------------------------------------------------
# The same trip_ids run on both service days (this is how GTT behaves). Each
# stop is predicted twice, the later prediction being the one that should win.
rows, uid = [], 1
spec = {  # trip -> (first prediction delays per stop 1..4, last prediction delays per stop 1..4)
    "T4A": ([0, 10, 40, 90], [0, 20, 70, 130]),
    "T4B": ([0, -30, -50, -70], [0, -40, -80, -100]),
    "T10A": ([0, 0, 0, 0], [0, 0, 0, 0]),        # reports exact zero everywhere: excluded from delay stats
    "T55A": ([0, 50, 200, 350], [0, 80, 260, 420]),
    "T55B": ([0, 15, -20, 30], [0, 25, -10, 45]),
    "T17A": ([0, 120, 300, 500], [0, 150, 330, 560]),
    "TM1A": ([0, -10, -20, -30], [0, -15, -25, -40]),
    "TN1A": ([0, 5, 10, 15], [0, 6, 12, 18]),
    "T12E": ([0, 200, 400, 600], [0, 220, 430, 640]),
}
for di, day in enumerate(DAYS):
    for ti, (trip, (first, last)) in enumerate(spec.items()):
        base = day + timedelta(hours=8 + ti)
        for stop in range(1, 5):
            for pass_no, delays in enumerate((first, last)):
                t = base + timedelta(minutes=30 * pass_no)
                d = delays[stop - 1] + 5 * di  # a small day-to-day difference
                if stop == 1:
                    d = 0
                rows.append((uid, trip, "", "08:00:00", day.strftime("%Y%m%d"), "V1", "V1", "AA000AA", "2",
                             stop, d, d, epoch(t), ts(t)))
                uid += 1
# A trip no feed ever contained, seen before the history table existed (route stays null).
for stop in range(1, 4):
    t = datetime(2026, 11, 2, 12, 0)
    rows.append((uid, "T_GHOST", "", "08:00:00", "20261102", "V9", "V9", "ZZ999ZZ", "3", stop, 60 * stop, 60 * stop,
                 epoch(t), ts(t)))
    uid += 1
write("raw_trip_updates",
      ["id", "trip_id", "route_id", "trip_start_time", "trip_start_date", "vehicle_id", "vehicle_label",
       "license_plate", "wheelchair_accessible", "stop_sequence", "arrival_delay_seconds",
       "departure_delay_seconds", "gps_timestamp", "fetched_at"], rows)

# ---- alerts ---------------------------------------------------------------
far = epoch(datetime(2099, 1, 1))
past = epoch(datetime(2026, 11, 1))
alerts = [
    # alert_id, hash, cause, effect, severity, header, route, stop
    ("A1", "h1", "CONSTRUCTION", "DETOUR", "WARNING", "Detour on 4", "4U", "", far),
    ("A1", "h1", "CONSTRUCTION", "DETOUR", "WARNING", "Detour on 4", "", "S1", far),
    ("A1", "h1", "CONSTRUCTION", "DETOUR", "WARNING", "Detour on 4", "", "S2", far),
    ("A2", "h2", "TECHNICAL_PROBLEM", "NO_SERVICE", "SEVERE", "No service M1", "M1U", "", far),
    ("A3", "h3", "OTHER_CAUSE", "OTHER_EFFECT", "INFO", "Notice", "55U", "", far),
    ("A4", "h4", "OTHER_CAUSE", "DETOUR", "WARNING", "Old detour", "17U", "", past),  # already ended
]
rows = []
for i, (aid, h, cause, eff, sev, head, rid, sid, end) in enumerate(alerts, start=1):
    seen = datetime(2026, 11, 3, 10, 0)
    rows.append((i, aid, h, cause, eff, sev, head, "", "", epoch(datetime(2026, 11, 1)), end, rid, sid, "",
                 ts(seen), ts(seen + timedelta(hours=2)), 3, ts(seen)))
write("raw_alerts",
      ["id", "alert_id", "content_hash", "cause", "effect", "severity_level", "header_text", "description_text",
       "url", "active_period_start", "active_period_end", "informed_route_id", "informed_stop_id",
       "informed_trip_id", "first_seen_at", "last_seen_at", "times_seen", "fetched_at"], rows)
print("seeds written to", OUT)
