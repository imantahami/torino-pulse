"""
One-off migration: add content_hash to existing raw_alerts rows,
collapse duplicates, and create the unique index the DAG relies on.

Run once from inside the scheduler container:
    python /opt/airflow/scripts/backfill_alert_hashes.py
"""

import hashlib
import psycopg2

conn = psycopg2.connect(
    host="postgres", port=5432,
    dbname="torino_pulse", user="airflow", password="airflow"
)
cur = conn.cursor()

for col, coltype in [
    ("content_hash", "TEXT"),
    ("first_seen_at", "TIMESTAMP"),
    ("last_seen_at", "TIMESTAMP"),
    ("times_seen", "INT DEFAULT 1"),
]:
    cur.execute(f"ALTER TABLE raw_alerts ADD COLUMN IF NOT EXISTS {col} {coltype}")
conn.commit()
print("Columns ensured.")

# Compute hashes in Python so they match exactly what the DAG produces
cur.execute("""
    SELECT id, cause, effect, severity_level, header_text,
           description_text, url, active_period_start, active_period_end
    FROM raw_alerts
    WHERE content_hash IS NULL
""")
rows = cur.fetchall()
print(f"Hashing {len(rows)} rows...")

for row in rows:
    row_id, cause, effect, severity, header, description, url, start, end = row
    content_hash = hashlib.sha256("|".join([
        str(cause), str(effect), str(severity),
        str(header), str(description), str(url),
        str(start), str(end),
    ]).encode("utf-8")).hexdigest()
    cur.execute(
        "UPDATE raw_alerts SET content_hash = %s WHERE id = %s",
        (content_hash, row_id)
    )

conn.commit()
print("Hashes written.")

# Collapse duplicates: keep the earliest row per unique key,
# carrying forward the observation window and count.
cur.execute("""
    WITH grouped AS (
        SELECT
            min(id) AS keep_id,
            count(*) AS n,
            min(fetched_at) AS first_at,
            max(fetched_at) AS last_at
        FROM raw_alerts
        GROUP BY
            alert_id, content_hash,
            COALESCE(informed_route_id, ''),
            COALESCE(informed_stop_id, ''),
            COALESCE(informed_trip_id, '')
    )
    UPDATE raw_alerts a
    SET first_seen_at = g.first_at,
        last_seen_at  = g.last_at,
        times_seen    = g.n
    FROM grouped g
    WHERE a.id = g.keep_id
""")
conn.commit()
print(f"Survivor rows annotated: {cur.rowcount}")

cur.execute("""
    DELETE FROM raw_alerts
    WHERE id NOT IN (
        SELECT min(id)
        FROM raw_alerts
        GROUP BY
            alert_id, content_hash,
            COALESCE(informed_route_id, ''),
            COALESCE(informed_stop_id, ''),
            COALESCE(informed_trip_id, '')
    )
""")
deleted = cur.rowcount
conn.commit()
print(f"Deleted {deleted} duplicate rows.")

cur.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS raw_alerts_dedup_idx
    ON raw_alerts (
        alert_id, content_hash,
        COALESCE(informed_route_id, ''),
        COALESCE(informed_stop_id, ''),
        COALESCE(informed_trip_id, '')
    )
""")
conn.commit()
print("Unique index created.")

cur.execute("VACUUM FULL raw_alerts")
print("Table vacuumed.")

cur.execute("SELECT count(*) FROM raw_alerts")
print(f"Final row count: {cur.fetchone()[0]}")

cur.close()
conn.close()