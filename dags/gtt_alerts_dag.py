from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import hashlib
import requests
import psycopg2
from google.transit import gtfs_realtime_pb2


def get_translation(field, lang='it'):
    """Extract text from a TranslatedString, preferring the given language."""
    if not field.translation:
        return None
    for t in field.translation:
        if t.language == lang:
            return t.text
    return field.translation[0].text


def fetch_and_store_alerts():
    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        dbname="torino_pulse",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw_alerts (
            id SERIAL PRIMARY KEY,
            alert_id TEXT,
            content_hash TEXT,
            cause TEXT,
            effect TEXT,
            severity_level TEXT,
            header_text TEXT,
            description_text TEXT,
            url TEXT,
            active_period_start BIGINT,
            active_period_end BIGINT,
            informed_route_id TEXT,
            informed_stop_id TEXT,
            informed_trip_id TEXT,
            first_seen_at TIMESTAMP,
            last_seen_at TIMESTAMP,
            times_seen INT DEFAULT 1,
            fetched_at TIMESTAMP
        )
    """)

    # Columns added after the original table was created
    for col, coltype in [
        ("content_hash", "TEXT"),
        ("first_seen_at", "TIMESTAMP"),
        ("last_seen_at", "TIMESTAMP"),
        ("times_seen", "INT DEFAULT 1"),
    ]:
        cur.execute(f"ALTER TABLE raw_alerts ADD COLUMN IF NOT EXISTS {col} {coltype}")

    # One row per (alert, affected entity, content version).
    # COALESCE because informed_* columns are frequently NULL and
    # NULLs never compare equal in a unique index.
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS raw_alerts_dedup_idx
        ON raw_alerts (
            alert_id,
            content_hash,
            COALESCE(informed_route_id, ''),
            COALESCE(informed_stop_id, ''),
            COALESCE(informed_trip_id, '')
        )
    """)
    conn.commit()

    url = "https://percorsieorari.gtt.to.it/das_gtfsrt/alerts.aspx"
    response = requests.get(url, timeout=60)
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)

    now = datetime.now()
    inserted = 0
    updated = 0

    for entity in feed.entity:
        if not entity.HasField('alert'):
            continue

        a = entity.alert

        header = get_translation(a.header_text)
        description = get_translation(a.description_text)
        alert_url = get_translation(a.url)

        period_start = a.active_period[0].start if a.active_period else None
        period_end = a.active_period[0].end if a.active_period else None

        cause = gtfs_realtime_pb2.Alert.Cause.Name(a.cause)
        effect = gtfs_realtime_pb2.Alert.Effect.Name(a.effect)
        severity = gtfs_realtime_pb2.Alert.SeverityLevel.Name(a.severity_level)

        # Hash everything that defines the *content* of the alert.
        # If any of it changes, we treat it as a new version and keep both.
        content_hash = hashlib.sha256("|".join([
            str(cause), str(effect), str(severity),
            str(header), str(description), str(alert_url),
            str(period_start), str(period_end),
        ]).encode("utf-8")).hexdigest()

        entities = a.informed_entity if a.informed_entity else [None]

        for ie in entities:
            route_id = (ie.route_id or None) if ie else None
            stop_id = (ie.stop_id or None) if ie else None
            trip_id = (ie.trip.trip_id or None) if (ie and ie.HasField('trip')) else None

            cur.execute("""
                INSERT INTO raw_alerts (
                    alert_id, content_hash, cause, effect, severity_level,
                    header_text, description_text, url,
                    active_period_start, active_period_end,
                    informed_route_id, informed_stop_id, informed_trip_id,
                    first_seen_at, last_seen_at, times_seen, fetched_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1, %s)
                ON CONFLICT (
                    alert_id, content_hash,
                    COALESCE(informed_route_id, ''),
                    COALESCE(informed_stop_id, ''),
                    COALESCE(informed_trip_id, '')
                )
                DO UPDATE SET
                    last_seen_at = EXCLUDED.last_seen_at,
                    times_seen = raw_alerts.times_seen + 1
                RETURNING (xmax = 0) AS was_inserted
            """, (
                entity.id, content_hash, cause, effect, severity,
                header, description, alert_url,
                period_start, period_end,
                route_id, stop_id, trip_id,
                now, now, now
            ))

            was_inserted = cur.fetchone()[0]
            if was_inserted:
                inserted += 1
            else:
                updated += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"Alerts: {inserted} new rows, {updated} existing rows refreshed")


default_args = {
    'owner': 'iman',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='gtt_alerts_ingestion',
    default_args=default_args,
    start_date=datetime(2026, 8, 18),
    schedule_interval=timedelta(minutes=15),
    catchup=False,
    tags=['torino-pulse', 'gtt', 'alerts'],
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_and_store_alerts',
        python_callable=fetch_and_store_alerts,
    )