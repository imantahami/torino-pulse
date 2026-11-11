"""Helpers shared by the GTT ingestion DAGs."""
import gzip
import hashlib
from datetime import datetime
from zoneinfo import ZoneInfo

ROME = ZoneInfo("Europe/Rome")

SNAPSHOT_DDL = """
    CREATE TABLE IF NOT EXISTS raw_feed_snapshots (
        id              SERIAL PRIMARY KEY,
        feed            TEXT      NOT NULL,
        fetched_at      TIMESTAMP NOT NULL,
        feed_timestamp  BIGINT,
        size_bytes      INT,
        sha256          TEXT,
        payload         BYTEA     NOT NULL
    )
"""
SNAPSHOT_INDEX_DDL = (
    "CREATE INDEX IF NOT EXISTS raw_feed_snapshots_feed_idx "
    "ON raw_feed_snapshots (feed, fetched_at)"
)


def store_feed_snapshot(cur, feed_name, fetched_at, content, feed_timestamp=None):
    """Keep the exact protobuf GTT sent, gzip-compressed.

    The parsed tables only hold the fields the DAGs read today. The snapshot
    lets a field added later (or a bug found later) be recovered for the past.
    It is best effort: a failure here is logged and never stops the ingestion,
    and the savepoint keeps a failed insert from aborting the caller's transaction.
    """
    try:
        cur.execute("SAVEPOINT feed_snapshot")
        cur.execute(SNAPSHOT_DDL)
        cur.execute(SNAPSHOT_INDEX_DDL)
        cur.execute(
            "INSERT INTO raw_feed_snapshots "
            "(feed, fetched_at, feed_timestamp, size_bytes, sha256, payload) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (
                feed_name, fetched_at, feed_timestamp, len(content),
                hashlib.sha256(content).hexdigest(),
                gzip.compress(content, compresslevel=6),
            ),
        )
        cur.execute("RELEASE SAVEPOINT feed_snapshot")
    except Exception as exc:
        cur.execute("ROLLBACK TO SAVEPOINT feed_snapshot")
        print(f"WARNING: could not store {feed_name} snapshot: {exc}")


def scheduled_epoch(start_date, time_text):
    """Epoch seconds of a GTFS schedule time on a service day.

    GTFS times count from 'noon minus 12 hours' of the service day and may
    exceed 24:00:00 for trips that run past midnight. start_date is YYYYMMDD.
    Returns None if either value is missing or malformed.
    """
    try:
        d = datetime.strptime(start_date, "%Y%m%d")
        hours, minutes, seconds = (int(x) for x in time_text.strip().split(":"))
    except (TypeError, ValueError, AttributeError):
        return None
    noon = datetime(d.year, d.month, d.day, 12, tzinfo=ROME).timestamp()
    return int(noon - 12 * 3600 + hours * 3600 + minutes * 60 + seconds)
