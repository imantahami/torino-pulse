-- One-off migration for the "store everything" change (12 Nov 2026).
-- Run it BEFORE deploying the new DAG code, with dbt_transform paused, so the
-- ALTERs do not queue behind a long dbt query (lock convoy). Each statement only
-- changes metadata and takes a moment once it has the lock.
--
--   docker compose exec -T airflow-webserver airflow dags pause dbt_transform
--   docker compose exec -T postgres psql -U airflow -d torino_pulse -v ON_ERROR_STOP=1 < scripts/migrate_2026_11_12.sql
--
-- Safe to re-run. Fails after 10 s instead of waiting if something holds a long lock.
SET lock_timeout = '10s';

ALTER TABLE raw_vehicle_positions
    ADD COLUMN IF NOT EXISTS entity_id TEXT,
    ADD COLUMN IF NOT EXISTS trip_schedule_relationship TEXT,
    ADD COLUMN IF NOT EXISTS feed_timestamp BIGINT;

ALTER TABLE raw_trip_updates
    ADD COLUMN IF NOT EXISTS entity_id TEXT,
    ADD COLUMN IF NOT EXISTS scheduled_arrival_time BIGINT;

ALTER TABLE raw_alerts
    ADD COLUMN IF NOT EXISTS informed_agency_id TEXT,
    ADD COLUMN IF NOT EXISTS feed_timestamp BIGINT;

-- New dedup index that includes the agency, then drop the old one.
CREATE UNIQUE INDEX IF NOT EXISTS raw_alerts_dedup_idx2
ON raw_alerts (
    alert_id, content_hash,
    COALESCE(informed_route_id, ''),
    COALESCE(informed_stop_id, ''),
    COALESCE(informed_trip_id, ''),
    COALESCE(informed_agency_id, '')
);
DROP INDEX IF EXISTS raw_alerts_dedup_idx;

-- Feed snapshots table (also created by the DAGs if missing).
CREATE TABLE IF NOT EXISTS raw_feed_snapshots (
    id              SERIAL PRIMARY KEY,
    feed            TEXT      NOT NULL,
    fetched_at      TIMESTAMP NOT NULL,
    feed_timestamp  BIGINT,
    size_bytes      INT,
    sha256          TEXT,
    payload         BYTEA     NOT NULL
);
CREATE INDEX IF NOT EXISTS raw_feed_snapshots_feed_idx ON raw_feed_snapshots (feed, fetched_at);

-- Newer columns on the GTFS history tables (created by the static DAG on first run).
DO $$
BEGIN
    IF to_regclass('gtfs_trips_history') IS NOT NULL THEN
        ALTER TABLE gtfs_trips_history
            ADD COLUMN IF NOT EXISTS service_id TEXT,
            ADD COLUMN IF NOT EXISTS trip_short_name TEXT,
            ADD COLUMN IF NOT EXISTS block_id TEXT,
            ADD COLUMN IF NOT EXISTS shape_id TEXT,
            ADD COLUMN IF NOT EXISTS wheelchair_accessible TEXT,
            ADD COLUMN IF NOT EXISTS bikes_allowed TEXT,
            ADD COLUMN IF NOT EXISTS limited_route TEXT;
    END IF;
    IF to_regclass('gtfs_routes_history') IS NOT NULL THEN
        ALTER TABLE gtfs_routes_history ADD COLUMN IF NOT EXISTS route_sort_order TEXT;
    END IF;
END $$;
