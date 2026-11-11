-- Data-quality checks behind the "Data quality notes" section of README.md.
-- Run with: docker compose exec -T postgres psql -U airflow -d torino_pulse -P pager=off -f /opt/airflow/analysis/data_quality.sql
-- (or paste a block into psql). The raw table has no index on fetched_at,
-- so recent windows are bounded by id instead.

-- 1. Missing delay data: NULL vs exact zero, last 1M rows.
--    Since 4 Nov 2026 14:50 UTC the trip-updates DAG stores NULL when GTT does
--    not send a delay (before that, protobuf's default 0 was stored).
--    If "zero = missing data" holds, the zero share collapses after the change.
select
    count(*)                                                                         as rows,
    round(100.0 * count(*) filter (where arrival_delay_seconds is null) / count(*), 1) as pct_null,
    round(100.0 * count(*) filter (where arrival_delay_seconds = 0)     / count(*), 1) as pct_zero,
    round(100.0 * count(*) filter (where arrival_delay_seconds <> 0)    / count(*), 1) as pct_nonzero
from raw_trip_updates
where id > (select max(id) - 1000000 from raw_trip_updates)
  and stop_sequence > 1;

-- 2. Rows per day, to spot collection gaps (positions vs delay observations).
select fetched_at::date as day, count(*) as delay_rows
from raw_trip_updates
where id > (select max(id) - 3000000 from raw_trip_updates)
group by 1
order by 1;
