{{ config(enabled = (target.name == 'ci')) }}

-- Regression test for the trip_id reuse bug.
--
-- GTT reuses a trip_id on every service day. is_latest_observation must pick
-- the last prediction per (trip_id, trip_start_date, stop_sequence), so a
-- trip seen on N service days has N latest rows per stop. When the window was
-- partitioned by trip_id alone it kept one, and earlier days' delays vanished.
--
-- Returns one row for every (trip, stop) with fewer latest rows than service days.

select
    trip_id,
    stop_sequence,
    count(distinct trip_start_date)                          as service_days,
    count(*) filter (where is_latest_observation)            as latest_rows
from {{ ref('stg_trip_updates') }}
group by trip_id, stop_sequence
having count(*) filter (where is_latest_observation) <> count(distinct trip_start_date)
