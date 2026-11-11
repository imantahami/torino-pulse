{{ config(enabled = (target.name == 'ci')) }}

-- GTT sometimes sends an absolute predicted arrival instead of a delay. Trip T4A on
-- 5 Nov (after the collector change) is built that way in the fixtures: the latest
-- prediction implies delays of 25, 75 and 140 s at stops 2 to 4. Staging must turn
-- the arrival times into those delays, tagged 'derived_from_time'.
--
-- Returns one row for every stop where that is not the case.

with expected(stop_sequence, expected_delay) as (
    values (2, 25), (3, 75), (4, 140)
)
select
    e.stop_sequence,
    e.expected_delay,
    t.arrival_delay_seconds,
    t.delay_source
from expected e
left join {{ ref('stg_trip_updates') }} t
    on  t.trip_id = 'T4A'
    and t.trip_start_date = '20261105'
    and t.stop_sequence = e.stop_sequence
    and t.is_latest_observation
where t.arrival_delay_seconds is distinct from e.expected_delay
   or t.delay_source is distinct from 'derived_from_time'
