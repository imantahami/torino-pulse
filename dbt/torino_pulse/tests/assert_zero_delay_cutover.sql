{{ config(enabled = (target.name == 'ci')) }}

-- An exact-zero delay means different things on either side of the collector change
-- (var delay_fix_cutover, 4 Nov 2026 14:50 UTC):
--   before: the protobuf default for a missing field, so the delay is unknown;
--   after:  GTT really predicted "on time", so the zero is kept.
-- T10A on 2 Nov is the "before" case (on 3 Nov the fixture adds a day-to-day offset, so
-- it is not all zeros), T55A on 5 Nov the "after" case.
--
-- Returns one row for every stop handled the wrong way.

select 'zero before the cutover should be unknown' as problem, trip_id, stop_sequence
from {{ ref('stg_trip_updates') }}
where trip_id = 'T10A'
  and trip_start_date = '20261102'
  and stop_sequence > 1
  and delay_source <> 'unknown'

union all

select 'zero after the cutover should be kept as reported', trip_id, stop_sequence
from {{ ref('stg_trip_updates') }}
where trip_id = 'T55A'
  and trip_start_date = '20261105'
  and stop_sequence > 1
  and is_latest_observation
  and not (delay_source = 'reported' and arrival_delay_seconds = 0)
