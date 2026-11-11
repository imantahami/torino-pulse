with base as (
    select
        tu.trip_id,
        t.route_id,
        t.direction_id,
        t.trip_headsign,
        tu.trip_start_time,
        tu.trip_start_date,
        tu.vehicle_id,
        tu.vehicle_label,
        tu.license_plate,
        case tu.wheelchair_accessible
            when '2' then true
            when '3' then false
            else null
        end as is_wheelchair_accessible,
        tu.stop_sequence,

        -- GTT sends each stop update with EITHER arrival.delay OR arrival.time (an
        -- absolute predicted arrival), never both, and a whole trip uses one form.
        -- The delay of a "time" stop is the predicted arrival minus the scheduled one
        -- (scheduled_arrival_time is looked up at ingestion).
        -- A reported delay of exactly 0 before the collector change is unknown: it was
        -- the protobuf default for a missing field, and the time form was not stored.
        case
            when tu.arrival_delay_seconds is not null
                 and not (tu.arrival_delay_seconds = 0
                          and tu.fetched_at < '{{ var("delay_fix_cutover") }}'::timestamp)
                then tu.arrival_delay_seconds
            when tu.arrival_time is not null and tu.scheduled_arrival_time is not null
                then (tu.arrival_time - tu.scheduled_arrival_time)::int
        end as arrival_delay_seconds,
        case
            when tu.arrival_delay_seconds is not null
                 and not (tu.arrival_delay_seconds = 0
                          and tu.fetched_at < '{{ var("delay_fix_cutover") }}'::timestamp)
                then 'reported'
            when tu.arrival_time is not null and tu.scheduled_arrival_time is not null
                then 'derived_from_time'
            else 'unknown'
        end as delay_source,
        tu.arrival_time,
        tu.scheduled_arrival_time,

        tu.departure_delay_seconds,
        to_timestamp(tu.gps_timestamp) at time zone 'UTC' as gps_reported_at,
        tu.fetched_at
    from {{ source('raw', 'raw_trip_updates') }} tu
    left join {{ ref('stg_gtfs_trips') }} t
        on tu.trip_id = t.trip_id
    where tu.trip_id is not null
      and tu.stop_sequence is not null
),

-- GTT reuses the same trip_id on many service days, so every window is
-- partitioned by trip_start_date as well. Without it, a trip's observations
-- from earlier days were treated as superseded and dropped from
-- is_latest_observation, so most days' delays were missing from the marts.
flagged as (
    select
        *,
        row_number() over (
            partition by trip_id, trip_start_date, stop_sequence
            order by fetched_at
        ) as observation_number,
        row_number() over (
            partition by trip_id, trip_start_date, stop_sequence
            order by fetched_at desc
        ) as recency_rank,
        count(*) over (
            partition by trip_id, trip_start_date, stop_sequence
        ) as total_observations,
        lag(arrival_delay_seconds) over (
            partition by trip_id, trip_start_date, stop_sequence
            order by fetched_at
        ) as previous_delay_seconds,
        first_value(arrival_delay_seconds) over (
            partition by trip_id, trip_start_date, stop_sequence
            order by fetched_at
            rows between unbounded preceding and unbounded following
        ) as first_delay_seconds
    from base
)

select
    trip_id,
    route_id,
    direction_id,
    trip_headsign,
    trip_start_time,
    trip_start_date,
    vehicle_id,
    vehicle_label,
    license_plate,
    is_wheelchair_accessible,
    stop_sequence,
    arrival_delay_seconds,
    delay_source,
    arrival_time,
    scheduled_arrival_time,
    departure_delay_seconds,
    previous_delay_seconds,
    first_delay_seconds,
    arrival_delay_seconds - previous_delay_seconds as delay_change_seconds,
    arrival_delay_seconds - first_delay_seconds    as delay_drift_seconds,
    (stop_sequence > 1) as is_en_route,
    observation_number,
    total_observations,
    recency_rank,
    (recency_rank = 1) as is_latest_observation,
    gps_reported_at,
    fetched_at,
    fetched_at at time zone 'UTC' at time zone 'Europe/Rome' as fetched_at_local,
    extract(hour from fetched_at at time zone 'UTC' at time zone 'Europe/Rome') as local_hour,
    extract(dow  from fetched_at at time zone 'UTC' at time zone 'Europe/Rome') as local_day_of_week,
    to_char(fetched_at at time zone 'UTC' at time zone 'Europe/Rome', 'Day') as local_day_name
from flagged