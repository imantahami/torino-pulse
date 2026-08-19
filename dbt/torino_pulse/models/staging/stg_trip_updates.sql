with base as (
    select
        trip_id,
        route_id,
        trip_start_time,
        trip_start_date,
        vehicle_id,
        vehicle_label,
        license_plate,
        wheelchair_accessible = 'WHEELCHAIR_ACCESSIBLE' as is_wheelchair_accessible,
        stop_sequence,
        arrival_delay_seconds,
        departure_delay_seconds,
        to_timestamp(gps_timestamp) at time zone 'UTC' as gps_reported_at,
        fetched_at
    from {{ source('raw', 'raw_trip_updates') }}
    where trip_id is not null
      and stop_sequence is not null
),

flagged as (
    select
        *,

        -- how many times this trip/stop has been observed so far
        row_number() over (
            partition by trip_id, stop_sequence
            order by fetched_at
        ) as observation_number,

        -- 1 = most recent observation for this trip/stop
        row_number() over (
            partition by trip_id, stop_sequence
            order by fetched_at desc
        ) as recency_rank,

        -- total observations for this trip/stop
        count(*) over (
            partition by trip_id, stop_sequence
        ) as total_observations,

        -- previous prediction, to measure how it drifted
        lag(arrival_delay_seconds) over (
            partition by trip_id, stop_sequence
            order by fetched_at
        ) as previous_delay_seconds,

        -- first prediction ever made for this trip/stop
        first_value(arrival_delay_seconds) over (
            partition by trip_id, stop_sequence
            order by fetched_at
            rows between unbounded preceding and unbounded following
        ) as first_delay_seconds

    from base
)

select
    trip_id,
    route_id,
    trip_start_time,
    trip_start_date,
    vehicle_id,
    vehicle_label,
    license_plate,
    is_wheelchair_accessible,
    stop_sequence,

    arrival_delay_seconds,
    departure_delay_seconds,
    previous_delay_seconds,
    first_delay_seconds,

    -- how much the prediction moved since the previous fetch
    arrival_delay_seconds - previous_delay_seconds as delay_change_seconds,

    -- how much it moved since the very first prediction
    arrival_delay_seconds - first_delay_seconds as delay_drift_seconds,

    observation_number,
    total_observations,
    recency_rank,
    recency_rank = 1 as is_latest_observation,

    gps_reported_at,
    fetched_at,

    -- convenience columns for time-based analysis (Turin is UTC+2 in summer)
    date_trunc('hour', fetched_at) as fetched_hour_utc,
    extract(hour from fetched_at + interval '2 hours') as local_hour,
    extract(dow from fetched_at + interval '2 hours') as local_day_of_week

from flagged