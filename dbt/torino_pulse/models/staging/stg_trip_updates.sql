with base as (
    select
        tu.trip_id,

        -- The realtime feed leaves route_id empty on trip updates,
        -- so we resolve it from the static GTFS trips table.
        t.route_id,
        t.direction_id,
        t.trip_headsign,

        tu.trip_start_time,
        tu.trip_start_date,
        tu.vehicle_id,
        tu.vehicle_label,
        tu.license_plate,

        -- GTFS-RT WheelchairAccessible enum, stored as its raw integer:
        --   0 = NO_VALUE, 1 = UNKNOWN,
        --   2 = WHEELCHAIR_ACCESSIBLE, 3 = WHEELCHAIR_INACCESSIBLE
        case tu.wheelchair_accessible
            when '2' then true
            when '3' then false
            else null
        end as is_wheelchair_accessible,

        tu.stop_sequence,
        tu.arrival_delay_seconds,
        tu.departure_delay_seconds,
        to_timestamp(tu.gps_timestamp) at time zone 'UTC' as gps_reported_at,
        tu.fetched_at

    from {{ source('raw', 'raw_trip_updates') }} tu
    left join {{ source('raw', 'gtfs_trips') }} t
        on tu.trip_id = t.trip_id
    where tu.trip_id is not null
      and tu.stop_sequence is not null
),

flagged as (
    select
        *,

        row_number() over (
            partition by trip_id, stop_sequence
            order by fetched_at
        ) as observation_number,

        row_number() over (
            partition by trip_id, stop_sequence
            order by fetched_at desc
        ) as recency_rank,

        count(*) over (
            partition by trip_id, stop_sequence
        ) as total_observations,

        lag(arrival_delay_seconds) over (
            partition by trip_id, stop_sequence
            order by fetched_at
        ) as previous_delay_seconds,

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
    departure_delay_seconds,
    previous_delay_seconds,
    first_delay_seconds,
    arrival_delay_seconds - previous_delay_seconds as delay_change_seconds,
    arrival_delay_seconds - first_delay_seconds    as delay_drift_seconds,

    -- The first stop of a trip always reports zero delay because the
    -- vehicle has not departed yet. Excluding it avoids diluting averages.
    (stop_sequence > 1) as is_en_route,

    observation_number,
    total_observations,
    recency_rank,
    recency_rank = 1 as