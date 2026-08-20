select
    id,
    vehicle_id,
    vehicle_label,
    route_id,
    trip_id,
    trip_start_time,
    trip_start_date,
    latitude,
    longitude,
    bearing,

    to_timestamp(gps_timestamp) at time zone 'UTC' as gps_reported_at,

    fetched_at,
    -- Local wall-clock time in Turin. Handles DST automatically,
    -- unlike a fixed +2h offset which is only correct in summer.
    fetched_at at time zone 'UTC' at time zone 'Europe/Rome' as fetched_at_local,

    extract(hour from fetched_at at time zone 'UTC' at time zone 'Europe/Rome') as local_hour,
    extract(dow  from fetched_at at time zone 'UTC' at time zone 'Europe/Rome') as local_day_of_week,
    to_char(fetched_at at time zone 'UTC' at time zone 'Europe/Rome', 'Day') as local_day_name,

    -- The feed omits trip_id for some vehicles. This peaks at end of
    -- service, but the underlying reason is not confirmed.
    (trip_id is not null and trip_id <> '') as has_assigned_trip

from {{ source('raw', 'raw_vehicle_positions') }}
where latitude <> 0
  and longitude <> 0