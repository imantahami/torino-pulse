select
    stop_id,
    stop_code,
    stop_name,
    stop_desc,
    cast(nullif(stop_lat, '') as double precision) as latitude,
    cast(nullif(stop_lon, '') as double precision) as longitude,
    zone_id,
    stop_url,
    nullif(location_type, '')  as location_type,
    nullif(parent_station, '') as parent_station,
    stop_timezone,

    -- GTFS wheelchair_boarding: 0/empty = unknown, 1 = accessible, 2 = not accessible
    wheelchair_boarding,
    case wheelchair_boarding
        when '1' then true
        when '2' then false
        else null
    end as is_wheelchair_accessible,

    loaded_at

from {{ source('raw', 'gtfs_stops') }}
where nullif(stop_lat, '') is not null
  and nullif(stop_lon, '') is not null