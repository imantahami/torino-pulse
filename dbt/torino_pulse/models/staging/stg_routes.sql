select
    route_id,
    agency_id,
    route_short_name,
    route_long_name,
    route_desc,

    -- GTFS route_type: 0=tram, 1=metro, 2=rail, 3=bus, ...
    route_type,
    case route_type
        when '0' then 'tram'
        when '1' then 'metro'
        when '2' then 'rail'
        when '3' then 'bus'
        when '4' then 'ferry'
        when '5' then 'cable_tram'
        when '6' then 'aerial_lift'
        when '7' then 'funicular'
        else 'other'
    end as route_mode,

    route_url,
    nullif(route_color, '')      as route_color_hex,
    nullif(route_text_color, '') as route_text_color_hex,

    -- 'U' suffix marks urban Turin routes; 'E' marks extra-urban
    case
        when route_id like '%U' then 'urban'
        when route_id like '%E' then 'extra_urban'
        else 'other'
    end as service_area,

    -- night routes carry an N in the short name
    route_short_name like '%N%' as is_night_route,

    loaded_at

from {{ source('raw', 'gtfs_routes') }}