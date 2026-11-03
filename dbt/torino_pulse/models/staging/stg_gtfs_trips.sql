-- One row per trip_id, resolved to the route it was most recently seen on.
--
-- Built from gtfs_trips_history rather than gtfs_trips. gtfs_trips is rebuilt
-- every day and only describes today's feed, while GTT rotates trip_ids when it
-- publishes a new one, so realtime observations recorded under an older feed
-- could no longer be matched to a route. The history table keeps every
-- trip_id that has ever been published (from the first run of the
-- update_gtfs_history task onward).
--
-- If a trip_id ever maps to more than one route, the most recently seen route
-- wins. The update_gtfs_history task logs a warning when that happens.

select distinct on (trip_id)
    trip_id,
    route_id,
    direction_id,
    trip_headsign,
    first_seen_at,
    last_seen_at
from {{ source('raw', 'gtfs_trips_history') }}
order by trip_id, last_seen_at desc, route_id
