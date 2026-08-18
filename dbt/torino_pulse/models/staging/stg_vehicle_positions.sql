select
    id,
    vehicle_id,
    latitude,
    longitude,
    fetched_at
from {{ source('raw', 'raw_vehicle_positions') }}
where latitude != 0 and longitude != 0