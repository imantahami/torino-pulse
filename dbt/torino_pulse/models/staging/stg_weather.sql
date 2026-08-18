with ranked as (
    select
        observed_at,
        temperature_c,
        precipitation_mm,
        wind_speed_kmh,
        weather_code,
        fetched_at,
        row_number() over (
            partition by observed_at
            order by fetched_at desc
        ) as rn
    from {{ source('raw', 'raw_weather') }}
)

select
    observed_at,
    temperature_c,
    precipitation_mm,
    wind_speed_kmh,
    weather_code,
    fetched_at
from ranked
where rn = 1