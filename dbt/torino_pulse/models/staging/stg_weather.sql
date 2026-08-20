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
    observed_at at time zone 'UTC' at time zone 'Europe/Rome' as observed_at_local,

    extract(hour from observed_at at time zone 'UTC' at time zone 'Europe/Rome') as local_hour,
    extract(dow  from observed_at at time zone 'UTC' at time zone 'Europe/Rome') as local_day_of_week,

    temperature_c,
    precipitation_mm,
    wind_speed_kmh,
    weather_code,

    -- WMO weather interpretation codes as used by Open-Meteo
    case
        when weather_code = 0 then 'clear'
        when weather_code between 1 and 3 then 'cloudy'
        when weather_code between 45 and 48 then 'fog'
        when weather_code between 51 and 57 then 'drizzle'
        when weather_code between 61 and 67 then 'rain'
        when weather_code between 71 and 77 then 'snow'
        when weather_code between 80 and 82 then 'rain_showers'
        when weather_code between 85 and 86 then 'snow_showers'
        when weather_code between 95 and 99 then 'thunderstorm'
        else 'other'
    end as weather_condition,

    (precipitation_mm > 0) as is_precipitating,

    fetched_at

from ranked
where rn = 1