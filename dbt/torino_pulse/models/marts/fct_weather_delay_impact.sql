{{ config(materialized='table') }}

-- Impact of weather conditions on transit delay.
-- Grain: one row per (weather_condition, local_hour).
-- Source: fct_hourly_service already joins delay and weather per hour.

select
    weather_condition,
    is_precipitating,
    had_precipitation,
    local_hour,
    local_day_name,

    count(*)                                          as hours_observed,
    round(avg(avg_temperature_c)::numeric, 1)         as avg_temp_c,
    round(avg(total_precipitation_mm)::numeric, 2)    as avg_precip_mm,
    round(avg(avg_wind_speed_kmh)::numeric, 1)        as avg_wind_kmh,

    round(avg(median_delay_seconds)::numeric)         as avg_median_delay,
    round(avg(avg_delay_seconds)::numeric)            as avg_delay,
    round(avg(pct_late)::numeric, 1)                  as avg_pct_late,
    round(avg(active_vehicles)::numeric)              as avg_fleet_size,

    -- Summary by rain vs no rain (simpler grouping)
    case
        when weather_condition in ('rain', 'rain_showers', 'thunderstorm') then 'rainy'
        when weather_condition = 'fog' then 'foggy'
        when weather_condition in ('clear', 'cloudy') then 'dry'
        else 'other'
    end as weather_group

from {{ ref('fct_hourly_service') }}
where median_delay_seconds is not null
  and is_sparse_hour = false
group by 1, 2, 3, 4, 5