{{ config(materialized='view') }}

-- Network-wide service level by local hour, with weather joined on.
--
-- One row per (date, hour). Fleet size and delay come from different
-- feeds at different frequencies, so they are aggregated separately and
-- joined on the hour bucket rather than row by row.

with fleet as (
    select
        date_trunc('hour', fetched_at_local)  as hour_bucket,
        count(distinct vehicle_id)            as active_vehicles,
        count(distinct route_id)              as active_routes,
        count(*)                              as position_reports,
        count(distinct vehicle_id) filter (where has_assigned_trip) as vehicles_on_a_trip
    from {{ ref('stg_vehicle_positions') }}
    group by 1
),

delays as (
    select
        date_trunc('hour', fetched_at_local) as hour_bucket,
        count(*)                             as delay_observations,
        count(distinct trip_id)              as trips_observed,
        round(avg(arrival_delay_seconds))                                         as avg_delay_seconds,
        round(percentile_cont(0.5) within group (order by arrival_delay_seconds)) as median_delay_seconds,
        round(100.0 * count(*) filter (where arrival_delay_seconds > 60) / nullif(count(*), 0), 1) as pct_late
    from {{ ref('stg_trip_updates') }}
    where is_en_route
      and is_latest_observation
      and arrival_delay_seconds <> 0
    group by 1
),

weather as (
    select
        date_trunc('hour', observed_at_local) as hour_bucket,
        round(avg(temperature_c)::numeric, 1)    as avg_temperature_c,
        round(sum(precipitation_mm)::numeric, 1) as total_precipitation_mm,
        round(avg(wind_speed_kmh)::numeric, 1)   as avg_wind_speed_kmh,
        bool_or(is_precipitating)                as had_precipitation,
        mode() within group (order by weather_condition) as dominant_condition
    from {{ ref('stg_weather') }}
    group by 1
)

select
    f.hour_bucket,
    extract(hour from f.hour_bucket)          as local_hour,
    to_char(f.hour_bucket, 'Day')             as local_day_name,
    extract(dow from f.hour_bucket)           as local_day_of_week,
    extract(dow from f.hour_bucket) in (0, 6) as is_weekend,

    f.active_vehicles,
    f.active_routes,
    f.vehicles_on_a_trip,
    f.position_reports,

    d.delay_observations,
    d.trips_observed,
    d.avg_delay_seconds,
    d.median_delay_seconds,
    d.pct_late,

    w.avg_temperature_c,
    w.total_precipitation_mm,
    w.avg_wind_speed_kmh,
    w.had_precipitation,
    w.dominant_condition,

    -- Collection was not continuous. Hours with unusually few position
    -- reports are likely partial and should be treated with caution.
    (f.position_reports < 1500) as is_sparse_hour

from fleet f
left join delays  d on f.hour_bucket = d.hour_bucket
left join weather w on f.hour_bucket = w.hour_bucket
order by f.hour_bucket