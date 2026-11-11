{{ config(materialized='table') }}

-- Route-level punctuality.
--
-- Three filters are applied, each with evidence behind it:
--   1. stop_sequence > 1  — the first stop always reports zero delay
--      because the vehicle has not departed yet.
--   2. is_latest_observation — each stop is predicted repeatedly as the
--      trip progresses; the last prediction is the most accurate.
--   3. arrival_delay_seconds is not null — GTT sends each stop as either a delay
--      or an absolute arrival time; staging turns the second form into a delay
--      against the timetable. What stays null is a delay of exactly 0 recorded
--      before 4 Nov 2026 14:50 UTC (it meant "no delay field", see
--      stg_trip_updates) and stops with no usable prediction. From that moment a
--      reported 0 is a real on-time prediction and is kept.

with observations as (
    select
        tu.route_id,
        tu.trip_id,
        tu.stop_sequence,
        tu.arrival_delay_seconds,
        tu.is_wheelchair_accessible,
        tu.local_hour,
        tu.local_day_name,
        tu.local_day_of_week,
        tu.fetched_at_local
    from {{ ref('stg_trip_updates') }} tu
    where tu.is_en_route
      and tu.is_latest_observation
      and tu.arrival_delay_seconds is not null
)

select
    r.route_id,
    r.route_short_name,
    r.route_long_name,
    r.route_mode,
    r.service_area,
    r.is_night_route,

    count(*)                             as observations,
    count(distinct o.trip_id)            as trips_observed,

    round(avg(o.arrival_delay_seconds))                                          as avg_delay_seconds,
    round(percentile_cont(0.5) within group (order by o.arrival_delay_seconds))  as median_delay_seconds,
    round(percentile_cont(0.9) within group (order by o.arrival_delay_seconds))  as p90_delay_seconds,
    round(stddev(o.arrival_delay_seconds))                                       as stddev_delay_seconds,
    min(o.arrival_delay_seconds)                                                 as max_early_seconds,
    max(o.arrival_delay_seconds)                                                 as max_late_seconds,

    -- Punctuality bands. GTT publishes no official tolerance, so these
    -- use a symmetric +/- 60s window as a working definition.
    round(100.0 * count(*) filter (where o.arrival_delay_seconds >  60) / count(*), 1) as pct_late,
    round(100.0 * count(*) filter (where o.arrival_delay_seconds < -60) / count(*), 1) as pct_early,
    round(100.0 * count(*) filter (where abs(o.arrival_delay_seconds) <= 60) / count(*), 1) as pct_on_time,

    -- One-sided "not late" measures, for comparing against operators that
    -- count only delays (early arrivals are not penalised). Same
    -- observations as above; only the threshold differs. A stop-time is
    -- "not late" when it arrives less than N minutes behind schedule.
    round(100.0 * count(*) filter (where o.arrival_delay_seconds < 180) / count(*), 1) as pct_not_late_3min,
    round(100.0 * count(*) filter (where o.arrival_delay_seconds < 300) / count(*), 1) as pct_not_late_5min,

    round(100.0 * count(*) filter (where o.is_wheelchair_accessible) / count(*), 1) as pct_wheelchair_accessible,

    min(o.fetched_at_local) as first_observed_at,
    max(o.fetched_at_local) as last_observed_at

from observations o
join {{ ref('stg_routes') }} r on o.route_id = r.route_id
group by 1, 2, 3, 4, 5, 6