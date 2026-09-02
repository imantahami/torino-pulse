{{ config(materialized='table') }}

-- Route-level punctuality.
--
-- Three filters are applied, each with evidence behind it:
--   1. stop_sequence > 1  — the first stop always reports zero delay
--      because the vehicle has not departed yet.
--   2. is_latest_observation — each stop is predicted repeatedly as the
--      trip progresses; the last prediction is the most accurate.
--   3. arrival_delay_seconds <> 0 — one third of trips report exactly
--      zero at *every* stop, uniformly across stop positions, and 80% of
--      vehicles are consistently in one group or the other. That pattern
--      fits missing data, not on-time performance. Treated as null.

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
      and tu.arrival_delay_seconds <> 0
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

    round(100.0 * count(*) filter (where o.is_wheelchair_accessible) / count(*), 1) as pct_wheelchair_accessible,

    min(o.fetched_at_local) as first_observed_at,
    max(o.fetched_at_local) as last_observed_at

from observations o
join {{ ref('stg_routes') }} r on o.route_id = r.route_id
group by 1, 2, 3, 4, 5, 6