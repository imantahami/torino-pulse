-- Queries behind the "Findings" section of README.md.
--
-- Re-run these whenever the Findings section is refreshed, so the
-- numbers in the README stay reproducible. All queries read from dbt
-- marts (already filtered/deduplicated per the Data quality notes),
-- except the two rate-of-collection queries at the end, which read
-- fct_hourly_service (small, pre-aggregated) rather than the raw
-- tables (large, slow to scan without an index on fetched_at).
--
-- Run with: docker compose exec postgres psql -U airflow -d torino_pulse -f /path/to/findings.sql
-- (or paste individual blocks into psql interactively)


-- ============================================================
-- 1. Collection window
-- ============================================================
select
    min(first_observed_at)                           as collection_start,
    now()                                             as as_of,
    (now()::date - min(first_observed_at)::date)      as days_collecting
from fct_route_performance;


-- ============================================================
-- 2. Network-wide OTP, pct_late, median delay (all routes, weighted by observations)
-- ============================================================
select
    count(*)                                                              as routes,
    sum(observations)                                                     as observations,
    round(sum(pct_on_time * observations) / sum(observations), 1)         as network_otp_pct,
    round(sum(pct_late * observations) / sum(observations), 1)            as network_pct_late,
    round(sum(median_delay_seconds * observations) / sum(observations))   as network_median_delay_seconds
from fct_route_performance;


-- ============================================================
-- 3. OTP / pct_late / median delay by mode (tram vs bus)
-- ============================================================
select
    route_mode,
    count(*)                                                            as routes,
    sum(observations)                                                   as observations,
    round(sum(pct_on_time * observations) / sum(observations), 1)       as otp_pct,
    round(sum(pct_late * observations) / sum(observations), 1)          as pct_late,
    round(sum(median_delay_seconds * observations) / sum(observations)) as median_delay_seconds
from fct_route_performance
group by route_mode
order by route_mode;


-- ============================================================
-- 4. Worst routes by pct_late (observations >= 200)
-- ============================================================
select route_short_name, pct_late, p90_delay_seconds, stddev_delay_seconds, observations
from fct_route_performance
where observations >= 200
order by pct_late desc
limit 6;


-- ============================================================
-- 5. Most unpredictable routes by stddev (observations >= 200)
-- ============================================================
select route_short_name, stddev_delay_seconds, pct_late, observations
from fct_route_performance
where observations >= 200
order by stddev_delay_seconds desc
limit 6;


-- ============================================================
-- 6. Most reliable routes by OTP (observations >= 200)
-- ============================================================
select route_short_name, pct_on_time, pct_late, stddev_delay_seconds, observations
from fct_route_performance
where observations >= 200
order by pct_on_time desc
limit 6;


-- ============================================================
-- 7. Fleet cycle: average active vehicles by hour of day
--    (excludes sparse/partial hours)
-- ============================================================
select
    local_hour,
    round(avg(active_vehicles)) as avg_active_vehicles
from fct_hourly_service
where not is_sparse_hour
group by local_hour
order by local_hour;


-- ============================================================
-- 8. Alerts: breakdown by effect, and how many are active right now
-- ============================================================
select effect, count(distinct alert_id) as n
from fct_alert_impact
group by effect
order by n desc;

select count(distinct alert_id) as active_alerts_now
from fct_alert_impact
where is_currently_active;


-- ============================================================
-- 9. Weather vs delay (dry / rainy / foggy)
-- ============================================================
select
    weather_group,
    sum(hours_observed)                      as hours,
    round(avg(avg_pct_late), 1)              as avg_pct_late,
    round(avg(avg_median_delay), 1)          as avg_median_delay
from fct_weather_delay_impact
group by weather_group
order by hours desc;


-- ============================================================
-- 10. Ingestion rate by day of week (position reports/day, averaged
--     over the whole collection window) — shows this is a live,
--     continuously-updating pipeline rather than a one-off snapshot.
--     Reads fct_hourly_service (pre-aggregated, fast) instead of
--     raw_vehicle_positions (1M+ rows, slow full-table scan).
-- ============================================================
select
    to_char(hour_bucket, 'Day')                                      as day_of_week,
    extract(dow from hour_bucket)::int                                as dow,
    count(distinct date_trunc('day', hour_bucket))                    as days_sampled,
    round(sum(position_reports) / count(distinct date_trunc('day', hour_bucket))) as avg_position_reports_per_day
from fct_hourly_service
where not is_sparse_hour
group by 1, 2
order by 2;
