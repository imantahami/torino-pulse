-- When do alerts happen, and when do demonstrations cluster?
-- Light: reads stg_alerts (small). Run with:
--   docker compose exec -T postgres psql -U airflow -d torino_pulse -P pager=off < analysis/alerts_patterns.sql
--
-- Notes
--  * One row per alert_id (stg_alerts has one row per affected entity).
--  * "first seen" is when the collector first saw the alert (15-min polling),
--    not when GTT published it. "planned start" is active_from, the time GTT
--    says the event begins; it is better for demonstrations.
--  * Alerts already active when collection began (19 Aug 2026) all get
--    first_seen = 19 Aug, so first-seen queries start on 20 Aug.
--  * Alerts shorter than the polling interval can be missed entirely.

create temp view alert_level as
select
    alert_id,
    min(effect)              as effect,
    min(cause)               as cause,
    min(header_text)         as header_text,
    min(first_seen_at_local) as first_seen_local,
    min(active_from_local)   as planned_start_local,
    min(active_until_local)  as planned_end_local
from stg_alerts
group by alert_id;

-- 1. New alerts by local hour of first sighting, split by effect.
select
    extract(hour from first_seen_local)::int                          as local_hour,
    count(*) filter (where effect = 'REDUCED_SERVICE')                as reduced_service,
    count(*) filter (where effect = 'DETOUR')                         as detour,
    count(*) filter (where effect not in ('REDUCED_SERVICE','DETOUR')) as other,
    count(*)                                                          as total
from alert_level
where first_seen_local >= '2026-08-20'
group by 1
order by 1;

-- 2. Average new alerts per day, by weekday of first sighting.
--    Divided by how many of that weekday fall in the window, so weekdays are comparable.
with days as (
    select extract(dow from d)::int as dow, count(*) as n_days
    from generate_series('2026-08-20'::date, (now() at time zone 'Europe/Rome')::date - 1, '1 day') d
    group by 1
)
select
    to_char(first_seen_local, 'Dy')                                         as weekday,
    extract(dow from first_seen_local)::int                                 as dow,
    count(*)                                                                as alerts,
    round(count(*)::numeric / max(days.n_days), 1)                          as alerts_per_day,
    round(count(*) filter (where effect = 'DETOUR')::numeric / max(days.n_days), 1)          as detours_per_day,
    round(count(*) filter (where effect = 'REDUCED_SERVICE')::numeric / max(days.n_days), 1) as reduced_service_per_day
from alert_level a
join days on days.dow = extract(dow from a.first_seen_local)::int
where first_seen_local >= '2026-08-20'
  and first_seen_local < (now() at time zone 'Europe/Rome')::date
group by 1, 2
order by 2;

-- 3. Demonstrations by weekday of the planned start.
select
    to_char(planned_start_local, 'Dy')            as weekday,
    extract(dow from planned_start_local)::int    as dow,
    count(*)                                      as demonstration_alerts,
    count(distinct planned_start_local::date)     as distinct_days
from alert_level
where cause = 'DEMONSTRATION'
group by 1, 2
order by 2;

-- 4. Demonstrations by local hour of the planned start.
select
    extract(hour from planned_start_local)::int as start_hour,
    count(*)                                    as demonstration_alerts
from alert_level
where cause = 'DEMONSTRATION'
group by 1
order by 1;

-- 5. The 15 days with the most demonstration alerts (by planned start date).
select
    planned_start_local::date                  as day,
    to_char(planned_start_local, 'Dy')         as weekday,
    count(*)                                   as demonstration_alerts
from alert_level
where cause = 'DEMONSTRATION'
group by 1, 2
order by demonstration_alerts desc, day
limit 15;

-- 6. Demonstration alerts per month (planned start).
select date_trunc('month', planned_start_local)::date as month, count(*) as demonstration_alerts
from alert_level
where cause = 'DEMONSTRATION'
group by 1
order by 1;

-- 7. Most repeated demonstration headlines.
select header_text, count(*) as alerts
from alert_level
where cause = 'DEMONSTRATION'
group by 1
order by alerts desc
limit 10;
