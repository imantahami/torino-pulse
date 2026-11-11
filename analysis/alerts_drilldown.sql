-- Drill-down for three questions raised by alerts_patterns.sql:
--   1. What are the ~156 alerts first seen at 03:00 local time?
--   2. Why are Thursdays the busiest weekday for new alerts?
--   3. What happened on 22 Sep and 30 Oct 2026 (most demonstration alerts)?
-- Light queries on stg_alerts. Run with:
--   docker compose exec -T postgres psql -U airflow -d torino_pulse -P pager=off < analysis/alerts_drilldown.sql

create temp view alert_level as
select
    alert_id,
    min(effect)              as effect,
    min(cause)               as cause,
    min(header_text)         as header_text,
    min(first_seen_local)    as first_seen_local,
    min(planned_start_local) as planned_start_local,
    min(planned_end_local)   as planned_end_local
from (
    select alert_id, effect, cause, header_text,
           first_seen_at_local as first_seen_local,
           active_from_local   as planned_start_local,
           active_until_local  as planned_end_local
    from stg_alerts
) s
group by alert_id;

-- 1a. The 03:00 alerts: headline, effect, cause, how many, on how many distinct days.
select
    left(header_text, 70)                    as header,
    effect,
    cause,
    count(*)                                 as alerts,
    count(distinct first_seen_local::date)   as distinct_days
from alert_level
where extract(hour from first_seen_local) = 3
  and first_seen_local >= '2026-08-20'
group by 1, 2, 3
order by alerts desc
limit 15;

-- 1b. Are they daily? Alerts first seen at 03:00 per date, most recent 14 days with any.
select first_seen_local::date as day, count(*) as alerts_at_03
from alert_level
where extract(hour from first_seen_local) = 3
  and first_seen_local >= '2026-08-20'
group by 1
order by 1 desc
limit 14;

-- 2a. New alerts per calendar day (first seen), 12 busiest days.
select first_seen_local::date as day,
       to_char(first_seen_local, 'Dy') as weekday,
       count(*) as alerts,
       count(*) filter (where effect = 'REDUCED_SERVICE') as reduced_service,
       count(*) filter (where effect = 'DETOUR')          as detour
from alert_level
where first_seen_local >= '2026-08-20'
group by 1, 2
order by alerts desc
limit 12;

-- 2b. Every Thursday: new alerts per day, to see whether a few days drive the average.
select first_seen_local::date as thursday, count(*) as alerts
from alert_level
where first_seen_local >= '2026-08-20'
  and extract(dow from first_seen_local) = 4
group by 1
order by 1;

-- 3. Demonstration alerts on 22 Sep and 30 Oct 2026, by headline (planned start date).
select
    planned_start_local::date as day,
    left(header_text, 80)     as header,
    effect,
    count(*)                  as alerts
from alert_level
where cause = 'DEMONSTRATION'
  and planned_start_local::date in ('2026-09-22', '2026-10-30')
group by 1, 2, 3
order by 1, alerts desc
limit 30;
