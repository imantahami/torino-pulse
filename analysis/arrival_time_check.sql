-- GTT sends each stop update with EITHER arrival.delay OR arrival.time, never both
-- (live feed, 11 Nov 2026: 233 delays + 94 absolute times = 327 stop updates).
-- The "time" rows were stored with a NULL delay (or 0 before 4 Nov).
--
-- Question: do those absolute times carry real-time information, or do they just
-- repeat the timetable? Compute the delay they imply (predicted arrival minus
-- scheduled arrival from gtfs_stop_times) and look at its distribution.
--
-- Uses the last 150k rows by id (post-4-Nov rows only) and today's static GTFS,
-- so it only covers trips still present in the current feed.
-- Run with: docker compose exec -T postgres psql -U airflow -d torino_pulse -P pager=off -f ... (or via stdin)

with recent as (
    select trip_id, trip_start_date, stop_sequence, arrival_time
    from raw_trip_updates
    where id > (select max(id) - 150000 from raw_trip_updates)
      and arrival_delay_seconds is null
      and arrival_time is not null
      and stop_sequence > 1
),
joined as (
    select
        r.arrival_time
          - extract(epoch from (
                (to_date(r.trip_start_date, 'YYYYMMDD')::timestamp + st.arrival_time::interval)
                at time zone 'Europe/Rome'
            ))::bigint as implied_delay_seconds
    from recent r
    join gtfs_stop_times st
      on st.trip_id = r.trip_id
     and st.stop_sequence::int = r.stop_sequence
    where st.arrival_time is not null and st.arrival_time <> ''
)
select
    count(*)                                                              as rows_matched,
    round(100.0 * count(*) filter (where implied_delay_seconds = 0) / count(*), 1)             as pct_exactly_zero,
    round(100.0 * count(*) filter (where abs(implied_delay_seconds) <= 60) / count(*), 1)      as pct_within_60s,
    percentile_disc(0.1) within group (order by implied_delay_seconds)    as p10,
    percentile_disc(0.5) within group (order by implied_delay_seconds)    as median,
    percentile_disc(0.9) within group (order by implied_delay_seconds)    as p90
from joined;

-- Also: where in the trip do the "time" rows occur, compared with "delay" rows?
select
    case when arrival_delay_seconds is not null then 'delay' else 'time' end as kind,
    count(*)                                  as rows,
    round(avg(stop_sequence), 1)              as avg_stop_sequence,
    count(distinct trip_id || trip_start_date) as trips
from raw_trip_updates
where id > (select max(id) - 150000 from raw_trip_updates)
  and stop_sequence > 1
  and (arrival_delay_seconds is not null or arrival_time is not null)
group by 1;

-- Which routes use the "time" form? A route where (almost) every stop update is a
-- "time" row would have been (almost) invisible in the route-level marts.
-- Route names come from today's static GTFS, so only trips in the current feed match.
with recent as (
    select trip_id, arrival_delay_seconds is not null as is_delay
    from raw_trip_updates
    where id > (select max(id) - 150000 from raw_trip_updates)
      and stop_sequence > 1
      and (arrival_delay_seconds is not null or arrival_time is not null)
)
select r.route_short_name,
       count(*)                                          as stop_updates,
       round(100.0 * avg((not is_delay)::int), 1)        as pct_time_rows
from recent
join gtfs_trips  t on t.trip_id  = recent.trip_id
join gtfs_routes r on r.route_id = t.route_id
group by 1
having count(*) >= 200
order by pct_time_rows desc, stop_updates desc
limit 15;

-- Do trips mix both forms, or is a trip entirely one form?
with per_trip as (
    select trip_id, trip_start_date,
           bool_or(arrival_delay_seconds is not null)                          as has_delay,
           bool_or(arrival_delay_seconds is null and arrival_time is not null) as has_time
    from raw_trip_updates
    where id > (select max(id) - 150000 from raw_trip_updates)
      and stop_sequence > 1
    group by 1, 2
)
select case when has_delay and has_time then 'mixed'
            when has_time               then 'time only'
            when has_delay              then 'delay only'
            else 'neither' end as trip_kind,
       count(*) as trips
from per_trip
group by 1
order by trips desc;
