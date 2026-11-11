-- HEAVY (scans stg_trip_updates, roughly 5-10 minutes on the VM). Pause the
-- dbt_transform DAG first, or the two will block each other.
--
-- Share of non-zero, latest-per-stop en-route delay observations that have no route, by month.
-- These are the observations that fct_route_performance cannot include.
select
    date_trunc('month', fetched_at)::date                                     as month,
    count(*)                                                                  as delay_observations,
    round(100.0 * count(*) filter (where route_id is null) / count(*), 1)     as pct_without_route
from stg_trip_updates
where is_en_route
  and is_latest_observation
  and arrival_delay_seconds <> 0
group by 1
order by 1;
