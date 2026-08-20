{{ config(materialized='view') }}

-- Which routes are affected by active service alerts, and how.
--
-- stg_alerts holds one row per (alert, affected entity), so a single
-- alert naming 20 stops produces 20 rows. Here we roll back up to the
-- route so each row answers "what is going on with this line".

with route_alerts as (
    select
        route_id,
        alert_id,
        effect,
        cause,
        severity_level,
        header_text,
        active_from_local,
        active_until_local,
        first_seen_at_local,
        last_seen_at_local,
        times_observed,
        is_currently_active
    from {{ ref('stg_alerts') }}
    where route_id is not null
),

-- Stops touched by an alert, counted per route via the alert they share
alert_stop_counts as (
    select
        alert_id,
        count(distinct stop_id) as stops_affected
    from {{ ref('stg_alerts') }}
    where stop_id is not null
    group by 1
)

select
    r.route_id,
    r.route_short_name,
    r.route_long_name,
    r.route_mode,
    r.service_area,

    a.alert_id,
    a.effect,
    a.cause,
    a.severity_level,
    a.header_text,

    coalesce(sc.stops_affected, 0) as stops_affected,

    a.active_from_local,
    a.active_until_local,
    a.active_until_local - a.active_from_local as planned_duration,
    a.is_currently_active,

    a.first_seen_at_local,
    a.last_seen_at_local,
    a.times_observed

from route_alerts a
join {{ ref('stg_routes') }} r
    on a.route_id = r.route_id
left join alert_stop_counts sc
    on a.alert_id = sc.alert_id