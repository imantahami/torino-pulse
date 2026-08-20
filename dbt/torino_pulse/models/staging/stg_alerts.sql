with base as (
    select
        alert_id,
        content_hash,
        cause,
        effect,
        severity_level,
        header_text,
        description_text,
        url,
        to_timestamp(active_period_start) at time zone 'UTC' as active_from,
        to_timestamp(active_period_end)   at time zone 'UTC' as active_until,
        nullif(informed_route_id, '') as route_id,
        nullif(informed_stop_id, '')  as stop_id,
        nullif(informed_trip_id, '')  as trip_id,
        first_seen_at,
        last_seen_at,
        times_seen,
        fetched_at
    from {{ source('raw', 'raw_alerts') }}
    where alert_id is not null
),

-- Ingestion already deduplicates on (alert_id, content_hash, entity),
-- so this collapses any rows written before that logic existed.
deduplicated as (
    select
        alert_id,
        route_id,
        stop_id,
        trip_id,

        min(cause)            as cause,
        min(effect)           as effect,
        min(severity_level)   as severity_level,
        min(header_text)      as header_text,
        min(description_text) as description_text,
        min(url)              as url,
        min(active_from)      as active_from,
        min(active_until)     as active_until,

        min(coalesce(first_seen_at, fetched_at)) as first_seen_at,
        max(coalesce(last_seen_at,  fetched_at)) as last_seen_at,
        sum(coalesce(times_seen, 1))             as times_observed

    from base
    group by alert_id, route_id, stop_id, trip_id
)

select
    alert_id,
    cause,
    effect,
    severity_level,
    header_text,
    description_text,
    url,

    route_id,
    stop_id,
    trip_id,

    active_from,
    active_until,
    active_from  at time zone 'UTC' at time zone 'Europe/Rome' as active_from_local,
    active_until at time zone 'UTC' at time zone 'Europe/Rome' as active_until_local,
    active_until - active_from as planned_duration,

    first_seen_at,
    last_seen_at,
    first_seen_at at time zone 'UTC' at time zone 'Europe/Rome' as first_seen_at_local,
    last_seen_at  at time zone 'UTC' at time zone 'Europe/Rome' as last_seen_at_local,
    last_seen_at - first_seen_at as observed_duration,
    times_observed,

    case
        when route_id is not null then 'route'
        when stop_id  is not null then 'stop'
        when trip_id  is not null then 'trip'
        else 'unknown'
    end as entity_type,

    now() between active_from and active_until as is_currently_active

from deduplicated