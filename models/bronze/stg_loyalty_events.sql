{{
  config(
    schema = 'BRONZE',
    materialized = 'incremental',
    unique_key = 'event_id',
    incremental_strategy = 'merge'
  )
}}

with base as (
    select
        ingestion_id,
        source,
        file_name,
        load_ts_utc,
        payload,

        payload:"event_id"::string    as event_id,
        payload:"member_id"::string   as member_id,
        payload:"event_type"::string  as event_type,
        payload:"points"::string      as points_str,

        payload:"created_at"::string  as created_at_str

    from {{ source('raw', 'external_loyalty_events_raw') }}
),

typed as (
    select
        event_id,
        member_id,
        event_type,
        try_to_number(points_str) as points,

        /* Fix "+00:00Z" -> "+00:00" (remove trailing Z only when an offset exists) */
        try_to_timestamp_tz(
          iff(
            regexp_like(created_at_str, '[-+][0-9]{2}:[0-9]{2}Z$'),
            regexp_replace(created_at_str, 'Z$', ''),
            created_at_str
          )
        ) as created_at,

        payload as raw_payload,
        load_ts_utc

    from base
),

deduped as (
    select
        *,
        row_number() over (
            partition by event_id
            order by load_ts_utc desc
        ) as rn
    from typed
)

select
    event_id,
    member_id,
    event_type,
    points,
    created_at,
    raw_payload,
    load_ts_utc
from deduped
where rn = 1

{% if is_incremental() %}
  and load_ts_utc > (
      select coalesce(max(load_ts_utc), '1900-01-01'::timestamp_tz)
      from {{ this }}
  )
{% endif %}




