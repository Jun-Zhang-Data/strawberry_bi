{{ config(
    schema = 'BRONZE',
    materialized = 'incremental',
    unique_key = 'event_id',
    incremental_strategy = 'merge'
) }}

with base as (
    select
        EVENT_ID,
        MEMBER_ID,
        EVENT_TYPE,
        try_to_number(POINTS)           as points,
        CREATED_AT                  as created_at,
        RAW_PAYLOAD,
        LOAD_TS_UTC
    from {{ source('raw', 'external_loyalty_events') }}
),

deduped as (
    select
        EVENT_ID      as event_id,
        MEMBER_ID     as member_id,
        EVENT_TYPE    as event_type,
        points,
        created_at,
        RAW_PAYLOAD   as raw_payload,
        LOAD_TS_UTC   as load_ts_utc,
        row_number() over (
            partition by EVENT_ID
            order by LOAD_TS_UTC desc
        ) as rn
    from base
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
