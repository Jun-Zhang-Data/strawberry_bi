{{ config(
    schema = 'SILVER',
    materialized = 'view'
) }}

with base as (
    select *
    from {{ ref('stg_loyalty_events') }}
),

with_member as (
    select
        b.*,
        dm.member_sk,
        dm.status as member_status
    from base b
    left join {{ ref('dim_member') }} dm
      on  b.member_id = dm.member_id
      and b.created_at >= dm.effective_from
      and (b.created_at < dm.effective_to or dm.effective_to is null)
),

matched as (
    select *
    from with_member
    where member_sk is not null
)

select
    event_id,
    member_id,
    member_sk,
    event_type,
    points,
    created_at,
    member_status,
    load_ts_utc
from matched
