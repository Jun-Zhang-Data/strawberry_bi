{{ config(
    schema = 'BRONZE',
    materialized = 'view'
) }}

-- Quarantine rows from stg_membership that violate key data quality rules.

with base as (

    select *
    from {{ ref('stg_membership') }}

),

flagged as (

    select
        *,
        case
            when member_id is null then 'MISSING_MEMBER_ID'
            when status not in ('BRONZE', 'SILVER', 'GOLD', 'PLATINUM') then 'INVALID_STATUS'
            when is_active is null then 'MISSING_IS_ACTIVE'
        end as dq_issue
    from base

)

select
    member_id,
    first_name,
    last_name,
    status,
    is_active,
    src_file_name,
    snapshot_loaded_at,
    dq_issue
from flagged
where dq_issue is not null
