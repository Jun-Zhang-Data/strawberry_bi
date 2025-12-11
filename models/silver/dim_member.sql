-- models/2_silver/dim_member.sql

{{ config(
    schema = 'SILVER',
    materialized = 'table'
) }}

with base as (

    select
        {{ dbt_utils.generate_surrogate_key(['member_id', 'dbt_valid_from']) }} as member_sk,
        member_id,
        first_name,
        last_name,
        status,
        is_active,
        dbt_valid_from  as effective_from,
        dbt_valid_to    as effective_to,
        (dbt_valid_to is null) as is_current,
        snapshot_loaded_at as load_ts_utc
    from {{ ref('member_status_snapshot') }}

)

select *
from base
