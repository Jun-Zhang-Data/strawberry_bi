{{ config(
    schema = 'BRONZE',
    materialized = 'view'
) }}

-- Quarantine rows from stg_survey_raw that violate basic data quality rules.

with base as (

    select *
    from {{ ref('stg_survey_raw') }}

),

flagged as (

    select
        *,
        case
            when survey_id is null then 'MISSING_SURVEY_ID'
            when submitted_on_utc is null then 'MISSING_SUBMITTED_ON_UTC'
            -- add more simple checks here once you confirm column names
        end as dq_issue
    from base

)

select
    survey_id,
    submitted_on_utc,
    dq_issue
from flagged
where dq_issue is not null

