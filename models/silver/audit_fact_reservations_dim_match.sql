-- models/2_silver/audit_fact_reservations_dim_match.sql

{{ config(
    schema = 'SILVER',
    materialized = 'view'
) }}

with fact as (

    select *
    from {{ ref('fact_reservations') }}

),

unmatched as (

    select *
    from {{ ref('fact_reservations_unmatched_dims') }}

)

select
    current_timestamp() as audit_ts,
    (select count(*) from fact)      as matched_rows,
    (select count(*) from unmatched) as unmatched_rows,
    (select count(*) from fact) + (select count(*) from unmatched) as total_rows
