-- tests/row_count_fact_vs_stg_reservations.sql
-- Fails if the row count in stg_pms_reservations
-- does NOT equal fact_reservations + fact_reservations_unmatched_dims

with stg as (
    select count(*) as row_count
    from {{ ref('stg_pms_reservations') }}
),

fact as (
    select count(*) as row_count
    from {{ ref('fact_reservations') }}
),

unmatched as (
    select count(*) as row_count
    from {{ ref('fact_reservations_unmatched_dims') }}
)

select
    stg.row_count        as stg_row_count,
    fact.row_count       as fact_matched_row_count,
    unmatched.row_count  as fact_unmatched_row_count,
    fact.row_count + unmatched.row_count as fact_total_row_count
from stg, fact, unmatched
where stg.row_count <> fact.row_count + unmatched.row_count
