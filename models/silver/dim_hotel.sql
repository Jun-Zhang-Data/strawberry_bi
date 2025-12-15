{{ config(
    schema = 'SILVER',
    materialized = 'view'
) }}

-- Hotel lookup table split out from PMS reservations.
-- One row per hotel_id seen in stg_pms_reservations.

with hotel_keys as (
    select distinct
        hotel_id
    from {{ ref('stg_pms_reservations') }}
    where hotel_id is not null
)

select
    hotel_id
from hotel_keys
