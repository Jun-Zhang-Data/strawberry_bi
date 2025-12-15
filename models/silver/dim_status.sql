{{ config(
    schema = 'SILVER',
    materialized = 'view'
) }}

-- Status lookup table split out from PMS reservations.
-- One row per status_code seen in stg_pms_reservations.

with status_codes as (
    select distinct
        status_code
    from {{ ref('stg_pms_reservations') }}
    where status_code is not null
)

select
    status_code
from status_codes
