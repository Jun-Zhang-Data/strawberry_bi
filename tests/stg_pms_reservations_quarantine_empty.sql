-- Fail if any quarantined reservations exist
select *
from {{ ref('stg_pms_reservations_quarantine') }}