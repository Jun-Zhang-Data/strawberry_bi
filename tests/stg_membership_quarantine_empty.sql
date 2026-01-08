-- Fail if any quarantined membership rows exist
select *
from {{ ref('stg_membership_quarantine') }}
