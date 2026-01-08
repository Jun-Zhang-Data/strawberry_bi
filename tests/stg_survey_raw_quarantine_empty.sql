-- Fail if any quarantined survey rows exist
select *
from {{ ref('stg_survey_raw_quarantine') }}
