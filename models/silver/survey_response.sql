{{ config(
    schema = 'SILVER',
    materialized = 'view',
    
) }}

SELECT
  survey_id        AS response_id,
  survey_id        AS survey_id,
  member_id,
  reservation_no,
  hotel_id,
  submitted_on_utc,
  is_anonymous,
  load_ts_utc
FROM {{ ref('stg_survey_raw') }}

