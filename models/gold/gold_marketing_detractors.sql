{{ config(
    schema = 'GOLD',
    materialized = 'view',
    
) }}

WITH scored AS (
    SELECT
      sa.*,
      TRY_TO_NUMBER(sa.answer_value) AS numeric_score
    FROM {{ ref('survey_answer') }} sa
),

platinum_members AS (
    SELECT DISTINCT
      member_id
    FROM {{ ref('dim_member') }}
    WHERE status = 'PLATINUM'
      AND is_current
),

joined AS (
    SELECT
      s.survey_answer_sk,
      s.survey_id,
      s.member_id,
      s.question_text,
      s.answer_value,
      s.numeric_score,
      sr.submitted_on_utc,
      sr.hotel_id
    FROM scored s
    JOIN {{ ref('survey_response') }} sr
      ON s.survey_id = sr.survey_id
    JOIN platinum_members p
      ON s.member_id = p.member_id
)

SELECT
  survey_answer_sk,
  survey_id,
  member_id,
  question_text,
  answer_value,
  numeric_score,
  submitted_on_utc,
  hotel_id
FROM joined
WHERE numeric_score IS NOT NULL
  AND numeric_score <= 3

