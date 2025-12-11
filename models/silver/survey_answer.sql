{{ config(
    schema = 'SILVER',
    materialized = 'view',
    
) }}

SELECT
  survey_answer_sk,
  survey_id        AS response_id,
  survey_id,
  member_id,
  qa_index,
  question_text,
  answer_value,
  load_ts_utc
FROM {{ ref('int_survey_answers_unpivoted') }}

