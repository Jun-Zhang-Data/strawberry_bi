{{ config(
    schema = 'SILVER',
    materialized = 'view',
    
) }}

SELECT
  survey_answer_sk,
  survey_id,
  member_id,
  qa_index,
  question_text,
  COALESCE(
  TO_VARCHAR(answer_value_num),
  TO_VARCHAR(answer_value_bool),
  TO_VARCHAR(answer_value_date),
  answer_value_s
) AS answer_value,
  load_ts_utc
FROM {{ ref('int_survey_answers_unpivoted') }}

