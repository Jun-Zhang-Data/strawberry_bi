{{ config(
    schema = 'SILVER',
    materialized = 'table',
    tags = ['silver']
) }}

WITH base AS (
    SELECT * FROM {{ ref('stg_survey_raw') }}
),

flattened AS (
    SELECT
      survey_id,
      member_id,
      is_anonymous,
      submitted_on_utc,
      reservation_no,
      hotel_id,
      load_ts_utc,
      f.key::STRING   AS key,
      f.value::STRING AS value
    FROM base,
         LATERAL FLATTEN(input => survey_json) f
    WHERE key LIKE 'Question_%' OR key LIKE 'Answer_%'
),

paired AS (
    SELECT
      survey_id,
      member_id,
      is_anonymous,
      submitted_on_utc,
      reservation_no,
      hotel_id,
      load_ts_utc,
      REGEXP_REPLACE(key, '^(Question_|Answer_)', '') AS qa_index,
      MAX(CASE WHEN key LIKE 'Question_%' THEN value END) AS question_text,
      MAX(CASE WHEN key LIKE 'Answer_%' THEN value END) AS answer_value
    FROM flattened
    GROUP BY
      survey_id,
      member_id,
      is_anonymous,
      submitted_on_utc,
      reservation_no,
      hotel_id,
      load_ts_utc,
      REGEXP_REPLACE(key, '^(Question_|Answer_)', '')
)

SELECT
  {{ dbt_utils.generate_surrogate_key(['survey_id', 'qa_index']) }} AS survey_answer_sk,
  survey_id,
  member_id,
  is_anonymous,
  submitted_on_utc,
  reservation_no,
  hotel_id,
  qa_index,
  question_text,
  answer_value,
  load_ts_utc
FROM paired

