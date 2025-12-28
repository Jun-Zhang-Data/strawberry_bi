{{ config(
    schema = 'SILVER',
    materialized = 'table',
    
) }}

WITH base_raw AS (
    SELECT * FROM {{ ref('stg_survey_raw') }}
),


base AS (
    -- Keep only the newest version of each survey_id
    SELECT *
    FROM base_raw
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY survey_id
      ORDER BY load_ts_utc DESC
    ) = 1
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
      f.key::STRING           AS json_key,
      f.value                 AS value_v,      -- VARIANT (keeps true type)
      f.value::STRING         AS value_s
    FROM base,
         LATERAL FLATTEN(input => survey_json) f
    WHERE REGEXP_LIKE(f.key::STRING, '^(question|answer)_[0-9]+$', 'i')

),


typed AS (
    SELECT
      survey_id,
      member_id,
      is_anonymous,
      submitted_on_utc,
      reservation_no,
      hotel_id,
      load_ts_utc,

      TRY_TO_NUMBER(REGEXP_SUBSTR(json_key, '[0-9]+$')) AS qa_seq,

      CASE
        WHEN REGEXP_LIKE(json_key, '^question_', 'i') THEN 'QUESTION'
        WHEN REGEXP_LIKE(json_key, '^answer_',   'i') THEN 'ANSWER'
      END AS qa_type,

      value_v,
      value_s
    FROM flattened
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
      qa_seq,

      MAX(IFF(qa_type = 'QUESTION', value_s, NULL)) AS question_text,
      MAX(IFF(qa_type = 'ANSWER',   value_v, NULL)) AS answer_value_v,
      MAX(IFF(qa_type = 'ANSWER',   value_s, NULL)) AS answer_value_s
    FROM typed
    WHERE qa_seq IS NOT NULL
    GROUP BY
      survey_id, member_id, is_anonymous, submitted_on_utc,
      reservation_no, hotel_id, load_ts_utc, qa_seq
)

SELECT
  {{ dbt_utils.generate_surrogate_key(["survey_id", "TO_VARCHAR(qa_seq)"]) }} AS survey_answer_sk,

  survey_id,
  member_id,
  is_anonymous,
  submitted_on_utc,
  reservation_no,
  hotel_id,

  qa_seq AS qa_index,
  question_text,

  answer_value_v,
  answer_value_s,

  TRY_TO_NUMBER(answer_value_s)  AS answer_value_num,
  TRY_TO_BOOLEAN(answer_value_s) AS answer_value_bool,
  TRY_TO_DATE(answer_value_s)    AS answer_value_date,

  IFF(IS_ARRAY(answer_value_v),  answer_value_v, NULL) AS answer_value_array,
  IFF(IS_OBJECT(answer_value_v), answer_value_v, NULL) AS answer_value_object,

  load_ts_utc
FROM paired

