{{ config(
    schema = 'SILVER',
    materialized = 'view',
    tags = ['silver']
) }}

SELECT
  question_text
FROM {{ ref('int_survey_answers_unpivoted') }}
WHERE question_text IS NOT NULL
GROUP BY question_text

