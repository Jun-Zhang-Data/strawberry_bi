{% test row_count_equals(model, compare_model) %}

WITH a AS (
    SELECT COUNT(*) AS cnt FROM {{ model }}
),
b AS (
    SELECT COUNT(*) AS cnt FROM {{ compare_model }}
)

SELECT
  a.cnt AS model_count,
  b.cnt AS compare_count
FROM a, b
WHERE a.cnt <> b.cnt

{% endtest %}
