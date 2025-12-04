{% test not_before(model, column_name, earlier_column, later_column) %}

SELECT
  {{ earlier_column }} AS earlier_value,
  {{ later_column }}  AS later_value
FROM {{ model }}
WHERE {{ later_column }} < {{ earlier_column }}

{% endtest %}

