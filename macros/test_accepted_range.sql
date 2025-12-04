{% test accepted_range(model, column_name, min_value=None, max_value=None) %}

SELECT
  {{ column_name }} AS value
FROM {{ model }}
WHERE 1=1
  {% if min_value is not none %}
    AND {{ column_name }} < {{ min_value }}
  {% endif %}
  {% if max_value is not none %}
    AND {{ column_name }} > {{ max_value }}
  {% endif %}

{% endtest %}
