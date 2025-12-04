{% test column_is_numeric(model, column_name) %}

select
  {{ column_name }} as value
from {{ model }}
where try_to_number({{ column_name }}::varchar) is null
  and {{ column_name }} is not null

{% endtest %}

