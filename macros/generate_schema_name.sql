{% macro generate_schema_name(custom_schema_name, node) -%}
  {# 
    Override dbt's default behavior.
    If a schema is set on the model (custom_schema_name), use that.
    Otherwise, fall back to target.schema.
  #}
  {%- if custom_schema_name is none or custom_schema_name == '' -%}
    {{ target.schema }}
  {%- else -%}
    {{ custom_schema_name }}
  {%- endif -%}
{%- endmacro %}
