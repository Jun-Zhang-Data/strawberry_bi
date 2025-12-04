-- Convenience macro to tag models by layer
{% macro bronze() -%}
  {{ config(tags=['bronze']) }}
{%- endmacro %}

{% macro silver() -%}
  {{ config(tags=['silver']) }}
{%- endmacro %}

{% macro gold() -%}
  {{ config(tags=['gold']) }}
{%- endmacro %}
