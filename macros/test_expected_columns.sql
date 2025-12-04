{% test expected_columns(model, expected_columns) %}

WITH cols AS (
    SELECT column_name
    FROM {{ target.database }}.information_schema.columns
    WHERE table_catalog = upper('{{ target.database }}')
      AND table_schema  = upper('{{ model.schema }}')
      AND table_name    = upper('{{ model.identifier }}')
),

missing AS (
    SELECT
      value::string AS expected_column
    FROM TABLE(
      FLATTEN(input => PARSE_JSON('{{ expected_columns | tojson }}'))
    )
    LEFT JOIN cols c
      ON UPPER(value::string) = UPPER(c.column_name)
    WHERE c.column_name IS NULL
)

SELECT * FROM missing

{% endtest %}


