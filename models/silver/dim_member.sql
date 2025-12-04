{{ config(
    schema = 'SILVER',
    materialized = 'table',
    tags = ['silver']
) }}

SELECT
  {{ dbt_utils.generate_surrogate_key(['member_id', 'dbt_valid_from']) }} AS member_sk,
  member_id,
  first_name,
  last_name,
  status,
  is_active,
  dbt_valid_from  AS effective_from,
  dbt_valid_to    AS effective_to,
  (dbt_valid_to IS NULL) AS is_current,
  snapshot_loaded_at AS load_ts_utc
FROM {{ ref('member_status_snapshot') }}
