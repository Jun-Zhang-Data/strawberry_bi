{{ config(
    schema = 'BRONZE',
    materialized = 'view',
    
) }}

SELECT
  ID::VARCHAR                AS member_id,
  FIRST_NAME::VARCHAR        AS first_name,
  LAST_NAME::VARCHAR         AS last_name,
  STATUS::VARCHAR            AS status,
  IS_ACTIVE::BOOLEAN         AS is_active,
  load_ts_utc::TIMESTAMP_LTZ AS snapshot_loaded_at,
  src_file_name
FROM {{ source('raw', 'membership_raw') }}


