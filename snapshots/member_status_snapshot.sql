{% snapshot member_status_snapshot %}

{{
    config(
      target_schema = 'SILVER',
      unique_key = 'member_id',
      strategy = 'timestamp',
      updated_at = 'load_ts_utc', 
    
)}}

SELECT
  member_id,
  first_name,
  last_name,
  status,
  is_active,
  load_ts_utc
FROM {{ ref('stg_membership') }}

{% endsnapshot %}

