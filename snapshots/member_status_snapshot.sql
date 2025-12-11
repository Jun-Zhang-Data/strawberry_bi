{% snapshot member_status_snapshot %}

{{
    config(
      target_schema = 'SILVER',
      unique_key = 'member_id',
      strategy = 'timestamp',
      updated_at = 'snapshot_loaded_at', 
    
)}}

SELECT
  member_id,
  first_name,
  last_name,
  status,
  is_active,
  snapshot_loaded_at
FROM {{ ref('stg_membership') }}

{% endsnapshot %}

