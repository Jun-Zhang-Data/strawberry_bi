{% snapshot member_status_snapshot %}

{{
    config(
      target_schema = 'SILVER',
      unique_key = 'member_id',
      strategy = 'check',
      check_cols = ['first_name','last_name','status','is_active']
    )
}}

SELECT
  member_id,
  first_name,
  last_name,
  status,
  is_active,
  snapshot_loaded_at
FROM {{ ref('stg_membership') }}

{% endsnapshot %}


