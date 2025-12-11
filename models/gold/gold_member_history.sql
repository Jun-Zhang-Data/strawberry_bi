{{ config(
    schema = 'GOLD',
    materialized = 'view',
    
) }}

SELECT
  member_id,
  first_name,
  last_name,
  status,
  is_active,
  effective_from,
  effective_to,
  is_current
FROM {{ ref('dim_member') }}

