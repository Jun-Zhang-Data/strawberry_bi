WITH ordered AS (
  SELECT
    member_id,
    effective_from,
    effective_to,
    LAG(effective_to) OVER (
      PARTITION BY member_id
      ORDER BY effective_from
    ) AS prev_effective_to
  FROM {{ ref('dim_member') }}
)

SELECT *
FROM ordered
WHERE prev_effective_to IS NOT NULL
  AND effective_from < prev_effective_to