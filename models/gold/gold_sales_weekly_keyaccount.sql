{{ config(
    schema = 'GOLD',
    materialized = 'view',
    
) }}

SELECT
  DATE_TRUNC('week', booking_start_date) AS week_start,
  hotel_id,
  COUNT(*)       AS booking_count,
  SUM(total_amount) AS booking_revenue
FROM {{ ref('fact_reservations') }}
GROUP BY
  DATE_TRUNC('week', booking_start_date),
  hotel_id
