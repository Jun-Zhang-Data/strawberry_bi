{{ config(
    schema = 'GOLD',
    materialized = 'view',
    
) }}

SELECT
  booking_start_date AS revenue_date,
  hotel_id,
  SUM(total_amount) AS total_revenue,
  COUNT(*)          AS reservation_count
FROM {{ ref('fact_reservations') }}
GROUP BY
  booking_start_date,
  hotel_id
