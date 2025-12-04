{{ config(
    schema = 'SILVER',
    materialized = 'view',
    tags = ['silver']
) }}

WITH base AS (
    SELECT * FROM {{ ref('stg_pms_reservations') }}
),

with_member AS (
    SELECT
      b.*,
      dm.member_sk,
      dm.status AS member_status
    FROM base b
    LEFT JOIN {{ ref('dim_member') }} dm
      ON b.member_id = dm.member_id
     AND b.booking_start_date >= dm.effective_from
     AND (b.booking_start_date < dm.effective_to OR dm.effective_to IS NULL)
),

with_hotel AS (
    SELECT
      wm.*,
      dh.hotel_name,
      dh.brand,
      dh.region
    FROM with_member wm
    LEFT JOIN {{ ref('dim_hotel') }} dh
      ON wm.hotel_id = dh.hotel_id
)

SELECT
  reservation_id,
  reservation_no,
  member_id,
  member_sk,
  hotel_id,
  hotel_name,
  brand,
  region,
  reservation_date,
  booking_start_date,
  booking_end_date,
  status_code,
  member_status,
  total_amount,
  room_rate,
  DATEDIFF('day', booking_start_date, booking_end_date) AS nights,
  updated_time_utc,
  load_ts_utc
FROM with_hotel
