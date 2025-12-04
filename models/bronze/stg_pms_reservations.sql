{{ config(
    schema = 'BRONZE',
    materialized = 'incremental',
    unique_key = 'reservation_id',
    incremental_strategy = 'merge',
    tags = ['bronze']
) }}

WITH source AS (

    SELECT
        record:"ID"::BIGINT                      AS reservation_id,
        record:"Reservation_no"::VARCHAR         AS reservation_no,
        record:"Reservation_date"::DATE          AS reservation_date,
        record:"Updated_time_utc"::TIMESTAMP_NTZ AS updated_time_utc,
        record:"Member_id"::VARCHAR              AS member_id,
        record:"Hotel_id"::BIGINT                AS hotel_id,
        record:"Booking_start_date"::DATE        AS booking_start_date,
        record:"Booking_end_date"::DATE          AS booking_end_date,
        record:"Status_code"::VARCHAR            AS status_code,
        record:"Room_rate"::NUMBER(18,2)         AS room_rate,
        record:"Total_amount_gross"::NUMBER(18,2) AS total_amount_gross,
        src_file_name,
        load_ts_utc
    FROM {{ source('raw', 'pms_raw') }}

    {% if is_incremental() %}

      -- Only re-scan last 48 hours in RAW based on load_ts_utc
      WHERE load_ts_utc >= DATEADD(
        hour,
        -48,
        COALESCE(
          (SELECT MAX(load_ts_utc) FROM {{ this }}),
          '1900-01-01'::TIMESTAMP_NTZ
        )
      )

    {% endif %}
),

dedup AS (

    SELECT
        reservation_id,
        reservation_no,
        reservation_date,
        updated_time_utc,
        member_id,
        hotel_id,
        booking_start_date,
        booking_end_date,
        status_code,
        room_rate,
        total_amount_gross,
        src_file_name,
        load_ts_utc,
        ROW_NUMBER() OVER (
          PARTITION BY reservation_id
          ORDER BY updated_time_utc DESC, load_ts_utc DESC
        ) AS rn
    FROM source
)

SELECT
    reservation_id,
    reservation_no,
    reservation_date,
    updated_time_utc,
    member_id,
    hotel_id,
    booking_start_date,
    booking_end_date,
    status_code,
    room_rate,
    total_amount_gross,
    src_file_name,
    load_ts_utc
FROM dedup
WHERE rn = 1


