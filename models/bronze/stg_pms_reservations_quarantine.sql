{{ config(
    schema = 'BRONZE',
    materialized = 'view'
) }}

with base as (
    select *
    from {{ ref('stg_pms_reservations') }}
),

flagged as (
    select
        *,
        case
            when reservation_id is null then 'MISSING_RESERVATION_ID'
            when hotel_id is null then 'MISSING_HOTEL_ID'
            when member_id is null then 'MISSING_MEMBER_ID'
            when booking_start_date is null or booking_end_date is null then 'MISSING_BOOKING_DATES'
            when booking_start_date > booking_end_date then 'NEGATIVE_STAY_LENGTH'
            when total_amount_gross < 0 then 'NEGATIVE_TOTAL_AMOUNT'
        end as dq_issue
    from base
)

select
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
    dq_issue
from flagged
where dq_issue is not null
