{{ config(
    schema = 'SILVER',
    materialized = 'view'
) }}

with base as (

    select *
    from {{ ref('stg_pms_reservations') }}

),

with_member as (

    select
        b.*,
        dm.member_sk,
        dm.status as member_status
    from base b
    left join {{ ref('dim_member') }} dm
      on  b.member_id = dm.member_id
      and b.booking_start_date >= dm.effective_from
      and (b.booking_start_date < dm.effective_to or dm.effective_to is null)

),

with_hotel as (

    select
        wm.*,
        dh.hotel_name,
        dh.brand,
        dh.region
    from with_member wm
    left join {{ ref('dim_hotel') }} dh
      on wm.hotel_id = dh.hotel_id

),

with_status as (

    select
        wh.*,
        ds.status_desc,
        ds.is_active as status_is_active
    from with_hotel wh
    left join {{ ref('dim_status') }} ds
      on wh.status_code = ds.status_code

)

select
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
    status_desc,
    member_status,
    status_is_active,
    total_amount_gross as total_amount,
    room_rate,
    datediff('day', booking_start_date, booking_end_date) as nights,
    updated_time_utc,
    load_ts_utc
from with_status
where member_sk      is null
   or hotel_name     is null
   or status_desc    is null

