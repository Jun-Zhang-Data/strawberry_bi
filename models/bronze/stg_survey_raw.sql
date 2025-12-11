{{ config(
    schema = 'BRONZE',
    materialized = 'view',
    
) }}

SELECT
    record:"ID"::BIGINT                        AS survey_id,
    record:"Member_id"::VARCHAR                AS member_id,
    record:"Is_anonymous"::BOOLEAN             AS is_anonymous,
    record:"Submitted_on_utc"::TIMESTAMP_NTZ   AS submitted_on_utc,
    record:"Reservation_no"::VARCHAR           AS reservation_no,
    record:"Hotel_id"::BIGINT                  AS hotel_id,
    record                                      AS survey_json,
    src_file_name,
    load_ts_utc
FROM {{ source('raw', 'survey_raw') }}


