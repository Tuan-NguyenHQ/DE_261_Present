-- ============================================================
--  fact_hotel_inventory — Hotel room inventory fact
--  Grain: 1 row = hotel × ngày × loại phòng
-- ============================================================

{{
  config(
    materialized='incremental',
    unique_key='(hotel_sk, date_id, room_type)',
    incremental_strategy='delete+insert',
    order_by='(hotel_sk, date_id, room_type)',
    on_schema_change='append_new_columns'
  )
}}

WITH booking_counts AS (
    SELECT
        hotel_id,
        room_type,
        checkin_date                                     AS stay_date,
        count(*)                                         AS booked_rooms,
        avg(total_amount / greatest(nights, 1))          AS adr
    FROM {{ ref('stg_bookings') }}
    WHERE booking_status = 'CONFIRMED'
    {% if is_incremental() %}
      AND _batch_date = '{{ var("execution_date") }}'
    {% endif %}
    GROUP BY hotel_id, room_type, checkin_date
)

SELECT
    h.hotel_sk,
    toUInt32(formatDateTime(bc.stay_date, '%Y%m%d'))      AS date_id,
    bc.room_type,
    toUInt16(h.total_rooms)                               AS total_rooms,
    toUInt16(bc.booked_rooms)                             AS booked_rooms,
    toUInt16(greatest(h.total_rooms - bc.booked_rooms, 0)) AS available_rooms,
    ROUND(bc.booked_rooms * 100.0 / greatest(h.total_rooms, 1), 2) AS occupancy_rate,
    ROUND(bc.adr, 2)                                      AS adr,
    ROUND(bc.adr * bc.booked_rooms / greatest(h.total_rooms, 1), 2) AS revpar,
    bc.stay_date                                          AS _batch_date

FROM booking_counts bc
LEFT JOIN {{ ref('dim_hotels') }} h ON bc.hotel_id = h.hotel_id AND h.is_current
WHERE h.hotel_sk IS NOT NULL
