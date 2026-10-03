-- ============================================================
--  fact_hotel_inventory — Grain: hotel × stay night × room_type
--  - Mỗi booking CONFIRMED được bung thành từng đêm lưu trú (checkin .. checkout-1)
--  - Build lại toàn bộ (table): một đêm lưu trú nhận booking từ nhiều batch khác nhau,
--    nên incremental theo _batch_date sẽ ghi đè mất số đã đặt từ các ngày trước
--  - ADR / RevPAR tính bằng USD (tỷ giá seed fx_rates) thay vì cộng lẫn tiền tệ
--  - Capacity: version dim_hotels hiệu lực tại đêm lưu trú (ASOF)
-- ============================================================

{{
  config(
    materialized='table',
    engine='MergeTree()',
    order_by='(hotel_sk, date_id, room_type)'
  )
}}

WITH confirmed AS (
    SELECT
        b.hotel_id,
        b.room_type,
        b.checkin_date,
        greatest(b.nights, 1)                                        AS nights,
        {{ to_usd('b.total_amount', 'fx.units_per_usd') }}           AS amount_usd
    FROM {{ ref('stg_bookings') }} b
    ASOF LEFT JOIN {{ ref('fx_rates') }} fx
        ON b.currency = fx.currency AND b._batch_date >= fx.valid_from
    WHERE b.booking_status = 'CONFIRMED'
),

nightly AS (
    SELECT
        hotel_id,
        room_type,
        addDays(checkin_date, arrayJoin(range(toUInt32(nights))))    AS stay_date,
        amount_usd / nights                                          AS night_rate_usd
    FROM confirmed
),

agg AS (
    SELECT
        hotel_id,
        room_type,
        stay_date,
        count()                                                      AS booked_rooms,
        avgIf(night_rate_usd, night_rate_usd IS NOT NULL)            AS adr_usd
    FROM nightly
    GROUP BY hotel_id, room_type, stay_date
)

SELECT
    ifNull(h.hotel_sk, toUInt64(0))                                  AS hotel_sk,
    toUInt32(formatDateTime(a.stay_date, '%Y%m%d'))                  AS date_id,
    CAST(a.room_type AS LowCardinality(String))                      AS room_type,
    toUInt16(ifNull(h.total_rooms, 0))                               AS total_rooms,
    toUInt16(a.booked_rooms)                                         AS booked_rooms,
    toUInt16(greatest(toInt64(ifNull(h.total_rooms, 0)) - toInt64(a.booked_rooms), 0))
                                                                     AS available_rooms,
    toFloat32(round(a.booked_rooms * 100.0 / greatest(ifNull(h.total_rooms, 0), 1), 2))
                                                                     AS occupancy_rate,
    toDecimal64(round(a.adr_usd, 2), 2)                              AS adr_usd,
    toDecimal64(round(a.adr_usd * a.booked_rooms / greatest(ifNull(h.total_rooms, 0), 1), 2), 2)
                                                                     AS revpar_usd,
    a.stay_date                                                      AS stay_date
FROM agg a
ASOF LEFT JOIN {{ ref('dim_hotels') }} h
    ON a.hotel_id = h.hotel_id AND a.stay_date >= h.valid_from
