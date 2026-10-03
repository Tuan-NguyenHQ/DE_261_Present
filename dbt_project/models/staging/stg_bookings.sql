-- ============================================================
--  stg_bookings — Staging view for booking data
--  Silver (Spark) đã chuẩn hoá status/channel. View này chỉ là lớp an toàn:
--  so sánh trên giá trị đã lower() nên chấp nhận cả mã thô (app_ios, web, partner_*)
--  lẫn giá trị chuẩn ('Mobile App', 'Web', 'Partner API').
-- ============================================================

{{
  config(
    materialized='view'
  )
}}

SELECT
    upper(trim(booking_id))                                 AS booking_id,
    upper(trim(hotel_id))                                   AS hotel_id,
    upper(trim(user_id))                                    AS user_id,
    trim(room_type)                                         AS room_type,
    checkin_date,
    checkout_date,
    total_amount,
    upper(trim(currency))                                   AS currency,
    -- Normalize status (idempotent với giá trị đã chuẩn hoá ở silver)
    CASE upper(trim(booking_status))
        WHEN 'CONFIRMED' THEN 'CONFIRMED'
        WHEN 'CONFIRM'   THEN 'CONFIRMED'
        WHEN '1'         THEN 'CONFIRMED'
        WHEN 'CANCELLED' THEN 'CANCELLED'
        WHEN 'CANCELED'  THEN 'CANCELLED'
        WHEN 'CANCEL'    THEN 'CANCELLED'
        WHEN 'PENDING'   THEN 'PENDING'
        WHEN 'NO_SHOW'   THEN 'NO_SHOW'
        ELSE 'UNKNOWN'
    END                                                     AS booking_status,
    -- Map channel → đúng tên trong dim_channels (mọi literal đều lowercase)
    CASE
        WHEN lower(trim(booking_channel)) IN ('app_ios', 'app_android', 'mobile app')
            THEN 'Mobile App'
        WHEN lower(trim(booking_channel)) IN ('web', 'website')
            THEN 'Web'
        WHEN startsWith(lower(trim(booking_channel)), 'partner')
            THEN 'Partner API'
        ELSE 'Other'
    END                                                     AS booking_channel,
    upper(trim(payment_method))                             AS payment_method,
    nights,
    created_at,
    _batch_date
FROM {{ source('staging', 'bookings') }} FINAL
WHERE booking_id IS NOT NULL
  AND booking_id != ''
