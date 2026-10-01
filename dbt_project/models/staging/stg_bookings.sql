-- ============================================================
--  stg_bookings — Staging view for booking data
-- ============================================================

{{
  config(
    materialized='view'
  )
}}

SELECT
    trim(booking_id)                                        AS booking_id,
    trim(hotel_id)                                          AS hotel_id,
    trim(user_id)                                           AS user_id,
    trim(room_type)                                         AS room_type,
    checkin_date,
    checkout_date,
    total_amount,
    trim(currency)                                          AS currency,
    -- Normalize status
    CASE upper(trim(booking_status))
        WHEN 'CONFIRMED' THEN 'CONFIRMED'
        WHEN 'CONFIRM'   THEN 'CONFIRMED'
        WHEN '1'         THEN 'CONFIRMED'
        WHEN 'CANCELLED' THEN 'CANCELLED'
        WHEN 'CANCEL'    THEN 'CANCELLED'
        WHEN 'PENDING'   THEN 'PENDING'
        WHEN 'NO_SHOW'   THEN 'NO_SHOW'
        ELSE 'UNKNOWN'
    END                                                     AS booking_status,
    -- Map channel
    CASE
        WHEN lower(trim(booking_channel)) IN ('app_ios','app_android','Mobile App')
            THEN 'Mobile App'
        WHEN lower(trim(booking_channel)) IN ('web','website','Web')
            THEN 'Web'
        WHEN lower(trim(booking_channel)) LIKE 'partner%'
            OR lower(trim(booking_channel)) = 'Partner API'
            THEN 'Partner API'
        ELSE 'Other'
    END                                                     AS booking_channel,
    trim(payment_method)                                    AS payment_method,
    nights,
    _batch_date
FROM {{ source('staging', 'bookings') }} FINAL
WHERE booking_id IS NOT NULL
  AND booking_id != ''
