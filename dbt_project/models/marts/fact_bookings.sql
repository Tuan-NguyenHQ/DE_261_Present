-- ============================================================
--  fact_bookings — Booking fact table
--  Grain: 1 row = 1 booking
-- ============================================================

{{
  config(
    materialized='incremental',
    unique_key='booking_id',
    incremental_strategy='delete+insert',
    order_by='(hotel_sk, checkin_date_id, booking_id)',
    on_schema_change='append_new_columns'
  )
}}

SELECT
    b.booking_id,
    h.hotel_sk,
    c.customer_sk,
    toUInt32(formatDateTime(b.checkin_date,  '%Y%m%d'))   AS checkin_date_id,
    toUInt32(formatDateTime(b.checkout_date, '%Y%m%d'))   AS checkout_date_id,
    toUInt32(formatDateTime(b._batch_date,   '%Y%m%d'))   AS booked_date_id,
    ch.channel_id,
    b.nights,
    b.total_amount,
    -- Simple USD conversion (placeholder rates)
    CASE
        WHEN b.currency = 'VND' THEN toDecimal64(ROUND(b.total_amount / 24500, 2), 2)
        WHEN b.currency = 'THB' THEN toDecimal64(ROUND(b.total_amount / 35, 2), 2)
        WHEN b.currency = 'SGD' THEN toDecimal64(ROUND(b.total_amount / toDecimal64(1.35, 2), 2), 2)
        WHEN b.currency = 'IDR' THEN toDecimal64(ROUND(b.total_amount / 15500, 2), 2)
        ELSE toDecimal64(b.total_amount, 2)
    END                                                   AS amount_usd,
    b.currency,
    b.booking_status,
    b.room_type,
    b.booking_status = 'CANCELLED'                        AS is_cancelled,
    b.booking_status = 'NO_SHOW'                          AS is_no_show,
    b._batch_date

FROM {{ ref('stg_bookings') }}            b
LEFT JOIN {{ ref('dim_hotels') }}         h   ON b.hotel_id        = h.hotel_id  AND h.is_current
LEFT JOIN {{ ref('dim_customers') }}      c   ON b.user_id         = c.user_id   AND c.is_current
LEFT JOIN {{ ref('dim_channels') }}       ch  ON b.booking_channel = ch.channel_name

{% if is_incremental() %}
WHERE b._batch_date = '{{ var("execution_date") }}'
{% endif %}
