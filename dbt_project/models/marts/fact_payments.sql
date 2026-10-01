-- ============================================================
--  fact_payments — Payment fact table
--  Grain: 1 row = 1 payment transaction
-- ============================================================

{{
  config(
    materialized='incremental',
    unique_key='payment_id',
    incremental_strategy='delete+insert',
    order_by='(customer_sk, payment_date_id, payment_id)',
    on_schema_change='append_new_columns'
  )
}}

SELECT
    p.payment_id                                          AS payment_id,
    p.booking_id                                          AS booking_id,
    h.hotel_sk                                            AS hotel_sk,
    c.customer_sk                                         AS customer_sk,
    toUInt32(formatDateTime(p.processed_at, '%Y%m%d'))    AS payment_date_id,
    p.amount                                              AS amount,
    -- Simple USD conversion
    CASE
        WHEN p.currency = 'VND' THEN toDecimal64(ROUND(p.amount / 24500, 2), 2)
        WHEN p.currency = 'THB' THEN toDecimal64(ROUND(p.amount / 35, 2), 2)
        WHEN p.currency = 'SGD' THEN toDecimal64(ROUND(p.amount / toDecimal64(1.35, 2), 2), 2)
        WHEN p.currency = 'IDR' THEN toDecimal64(ROUND(p.amount / 15500, 2), 2)
        ELSE toDecimal64(p.amount, 2)
    END                                                   AS amount_usd,
    p.currency                                            AS currency,
    p.payment_status                                      AS payment_status,
    p.payment_method                                      AS payment_method,
    p.refund_amount                                       AS refund_amount,
    p.payment_status = 'REFUNDED'                         AS is_refunded,
    p._batch_date                                         AS _batch_date

FROM {{ ref('stg_payments') }}            p
LEFT JOIN {{ ref('stg_bookings') }}       b   ON p.booking_id = b.booking_id
LEFT JOIN {{ ref('dim_hotels') }}         h   ON b.hotel_id   = h.hotel_id  AND h.is_current
LEFT JOIN {{ ref('dim_customers') }}      c   ON b.user_id    = c.user_id   AND c.is_current

{% if is_incremental() %}
WHERE p._batch_date = '{{ var("execution_date") }}'
{% endif %}
