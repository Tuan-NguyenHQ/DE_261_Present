-- ============================================================
--  stg_payments — Staging view for payment transactions
-- ============================================================

{{
  config(
    materialized='view'
  )
}}

SELECT
    trim(payment_id)                              AS payment_id,
    trim(booking_id)                              AS booking_id,
    amount,
    upper(trim(currency))                         AS currency,
    trim(payment_method)                          AS payment_method,
    upper(trim(payment_status))                   AS payment_status,
    trim(gateway_code)                            AS gateway_code,
    processed_at,
    COALESCE(refund_amount, 0)                    AS refund_amount,
    refund_at,
    _batch_date
FROM {{ source('staging', 'payments') }} FINAL
WHERE payment_id IS NOT NULL
  AND payment_id != ''
