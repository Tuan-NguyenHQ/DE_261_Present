-- ============================================================
--  stg_customers — Staging view for customer data (PII masked)
-- ============================================================

{{
  config(
    materialized='view'
  )
}}

SELECT
    upper(trim(user_id))                          AS user_id,
    email_hash,
    phone_hash,
    upper(trim(country))                          AS country,
    lower(trim(user_segment))                     AS user_segment,
    trim(loyalty_tier)                            AS loyalty_tier,
    loyalty_points,
    first_booking_date,
    total_bookings,
    is_active,
    registered_at,
    _batch_date
FROM {{ source('staging', 'customers') }} FINAL
WHERE user_id IS NOT NULL
  AND user_id != ''
