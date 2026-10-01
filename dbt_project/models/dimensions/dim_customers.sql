-- ============================================================
--  dim_customers — Customer dimension (PII masked, SCD Type 2)
-- ============================================================

{{
  config(
    materialized='table',
    order_by='customer_sk'
  )
}}

SELECT
    cityHash64(concat(user_id, toString(_batch_date)))   AS customer_sk,
    user_id,
    email_hash,
    country,
    user_segment,
    loyalty_tier,
    first_booking_date,
    is_active,
    _batch_date                                          AS valid_from,
    toDate('9999-12-31')                                 AS valid_to,
    true                                                 AS is_current
FROM {{ ref('stg_customers') }}
