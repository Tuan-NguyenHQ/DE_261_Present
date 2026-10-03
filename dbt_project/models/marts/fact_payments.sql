-- ============================================================
--  fact_payments — Grain: 1 payment transaction
--  - hotel_sk / customer_sk kế thừa từ fact_bookings (đúng version SCD2 của booking)
--  - Payment không tìm thấy booking → is_orphan = true, sk = 0 (unknown member)
--  - amount_usd / refund_amount_usd: tỷ giá từ seed fx_rates
-- ============================================================

{{
  config(
    materialized='incremental',
    unique_key='payment_id',
    incremental_strategy='delete+insert',
    engine='ReplacingMergeTree(_batch_date)',
    partition_by='toYYYYMM(_batch_date)',
    order_by='(customer_sk, payment_date_id, payment_id)',
    on_schema_change='append_new_columns'
  )
}}

WITH p AS (
    SELECT *
    FROM {{ ref('stg_payments') }}
    {% if is_incremental() %}
    WHERE _batch_date = toDate('{{ var("execution_date") }}')
    {% endif %}
),

bk AS (
    SELECT booking_id, hotel_sk, customer_sk
    FROM {{ ref('fact_bookings') }} FINAL
)

SELECT
    p.payment_id                                              AS payment_id,
    p.booking_id                                              AS booking_id,
    ifNull(bk.hotel_sk, toUInt64(0))                          AS hotel_sk,
    ifNull(bk.customer_sk, toUInt64(0))                       AS customer_sk,
    toUInt32(formatDateTime(p.processed_at, '%Y%m%d'))        AS payment_date_id,
    p.amount                                                  AS amount,
    fx.units_per_usd                                          AS fx_units_per_usd,
    {{ to_usd('p.amount', 'fx.units_per_usd') }}              AS amount_usd,
    CAST(p.currency AS LowCardinality(String))                AS currency,
    CAST(p.payment_status AS LowCardinality(String))          AS payment_status,
    CAST(p.payment_method AS LowCardinality(String))          AS payment_method,
    p.refund_amount                                           AS refund_amount,
    {{ to_usd('p.refund_amount', 'fx.units_per_usd') }}       AS refund_amount_usd,
    p.payment_status = 'REFUNDED'                             AS is_refunded,
    bk.booking_id IS NULL                                     AS is_orphan,
    p._batch_date                                             AS _batch_date
FROM p
LEFT JOIN      bk                        ON p.booking_id = bk.booking_id
ASOF LEFT JOIN {{ ref('fx_rates') }} fx  ON p.currency = fx.currency AND p._batch_date >= fx.valid_from
