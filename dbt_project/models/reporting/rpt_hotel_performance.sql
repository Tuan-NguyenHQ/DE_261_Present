-- ============================================================
--  rpt_hotel_performance — Hiệu suất khách sạn (lũy kế đến execution_date)
--  - Gom fact theo hotel_id (qua MỌI version SCD2), hiển thị thuộc tính version hiện tại
--    → không mất booking gắn với version cũ
--  - Tiền tệ: USD; refund dùng refund_amount_usd
--  - Pre-aggregate bookings & payments riêng để tránh fan-out khi join
-- ============================================================

{{ config(materialized='table', engine='MergeTree()', order_by='(hotel_id)') }}

WITH sk_map AS (
    SELECT hotel_sk, hotel_id
    FROM {{ ref('dim_hotels') }}
    WHERE hotel_sk != 0
),

bookings AS (
    SELECT
        m.hotel_id                                                AS hotel_id,
        uniqExact(f.booking_id)                                   AS total_bookings,
        countIf(f.is_cancelled)                                   AS cancelled_bookings,
        sumIf(f.amount_usd, f.booking_status = 'CONFIRMED')       AS total_revenue_usd,
        avg(f.nights)                                             AS avg_stay_nights
    FROM {{ ref('fact_bookings') }} AS f FINAL
    INNER JOIN sk_map m ON f.hotel_sk = m.hotel_sk
    WHERE f._batch_date <= toDate('{{ var("execution_date") }}')
    GROUP BY m.hotel_id
),

payments AS (
    SELECT
        m.hotel_id                                                AS hotel_id,
        sumIf(p.amount_usd, p.payment_status = 'SUCCESS')         AS total_payments_usd,
        sumIf(p.refund_amount_usd, p.is_refunded)                 AS total_refunds_usd
    FROM {{ ref('fact_payments') }} AS p FINAL
    INNER JOIN sk_map m ON p.hotel_sk = m.hotel_sk
    WHERE p._batch_date <= toDate('{{ var("execution_date") }}')
    GROUP BY m.hotel_id
)

SELECT
    h.hotel_id                                                    AS hotel_id,
    h.hotel_name                                                  AS hotel_name,
    h.city                                                        AS city,
    h.country                                                     AS country,
    h.hotel_tier                                                  AS hotel_tier,
    h.star_rating                                                 AS star_rating,
    ifNull(b.total_bookings, 0)                                   AS total_bookings,
    ifNull(b.cancelled_bookings, 0)                               AS cancelled_bookings,
    ifNull(b.total_revenue_usd, 0)                                AS total_revenue_usd,
    b.avg_stay_nights                                             AS avg_stay_nights,
    round(100.0 * b.cancelled_bookings / nullIf(b.total_bookings, 0), 2)
                                                                  AS cancellation_rate_pct,
    ifNull(p.total_payments_usd, 0)                               AS total_payments_usd,
    ifNull(p.total_refunds_usd, 0)                                AS total_refunds_usd,
    toDate('{{ var("execution_date") }}')                         AS report_date
FROM {{ ref('dim_hotels') }} h
LEFT JOIN bookings b ON h.hotel_id = b.hotel_id
LEFT JOIN payments p ON h.hotel_id = p.hotel_id
WHERE h.is_current
  AND h.hotel_sk != 0
