-- ============================================================
--  rpt_hotel_performance — Hotel Performance Report
--  Occupancy, Revenue, Reviews by hotel
-- ============================================================

{{ config(materialized='table') }}

SELECT
    h.hotel_id,
    h.hotel_name,
    h.city,
    h.country,
    h.hotel_tier,
    h.star_rating,

    -- Booking metrics
    count(DISTINCT fb.booking_id)                          AS total_bookings,
    countIf(fb.is_cancelled)                               AS cancelled_bookings,
    sumIf(fb.amount_usd, fb.booking_status = 'CONFIRMED') AS total_revenue_usd,
    avg(fb.nights)                                         AS avg_stay_nights,
    ROUND(100.0 * countIf(fb.is_cancelled) / nullIf(count(DISTINCT fb.booking_id), 0), 2)
                                                           AS cancellation_rate_pct,

    -- Payment metrics
    sum(fp.amount_usd)                                     AS total_payments_usd,
    sumIf(fp.refund_amount, fp.is_refunded)                AS total_refunds,

    toDate('{{ var("execution_date") }}')                  AS report_date

FROM {{ ref('dim_hotels') }}            h
LEFT JOIN {{ ref('fact_bookings') }}    fb  ON h.hotel_sk = fb.hotel_sk AND fb._batch_date = '{{ var("execution_date") }}'
LEFT JOIN {{ ref('fact_payments') }}    fp  ON fb.booking_id = fp.booking_id

WHERE h.is_current
GROUP BY
    h.hotel_id, h.hotel_name, h.city, h.country,
    h.hotel_tier, h.star_rating
ORDER BY total_revenue_usd DESC
