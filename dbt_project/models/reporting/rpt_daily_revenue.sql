-- ============================================================
--  rpt_daily_revenue — Daily Revenue Report
--  Cross-domain: booking × hotel × channel
-- ============================================================

{{ config(materialized='table') }}

SELECT
    f._batch_date                                         AS report_date,
    h.country,
    h.region,
    h.hotel_tier,
    ch.channel_name,
    count(*)                                              AS total_bookings,
    countIf(f.is_cancelled)                               AS cancelled_bookings,
    sumIf(f.total_amount, f.booking_status = 'CONFIRMED') AS confirmed_revenue,
    sumIf(f.amount_usd,   f.booking_status = 'CONFIRMED') AS confirmed_revenue_usd,
    avg(f.nights)                                         AS avg_nights,
    ROUND(100.0 * countIf(f.is_cancelled) / nullIf(count(*), 0), 2)
                                                          AS cancellation_rate_pct

FROM {{ ref('fact_bookings') }}        f
LEFT JOIN {{ ref('dim_hotels') }}      h   ON f.hotel_sk   = h.hotel_sk
LEFT JOIN {{ ref('dim_channels') }}    ch  ON f.channel_id = ch.channel_id

WHERE f._batch_date = '{{ var("execution_date") }}'
GROUP BY 1, 2, 3, 4, 5
ORDER BY confirmed_revenue_usd DESC
