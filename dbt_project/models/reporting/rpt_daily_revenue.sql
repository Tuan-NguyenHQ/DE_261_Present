-- ============================================================
--  rpt_daily_revenue — Doanh thu theo ngày × country × region × tier × channel
--  - Thuộc tính hotel lấy theo version SCD2 tại thời điểm booking (join hotel_sk)
--  - Chỉ cộng USD (không cộng lẫn nhiều loại tiền tệ)
--  - Build lại toàn bộ lịch sử mỗi lần chạy → giữ được chuỗi ngày
-- ============================================================

{{ config(materialized='table', engine='MergeTree()', order_by='(report_date, country, channel_name)') }}

SELECT
    f._batch_date                                             AS report_date,
    ifNull(h.country, 'Unknown')                              AS country,
    ifNull(h.region, 'Unknown')                               AS region,
    ifNull(h.hotel_tier, 'Unknown')                           AS hotel_tier,
    ifNull(ch.channel_name, 'Other')                          AS channel_name,
    count()                                                   AS total_bookings,
    countIf(f.is_cancelled)                                   AS cancelled_bookings,
    countIf(f.booking_status = 'CONFIRMED')                   AS confirmed_bookings,
    sumIf(f.amount_usd, f.booking_status = 'CONFIRMED')       AS confirmed_revenue_usd,
    countIf(f.amount_usd IS NULL)                             AS bookings_missing_fx,
    avg(f.nights)                                             AS avg_nights,
    round(100.0 * countIf(f.is_cancelled) / nullIf(count(), 0), 2)
                                                              AS cancellation_rate_pct
FROM {{ ref('fact_bookings') }} AS f FINAL
LEFT JOIN {{ ref('dim_hotels') }}   h   ON f.hotel_sk   = h.hotel_sk
LEFT JOIN {{ ref('dim_channels') }} ch  ON f.channel_id = ch.channel_id
GROUP BY report_date, country, region, hotel_tier, channel_name
