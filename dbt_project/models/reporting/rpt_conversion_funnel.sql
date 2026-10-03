-- ============================================================
--  rpt_conversion_funnel — Web Conversion Funnel Report
-- ============================================================

{{ config(materialized='table') }}

SELECT
    s._batch_date                                        AS report_date,
    s.device_type,

    count(*)                                             AS total_sessions,
    countIf(s.page_views > 0)                            AS sessions_with_views,
    countIf(s.search_count > 0)                          AS sessions_with_search,
    countIf(s.hotels_viewed > 0)                         AS sessions_with_hotel_view,
    countIf(s.converted)                                 AS converted_sessions,
    countIf(s.booking_id IS NOT NULL)                    AS attributed_sessions,

    -- Funnel rates
    ROUND(100.0 * countIf(s.search_count > 0)   / nullIf(count(*), 0), 2)
                                                         AS search_rate_pct,
    ROUND(100.0 * countIf(s.hotels_viewed > 0)  / nullIf(countIf(s.search_count > 0), 0), 2)
                                                         AS view_after_search_pct,
    ROUND(100.0 * countIf(s.converted)          / nullIf(count(*), 0), 2)
                                                         AS conversion_rate_pct,

    -- Engagement
    avg(s.session_duration_sec)                          AS avg_session_duration_sec,
    avg(s.page_views)                                    AS avg_page_views,
    avg(s.total_events)                                  AS avg_events_per_session

-- Build lại toàn bộ lịch sử (giữ chuỗi ngày thay vì chỉ ngày chạy gần nhất)
FROM {{ ref('fact_web_sessions') }} AS s FINAL
GROUP BY 1, 2
ORDER BY report_date, total_sessions DESC
