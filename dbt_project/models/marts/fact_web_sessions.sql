-- ============================================================
--  fact_web_sessions — Web session fact table
--  Grain: 1 row = 1 session (aggregated from clickstream)
-- ============================================================

{{
  config(
    materialized='incremental',
    unique_key='session_id',
    incremental_strategy='delete+insert',
    order_by='(session_date_id, session_id)',
    on_schema_change='append_new_columns'
  )
}}

WITH session_agg AS (
    SELECT
        session_id,
        -- Take the first non-null user_id in the session
        any(user_id)                                      AS user_id,
        toDate(min(event_timestamp))                      AS session_date,
        count(*)                                          AS total_events,
        countIf(event_type = 'page_view')                 AS page_views,
        countIf(event_type = 'search')                    AS search_count,
        countIf(event_type = 'click' AND hotel_id != '')  AS hotels_viewed,
        dateDiff('second',
                 min(event_timestamp),
                 max(event_timestamp))                    AS session_duration_sec,
        countIf(event_type = 'booking_complete') > 0      AS converted,
        any(device_type)                                  AS device_type,
        _batch_date
    FROM {{ ref('stg_clickstream') }}
    {% if is_incremental() %}
    WHERE _batch_date = '{{ var("execution_date") }}'
    {% endif %}
    GROUP BY session_id, _batch_date
)

SELECT
    s.session_id,
    COALESCE(c.customer_sk, 0)                            AS customer_sk,
    toUInt32(formatDateTime(s.session_date, '%Y%m%d'))     AS session_date_id,
    CASE s.device_type
        WHEN 'mobile'  THEN toUInt16(2)
        WHEN 'tablet'  THEN toUInt16(2)
        WHEN 'desktop' THEN toUInt16(1)
        ELSE toUInt16(4)
    END                                                   AS channel_id,
    s.total_events,
    s.page_views,
    toUInt16(s.search_count)                              AS search_count,
    toUInt16(s.hotels_viewed)                             AS hotels_viewed,
    s.session_duration_sec,
    s.converted,
    ''                                                    AS booking_id,
    s.device_type,
    s._batch_date

FROM session_agg s
LEFT JOIN {{ ref('dim_customers') }} c ON s.user_id = c.user_id AND c.is_current
