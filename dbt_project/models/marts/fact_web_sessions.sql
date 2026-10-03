-- ============================================================
--  fact_web_sessions — Grain: 1 session
--  - customer_sk: version SCD2 tại ngày session (ASOF), ẩn danh → 0
--  - booking_id: attribution — event booking_complete được nối với booking
--    cùng user_id + hotel_id + _batch_date, chọn booking có created_at gần nhất.
--    Không khớp → NULL (session converted nhưng chưa xác định được booking).
-- ============================================================

{{
  config(
    materialized='incremental',
    unique_key='session_id',
    incremental_strategy='delete+insert',
    engine='ReplacingMergeTree(_batch_date)',
    partition_by='toYYYYMM(_batch_date)',
    order_by='(session_date_id, session_id)',
    on_schema_change='append_new_columns'
  )
}}

WITH events AS (
    SELECT *
    FROM {{ ref('stg_clickstream') }}
    {% if is_incremental() %}
    WHERE _batch_date = toDate('{{ var("execution_date") }}')
    {% endif %}
),

session_agg AS (
    SELECT
        session_id,
        anyIf(user_id, user_id IS NOT NULL)                       AS user_id,
        toDate(min(event_timestamp))                              AS session_date,
        count()                                                   AS total_events,
        countIf(event_type = 'page_view')                         AS page_views,
        countIf(event_type = 'search')                            AS search_count,
        uniqExactIf(hotel_id, event_type = 'click' AND hotel_id IS NOT NULL)
                                                                  AS hotels_viewed,
        dateDiff('second', min(event_timestamp), max(event_timestamp))
                                                                  AS session_duration_sec,
        countIf(event_type = 'booking_complete') > 0              AS converted,
        any(device_type)                                          AS device_type,
        max(_batch_date)                                          AS _batch_date
    FROM events
    GROUP BY session_id
),

-- Session → booking attribution
attributed AS (
    SELECT
        e.session_id                                              AS session_id,
        argMin(b.booking_id,
               abs(dateDiff('second', e.event_timestamp, b.created_at)))
                                                                  AS booking_id
    FROM events e
    INNER JOIN {{ ref('stg_bookings') }} b
        ON  e.user_id     = b.user_id
        AND e.hotel_id    = b.hotel_id
        AND e._batch_date = b._batch_date
    WHERE e.event_type = 'booking_complete'
    GROUP BY e.session_id
)

SELECT
    s.session_id                                                  AS session_id,
    ifNull(c.customer_sk, toUInt64(0))                            AS customer_sk,
    toUInt32(formatDateTime(s.session_date, '%Y%m%d'))            AS session_date_id,
    CASE s.device_type
        WHEN 'mobile'  THEN toUInt16(2)
        WHEN 'tablet'  THEN toUInt16(2)
        WHEN 'desktop' THEN toUInt16(1)
        ELSE toUInt16(4)
    END                                                           AS channel_id,
    toUInt32(s.total_events)                                      AS total_events,
    toUInt32(s.page_views)                                        AS page_views,
    toUInt16(s.search_count)                                      AS search_count,
    toUInt16(s.hotels_viewed)                                     AS hotels_viewed,
    toUInt32(s.session_duration_sec)                              AS session_duration_sec,
    s.converted                                                   AS converted,
    a.booking_id                                                  AS booking_id,
    CAST(s.device_type AS LowCardinality(String))                 AS device_type,
    s._batch_date                                                 AS _batch_date
FROM session_agg s
LEFT JOIN attributed a ON s.session_id = a.session_id
ASOF LEFT JOIN {{ ref('dim_customers') }} c
    ON ifNull(s.user_id, '') = c.user_id AND s._batch_date >= c.valid_from
