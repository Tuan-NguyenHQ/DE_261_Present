-- ============================================================
--  dim_customers — Customer dimension, SCD Type 2 (PII masked)
--  Cùng thuật toán với dim_hotels:
--    snapshot hằng ngày → attr_hash → điểm thay đổi → valid_from/valid_to/is_current
--  customer_sk = cityHash64(user_id, valid_from) (ổn định), unknown member = 0
--  Không theo dõi loyalty_points/total_bookings (thay đổi liên tục, không phải thuộc tính dim)
-- ============================================================

{{
  config(
    materialized='table',
    engine='MergeTree()',
    order_by='(user_id, valid_from)'
  )
}}

WITH snapshots AS (
    SELECT
        *,
        cityHash64(email_hash, country, user_segment, loyalty_tier,
                   first_booking_date, is_active)               AS attr_hash
    FROM {{ ref('stg_customers') }}
),

flagged AS (
    SELECT
        *,
        lagInFrame(attr_hash, 1, toUInt64(0)) OVER w            AS prev_hash,
        row_number() OVER w                                      AS rn
    FROM snapshots
    WINDOW w AS (PARTITION BY user_id ORDER BY _batch_date
                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),

versions AS (
    SELECT *
    FROM flagged
    WHERE rn = 1 OR attr_hash != prev_hash
),

ranged AS (
    SELECT
        *,
        _batch_date                                              AS valid_from,
        leadInFrame(_batch_date, 1, toDate('9999-12-31')) OVER (
            PARTITION BY user_id ORDER BY _batch_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )                                                        AS next_valid_from
    FROM versions
)

SELECT
    cityHash64(user_id, valid_from)                              AS customer_sk,
    user_id,
    email_hash,
    country,
    user_segment,
    loyalty_tier,
    first_booking_date,
    is_active,
    valid_from,
    if(next_valid_from = toDate('9999-12-31'),
       next_valid_from, next_valid_from - 1)                     AS valid_to,
    next_valid_from = toDate('9999-12-31')                       AS is_current
FROM ranged

UNION ALL

-- Unknown member (khách ẩn danh / không tra được)
SELECT
    toUInt64(0)                 AS customer_sk,
    'UNKNOWN'                   AS user_id,
    ''                          AS email_hash,
    'UNKNOWN'                   AS country,
    'unknown'                   AS user_segment,
    'Unknown'                   AS loyalty_tier,
    toDate('1970-01-01')        AS first_booking_date,
    false                       AS is_active,
    toDate('1970-01-01')        AS valid_from,
    toDate('9999-12-31')        AS valid_to,
    true                        AS is_current
