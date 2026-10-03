-- ============================================================
--  dim_hotels — Hotel dimension, SCD Type 2
--
--  Nguồn: stg_hotels = toàn bộ snapshot hằng ngày (hotel_id × _batch_date)
--  1. Hash các thuộc tính được theo dõi (attr_hash)
--  2. Giữ các ngày mà attr_hash khác ngày trước  → điểm bắt đầu version mới
--  3. valid_to = ngày trước version kế tiếp, version cuối → 9999-12-31, is_current
--  4. hotel_sk = cityHash64(hotel_id, valid_from)
--     → ổn định qua các lần chạy (không phụ thuộc ngày chạy), fact cũ không bị "mồ côi"
--  5. Unknown member hotel_sk = 0 cho fact không tra được hotel
--
--  Fact tra version bằng: ASOF LEFT JOIN dim_hotels h
--      ON f.hotel_id = h.hotel_id AND f.<event_date> >= h.valid_from
-- ============================================================

{{
  config(
    materialized='table',
    engine='MergeTree()',
    order_by='(hotel_id, valid_from)'
  )
}}

WITH snapshots AS (
    SELECT
        *,
        cityHash64(hotel_name, hotel_tier, star_rating, city, country, region,
                   latitude, longitude, total_rooms, facilities, is_active)  AS attr_hash
    FROM {{ ref('stg_hotels') }}
),

flagged AS (
    SELECT
        *,
        lagInFrame(attr_hash, 1, toUInt64(0)) OVER w   AS prev_hash,
        row_number() OVER w                             AS rn
    FROM snapshots
    WINDOW w AS (PARTITION BY hotel_id ORDER BY _batch_date
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
        _batch_date                                             AS valid_from,
        leadInFrame(_batch_date, 1, toDate('9999-12-31')) OVER (
            PARTITION BY hotel_id ORDER BY _batch_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )                                                       AS next_valid_from
    FROM versions
)

SELECT
    cityHash64(hotel_id, valid_from)                            AS hotel_sk,
    hotel_id,
    hotel_name,
    hotel_tier,
    star_rating,
    city,
    country,
    region,
    latitude,
    longitude,
    total_rooms,
    facilities,
    length(facilities)                                          AS facility_count,
    is_active,
    valid_from,
    if(next_valid_from = toDate('9999-12-31'),
       next_valid_from, next_valid_from - 1)                    AS valid_to,
    next_valid_from = toDate('9999-12-31')                      AS is_current
FROM ranged

UNION ALL

-- Unknown member
SELECT
    toUInt64(0)                     AS hotel_sk,
    'UNKNOWN'                       AS hotel_id,
    'Unknown hotel'                 AS hotel_name,
    'unknown'                       AS hotel_tier,
    toUInt8(0)                      AS star_rating,
    'Unknown'                       AS city,
    'UNKNOWN'                       AS country,
    'Unknown'                       AS region,
    toFloat64(0)                    AS latitude,
    toFloat64(0)                    AS longitude,
    toUInt32(0)                     AS total_rooms,
    CAST([] AS Array(String))       AS facilities,
    toUInt64(0)                     AS facility_count,
    false                           AS is_active,
    toDate('1970-01-01')            AS valid_from,
    toDate('9999-12-31')            AS valid_to,
    true                            AS is_current
