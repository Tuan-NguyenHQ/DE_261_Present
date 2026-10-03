-- ============================================================
--  SCD2 integrity — dim_hotels & dim_customers
--  Trả về dòng vi phạm (test pass khi 0 dòng):
--   1. Mỗi natural key có đúng 1 version is_current
--   2. Version không chồng lấn / không hở: valid_from(version sau) = valid_to(version trước) + 1
--   3. valid_from <= valid_to
-- ============================================================

WITH dims AS (
    SELECT 'dim_hotels' AS dim, hotel_id AS nk, valid_from, valid_to, is_current
    FROM {{ ref('dim_hotels') }}
    UNION ALL
    SELECT 'dim_customers' AS dim, user_id AS nk, valid_from, valid_to, is_current
    FROM {{ ref('dim_customers') }}
),

current_check AS (
    SELECT dim, nk, 'current_versions != 1' AS issue
    FROM dims
    GROUP BY dim, nk
    HAVING countIf(is_current) != 1
),

ordered AS (
    SELECT
        dim, nk, valid_from, valid_to,
        leadInFrame(valid_from, 1, toDate('9999-12-31')) OVER (
            PARTITION BY dim, nk ORDER BY valid_from
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS next_from
    FROM dims
),

range_check AS (
    SELECT dim, nk, 'gap_or_overlap' AS issue
    FROM ordered
    WHERE valid_from > valid_to
       OR (next_from != toDate('9999-12-31') AND next_from != valid_to + 1)
)

SELECT * FROM current_check
UNION ALL
SELECT * FROM range_check
