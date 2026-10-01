-- ============================================================
--  dim_dates — Date dimension table (populated for 3 years)
-- ============================================================

{{
  config(
    materialized='table',
    order_by='date_id'
  )
}}

WITH date_range AS (
    SELECT
        toDate('2024-01-01') + number AS full_date
    FROM numbers(1096)  -- ~3 years
)

SELECT
    toUInt32(formatDateTime(full_date, '%Y%m%d'))       AS date_id,
    full_date,
    toYear(full_date)                                    AS year,
    toQuarter(full_date)                                 AS quarter,
    toMonth(full_date)                                   AS month,
    dateName('month', full_date)                         AS month_name,
    toISOWeek(full_date)                                 AS week_of_year,
    toDayOfWeek(full_date)                               AS day_of_week,
    dateName('weekday', full_date)                       AS day_name,
    toDayOfWeek(full_date) >= 6                          AS is_weekend,
    false                                                AS is_holiday,
    CASE
        WHEN toMonth(full_date) IN (6, 7, 8, 12)
            THEN 'high_season'
        WHEN toMonth(full_date) IN (1, 2, 9)
            THEN 'shoulder'
        ELSE 'low_season'
    END                                                  AS season
FROM date_range
