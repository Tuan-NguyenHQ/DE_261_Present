-- ============================================================
--  dim_hotels — Hotel dimension (SCD Type 2)
-- ============================================================

{{
  config(
    materialized='table',
    order_by='hotel_sk'
  )
}}

SELECT
    cityHash64(concat(hotel_id, toString(_batch_date)))  AS hotel_sk,
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
    is_active,
    _batch_date                                          AS valid_from,
    toDate('9999-12-31')                                 AS valid_to,
    true                                                 AS is_current
FROM {{ ref('stg_hotels') }}
