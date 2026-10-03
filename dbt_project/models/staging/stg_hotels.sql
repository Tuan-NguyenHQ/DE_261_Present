-- ============================================================
--  stg_hotels — Staging view for hotel master data
--  Trả về TOÀN BỘ lịch sử snapshot (1 dòng / hotel / _batch_date)
--  → nguồn cho dim_hotels (SCD Type 2)
-- ============================================================

{{
  config(
    materialized='view'
  )
}}

SELECT
    upper(trim(hotel_id))                         AS hotel_id,
    trim(hotel_name)                              AS hotel_name,
    lower(trim(hotel_tier))                       AS hotel_tier,
    toUInt8(star_rating)                          AS star_rating,
    trim(city)                                    AS city,
    upper(trim(country))                          AS country,
    trim(region)                                  AS region,
    latitude,
    longitude,
    total_rooms,
    -- "bar,beach,parking" → ['bar','beach','parking'] (sắp xếp để hash ổn định)
    arraySort(arrayFilter(x -> x != '', splitByChar(',', lower(facilities))))
                                                  AS facilities,
    partner_since,
    is_active,
    _batch_date
FROM {{ source('staging', 'hotels') }} FINAL
WHERE hotel_id IS NOT NULL
  AND hotel_id != ''
