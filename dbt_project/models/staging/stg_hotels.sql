-- ============================================================
--  stg_hotels — Staging view for hotel master data
-- ============================================================

{{
  config(
    materialized='view'
  )
}}

SELECT
    trim(hotel_id)                                AS hotel_id,
    trim(hotel_name)                              AS hotel_name,
    lower(trim(hotel_tier))                       AS hotel_tier,
    toUInt8(star_rating)                          AS star_rating,
    trim(city)                                    AS city,
    upper(trim(country))                          AS country,
    trim(region)                                  AS region,
    latitude,
    longitude,
    total_rooms,
    is_active,
    _batch_date
FROM {{ source('staging', 'hotels') }} FINAL
WHERE hotel_id IS NOT NULL
  AND hotel_id != ''
