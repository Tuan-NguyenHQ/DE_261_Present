-- ============================================================
--  stg_clickstream — Staging view for web/app events
-- ============================================================

{{
  config(
    materialized='view'
  )
}}

SELECT
    trim(event_id)                                AS event_id,
    trim(session_id)                              AS session_id,
    trim(user_id)                                 AS user_id,
    lower(trim(event_type))                       AS event_type,
    trim(page_url)                                AS page_url,
    trim(hotel_id)                                AS hotel_id,
    trim(search_query)                            AS search_query,
    lower(trim(device_type))                      AS device_type,
    trim(os)                                      AS os,
    event_timestamp,
    _batch_date
FROM {{ source('staging', 'clickstream') }} FINAL
WHERE event_id IS NOT NULL
  AND event_id != ''
