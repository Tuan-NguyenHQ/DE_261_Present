-- ============================================================
--  dim_channels — Booking channel dimension (seeded)
-- ============================================================

{{
  config(
    materialized='table',
    order_by='channel_id'
  )
}}

SELECT 1 AS channel_id, 'Web' AS channel_name, 'direct' AS channel_type UNION ALL
SELECT 2, 'Mobile App', 'direct' UNION ALL
SELECT 3, 'Partner API', 'partner' UNION ALL
SELECT 4, 'Other', 'other'
