-- ============================================================
--  fact_bookings — Grain: 1 booking
--  - hotel_sk / customer_sk: version SCD2 hiệu lực tại _batch_date (ASOF JOIN)
--    (dùng ngày batch thay vì created_at để luôn có snapshot dim cùng ngày)
--  - Không tra được → unknown member (0) — được bắt bởi test cảnh báo
--  - amount_usd: tỷ giá từ seed fx_rates (ASOF theo ngày), tiền tệ lạ → NULL
-- ============================================================

{{
  config(
    materialized='incremental',
    unique_key='booking_id',
    incremental_strategy='delete+insert',
    engine='ReplacingMergeTree(_batch_date)',
    partition_by='toYYYYMM(_batch_date)',
    order_by='(hotel_sk, checkin_date_id, booking_id)',
    on_schema_change='append_new_columns'
  )
}}

WITH b AS (
    SELECT
        *,
        toDate(created_at)                                AS booked_date
    FROM {{ ref('stg_bookings') }}
    {% if is_incremental() %}
    WHERE _batch_date = toDate('{{ var("execution_date") }}')
    {% endif %}
)

SELECT
    b.booking_id                                          AS booking_id,
    b.hotel_id                                            AS hotel_id,
    b.user_id                                             AS user_id,
    ifNull(h.hotel_sk, toUInt64(0))                       AS hotel_sk,
    ifNull(c.customer_sk, toUInt64(0))                    AS customer_sk,
    toUInt32(formatDateTime(b.checkin_date,  '%Y%m%d'))   AS checkin_date_id,
    toUInt32(formatDateTime(b.checkout_date, '%Y%m%d'))   AS checkout_date_id,
    toUInt32(formatDateTime(b.booked_date,   '%Y%m%d'))   AS booked_date_id,
    ifNull(ch.channel_id, toUInt16(4))                    AS channel_id,
    toInt16(b.nights)                                     AS nights,
    b.total_amount                                        AS total_amount,
    fx.units_per_usd                                      AS fx_units_per_usd,
    {{ to_usd('b.total_amount', 'fx.units_per_usd') }}    AS amount_usd,
    CAST(b.currency AS LowCardinality(String))            AS currency,
    CAST(b.booking_status AS LowCardinality(String))      AS booking_status,
    CAST(b.room_type AS LowCardinality(String))           AS room_type,
    b.booking_status = 'CANCELLED'                        AS is_cancelled,
    b.booking_status = 'NO_SHOW'                          AS is_no_show,
    b._batch_date                                         AS _batch_date
FROM b
ASOF LEFT JOIN {{ ref('dim_hotels') }}    h   ON b.hotel_id = h.hotel_id AND b._batch_date >= h.valid_from
ASOF LEFT JOIN {{ ref('dim_customers') }} c   ON b.user_id  = c.user_id  AND b._batch_date >= c.valid_from
LEFT JOIN      {{ ref('dim_channels') }}  ch  ON b.booking_channel = ch.channel_name
ASOF LEFT JOIN {{ ref('fx_rates') }}      fx  ON b.currency = fx.currency AND b.booked_date >= fx.valid_from
