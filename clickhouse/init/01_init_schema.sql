-- ============================================================
--  ClickHouse Init — Create Databases & Staging Tables
-- ============================================================

CREATE DATABASE IF NOT EXISTS staging;
CREATE DATABASE IF NOT EXISTS analytics;

-- ════════════════════════════════════════════════════
--  STAGING TABLES (landing zone from Spark Silver)
-- ════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS staging.bookings (
    booking_id      String,
    hotel_id        String,
    user_id         String,
    room_type       String,
    checkin_date     Date,
    checkout_date    Date,
    total_amount     Decimal(15,2),
    currency         String,
    booking_status   String,
    booking_channel  String,
    payment_method   String,
    nights           Int16,
    created_at       DateTime DEFAULT now(),
    _ingested_at     DateTime DEFAULT now(),
    _source_file     String   DEFAULT '',
    _batch_date      Date
) ENGINE = ReplacingMergeTree(_batch_date)
PARTITION BY toYYYYMM(_batch_date)
ORDER BY (booking_id);


CREATE TABLE IF NOT EXISTS staging.hotels (
    hotel_id        String,
    hotel_name      String,
    hotel_tier      String,
    star_rating     UInt8,
    city            String,
    country         String,
    region          String,
    latitude        Float64,
    longitude       Float64,
    total_rooms     UInt32,
    facilities      String,
    partner_since   Date,
    is_active       Bool     DEFAULT true,
    _ingested_at    DateTime DEFAULT now(),
    _source_file    String   DEFAULT '',
    _batch_date     Date
) ENGINE = ReplacingMergeTree(_batch_date)
PARTITION BY toYYYYMM(_batch_date)
ORDER BY (hotel_id);


CREATE TABLE IF NOT EXISTS staging.payments (
    payment_id      String,
    booking_id      String,
    amount          Decimal(15,2),
    currency        String,
    payment_method  String,
    payment_status  String,
    gateway_code    String,
    processed_at    DateTime,
    refund_amount   Decimal(15,2)  DEFAULT 0,
    refund_at       Nullable(DateTime),
    _ingested_at    DateTime DEFAULT now(),
    _source_file    String   DEFAULT '',
    _batch_date     Date
) ENGINE = ReplacingMergeTree(_batch_date)
PARTITION BY toYYYYMM(_batch_date)
ORDER BY (payment_id);


CREATE TABLE IF NOT EXISTS staging.customers (
    user_id            String,
    email_hash         String,
    phone_hash         String,
    country            String,
    user_segment       String,
    loyalty_tier       String,
    loyalty_points     UInt32    DEFAULT 0,
    first_booking_date Date,
    total_bookings     UInt32    DEFAULT 0,
    is_active          Bool      DEFAULT true,
    registered_at      Date,
    _ingested_at       DateTime  DEFAULT now(),
    _source_file       String    DEFAULT '',
    _batch_date        Date
) ENGINE = ReplacingMergeTree(_batch_date)
PARTITION BY toYYYYMM(_batch_date)
ORDER BY (user_id);


CREATE TABLE IF NOT EXISTS staging.clickstream (
    event_id         String,
    session_id       String,
    user_id          Nullable(String),
    event_type       String,
    page_url         String,
    hotel_id         Nullable(String),
    search_query     Nullable(String),
    device_type      String,
    os               String,
    event_timestamp  DateTime,
    _ingested_at     DateTime DEFAULT now(),
    _source_file     String   DEFAULT '',
    _batch_date      Date
) ENGINE = ReplacingMergeTree(_batch_date)
PARTITION BY toYYYYMM(_batch_date)
ORDER BY (event_id);


-- ════════════════════════════════════════════════════
--  ANALYTICS DIMENSION TABLES
-- ════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS analytics.dim_dates (
    date_id         UInt32,
    full_date       Date,
    year            UInt16,
    quarter         UInt8,
    month           UInt8,
    month_name      String,
    week_of_year    UInt8,
    day_of_week     UInt8,
    day_name        String,
    is_weekend      Bool,
    is_holiday      Bool        DEFAULT false,
    season          String      DEFAULT 'regular'
) ENGINE = ReplacingMergeTree()
ORDER BY (date_id);


CREATE TABLE IF NOT EXISTS analytics.dim_hotels (
    hotel_sk        UInt64,
    hotel_id        String,
    hotel_name      String,
    hotel_tier      LowCardinality(String),
    star_rating     UInt8,
    city            LowCardinality(String),
    country         LowCardinality(String),
    region          LowCardinality(String),
    latitude        Float64,
    longitude       Float64,
    total_rooms     UInt32,
    is_active       Bool,
    valid_from      Date,
    valid_to        Date        DEFAULT '9999-12-31',
    is_current      Bool        DEFAULT true
) ENGINE = ReplacingMergeTree()
ORDER BY (hotel_sk);


CREATE TABLE IF NOT EXISTS analytics.dim_customers (
    customer_sk        UInt64,
    user_id            String,
    email_hash         String,
    country            LowCardinality(String),
    user_segment       LowCardinality(String),
    loyalty_tier       LowCardinality(String),
    first_booking_date Date,
    is_active          Bool,
    valid_from         Date,
    valid_to           Date       DEFAULT '9999-12-31',
    is_current         Bool       DEFAULT true
) ENGINE = ReplacingMergeTree()
ORDER BY (customer_sk);


CREATE TABLE IF NOT EXISTS analytics.dim_channels (
    channel_id      UInt16,
    channel_name    String,
    channel_type    LowCardinality(String)
) ENGINE = ReplacingMergeTree()
ORDER BY (channel_id);


CREATE TABLE IF NOT EXISTS analytics.dim_geography (
    geo_id          UInt32,
    city            String,
    country         String,
    country_code    String,
    region          String,
    timezone        String
) ENGINE = ReplacingMergeTree()
ORDER BY (geo_id);


-- ════════════════════════════════════════════════════
--  ANALYTICS FACT TABLES
-- ════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS analytics.fact_bookings (
    booking_id          String,
    hotel_sk            UInt64,
    customer_sk         UInt64,
    checkin_date_id     UInt32,
    checkout_date_id    UInt32,
    booked_date_id      UInt32,
    channel_id          UInt16,
    nights              Int16,
    total_amount        Decimal(15,2),
    amount_usd          Decimal(15,2)        DEFAULT 0,
    currency            LowCardinality(String),
    booking_status      LowCardinality(String),
    room_type           LowCardinality(String),
    is_cancelled        Bool,
    is_no_show          Bool,
    _batch_date         Date
) ENGINE = ReplacingMergeTree(_batch_date)
PARTITION BY toYYYYMM(_batch_date)
ORDER BY (hotel_sk, checkin_date_id, booking_id);


CREATE TABLE IF NOT EXISTS analytics.fact_payments (
    payment_id          String,
    booking_id          String,
    hotel_sk            UInt64,
    customer_sk         UInt64,
    payment_date_id     UInt32,
    amount              Decimal(15,2),
    amount_usd          Decimal(15,2)        DEFAULT 0,
    currency            LowCardinality(String),
    payment_status      LowCardinality(String),
    payment_method      LowCardinality(String),
    refund_amount       Decimal(15,2),
    is_refunded         Bool,
    _batch_date         Date
) ENGINE = ReplacingMergeTree(_batch_date)
PARTITION BY toYYYYMM(_batch_date)
ORDER BY (customer_sk, payment_date_id, payment_id);


CREATE TABLE IF NOT EXISTS analytics.fact_hotel_inventory (
    hotel_sk            UInt64,
    date_id             UInt32,
    room_type           LowCardinality(String),
    total_rooms         UInt16,
    booked_rooms        UInt16,
    available_rooms     UInt16,
    occupancy_rate      Float32,
    adr                 Decimal(10,2),
    revpar              Decimal(10,2),
    _batch_date         Date
) ENGINE = ReplacingMergeTree(_batch_date)
PARTITION BY toYYYYMM(_batch_date)
ORDER BY (hotel_sk, date_id, room_type);


CREATE TABLE IF NOT EXISTS analytics.fact_reviews (
    review_id           String,
    booking_id          String,
    hotel_sk            UInt64,
    customer_sk         UInt64,
    review_date_id      UInt32,
    overall_score       Float32,
    cleanliness_score   Float32,
    service_score       Float32,
    location_score      Float32,
    value_score         Float32,
    has_text_review     Bool,
    sentiment           LowCardinality(String),
    _batch_date         Date
) ENGINE = ReplacingMergeTree(_batch_date)
PARTITION BY toYYYYMM(_batch_date)
ORDER BY (hotel_sk, review_date_id, review_id);


CREATE TABLE IF NOT EXISTS analytics.fact_web_sessions (
    session_id          String,
    customer_sk         UInt64,
    session_date_id     UInt32,
    channel_id          UInt16,
    total_events        UInt32,
    page_views          UInt32,
    search_count        UInt16,
    hotels_viewed       UInt16,
    session_duration_sec UInt32,
    converted           Bool,
    booking_id          String   DEFAULT '',
    device_type         LowCardinality(String),
    _batch_date         Date
) ENGINE = ReplacingMergeTree(_batch_date)
PARTITION BY toYYYYMM(_batch_date)
ORDER BY (session_date_id, session_id);


-- ════════════════════════════════════════════════════
--  METADATA / AUDIT
-- ════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS analytics.pipeline_runs (
    run_id          String,
    execution_date  Date,
    step_name       String,
    status          String,        -- SUCCESS / FAILED
    rows_processed  UInt64         DEFAULT 0,
    started_at      DateTime,
    finished_at     DateTime,
    error_message   String         DEFAULT '',
    _created_at     DateTime       DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(execution_date)
ORDER BY (execution_date, run_id, step_name);


-- ════════════════════════════════════════════════════
--  SEED: dim_channels
-- ════════════════════════════════════════════════════

INSERT INTO analytics.dim_channels (channel_id, channel_name, channel_type)
SELECT 1, 'Web',         'direct' UNION ALL
SELECT 2, 'Mobile App',  'direct' UNION ALL
SELECT 3, 'Partner API', 'partner' UNION ALL
SELECT 4, 'Other',       'other';


-- ════════════════════════════════════════════════════
--  SEED: dim_geography
-- ════════════════════════════════════════════════════

INSERT INTO analytics.dim_geography (geo_id, city, country, country_code, region, timezone)
SELECT 1,  'Ho Chi Minh City', 'Vietnam',   'VN', 'South',   'Asia/Ho_Chi_Minh' UNION ALL
SELECT 2,  'Hanoi',            'Vietnam',   'VN', 'North',   'Asia/Ho_Chi_Minh' UNION ALL
SELECT 3,  'Da Nang',          'Vietnam',   'VN', 'Central', 'Asia/Ho_Chi_Minh' UNION ALL
SELECT 4,  'Nha Trang',        'Vietnam',   'VN', 'Central', 'Asia/Ho_Chi_Minh' UNION ALL
SELECT 5,  'Phu Quoc',         'Vietnam',   'VN', 'South',   'Asia/Ho_Chi_Minh' UNION ALL
SELECT 6,  'Hue',              'Vietnam',   'VN', 'Central', 'Asia/Ho_Chi_Minh' UNION ALL
SELECT 7,  'Bangkok',          'Thailand',  'TH', 'Central', 'Asia/Bangkok' UNION ALL
SELECT 8,  'Chiang Mai',       'Thailand',  'TH', 'North',   'Asia/Bangkok' UNION ALL
SELECT 9,  'Phuket',           'Thailand',  'TH', 'South',   'Asia/Bangkok' UNION ALL
SELECT 10, 'Pattaya',          'Thailand',  'TH', 'Central', 'Asia/Bangkok' UNION ALL
SELECT 11, 'Singapore',        'Singapore', 'SG', 'Central', 'Asia/Singapore' UNION ALL
SELECT 12, 'Jakarta',          'Indonesia', 'ID', 'Java',    'Asia/Jakarta' UNION ALL
SELECT 13, 'Bali',             'Indonesia', 'ID', 'Bali',    'Asia/Makassar' UNION ALL
SELECT 14, 'Yogyakarta',       'Indonesia', 'ID', 'Java',    'Asia/Jakarta' UNION ALL
SELECT 15, 'Bandung',          'Indonesia', 'ID', 'Java',    'Asia/Jakarta';
