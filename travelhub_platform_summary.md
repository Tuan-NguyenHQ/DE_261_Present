# 🏨 TravelHub Data Platform — Tài Liệu Tổng Hợp

> **Lĩnh vực:** Tourism / Online Travel Agency (OTA)
> **Stack:** Apache Spark · ClickHouse · dbt Core · Apache Iceberg · Python
> **Phiên bản:** 1.0 · 2026-10-01

---

## 📑 Mục Lục

1. [Use Case](#1-use-case)
2. [Nguồn Dữ Liệu trong Data Lake](#2-nguồn-dữ-liệu-trong-data-lake)
3. [Fact Tables trong Data Warehouse](#3-fact-tables-trong-data-warehouse)
4. [Pipeline Cơ Bản](#4-pipeline-cơ-bản)
5. [Tech Stack & Cài Đặt](#5-tech-stack--cài-đặt)

---

## 1. Use Case

### 🎯 Bối Cảnh

**TravelHub** là nền tảng đặt phòng du lịch trực tuyến (OTA) phục vụ thị trường Đông Nam Á.

| Chỉ số | Giá trị |
|--------|---------|
| Booking/ngày | ~50.000 đơn |
| Khách sạn đối tác | 12.000+ cơ sở |
| Thị trường | VN, TH, SG, ID |
| Volume raw data/ngày | ~80–120 GB |
| Format file | **CSV, XML** |

### 🔴 Vấn Đề Hiện Tại

- Dữ liệu phân tán ở nhiều nguồn — không có cái nhìn tổng thể
- BI team chờ đến 10:00 AM mới có báo cáo doanh thu hôm qua
- Không phát hiện được đối tác có tỷ lệ hủy phòng cao
- Báo cáo sai do duplicate booking, không có data quality

### ✅ Mục Tiêu Pipeline

| Mục tiêu | Đo lường |
|----------|----------|
| Dữ liệu sẵn sàng trước 06:00 AM | Airflow/cron SLA |
| Không có duplicate trong DWH | dbt unique test pass |
| Data quality ≥ 98% | GX validation score |
| Báo cáo cross-domain | Join booking + hotel + payment |
| Hỗ trợ backfill theo ngày | Idempotent pipeline |

### 🗺️ Luồng Tổng Thể

```
                        TRAVELHUB DATA PLATFORM

 SOURCE SYSTEMS         DATA LAKE (MinIO/S3)          DATA WAREHOUSE
 ──────────────         ────────────────────          ──────────────
 Booking Engine ──CSV──►┌─────────────────┐
 Hotel PMS      ──XML──►│  BRONZE LAYER   │
 Payment GW     ──CSV──►│  (raw, as-is)   │──Spark──►  staging.*
 CRM System     ──CSV──►│                 │
 Web Tracking   ──CSV──►└─────────────────┘
                         ┌─────────────────┐              │
                         │  SILVER LAYER   │◄──Spark──    │
                         │  (clean, typed) │──────────► dbt run
                         └─────────────────┘              │
                         ┌─────────────────┐              ▼
                         │   GOLD LAYER    │         analytics.*
                         │  (aggregated)   │──────►  (ClickHouse)
                         └─────────────────┘
                                                          │
                                                          ▼
                                                   BI Dashboard
                                                 (Superset / Metabase)
```

---

## 2. Nguồn Dữ Liệu trong Data Lake

### 📂 Cấu Trúc Data Lake (MinIO / S3)

```
s3://travelhub-lake/
├── raw/                          ← Landing zone (file gốc, không sửa)
│   ├── bookings/
│   │   └── dt=2026-10-01/
│   │       ├── bookings_001.csv
│   │       └── bookings_002.xml
│   ├── hotels/
│   │   └── dt=2026-10-01/
│   │       └── hotels_full.xml
│   ├── payments/
│   │   └── dt=2026-10-01/
│   │       └── payments.csv
│   ├── customers/
│   │   └── dt=2026-10-01/
│   │       └── crm_export.csv
│   └── clickstream/
│       └── dt=2026-10-01/
│           └── web_events.csv
│
├── bronze/                       ← Raw data + metadata (Iceberg tables)
├── silver/                       ← Cleaned, typed (Iceberg tables)
├── gold/                         ← Aggregated (Iceberg tables)
└── rejected/                     ← File lỗi, không parse được
```

---

### 📋 Nguồn 1: Booking (CSV + XML)

**Mô tả:** Dữ liệu đặt phòng từ Booking Engine và các partner API.

**File:** `raw/bookings/dt=YYYY-MM-DD/*.csv` và `*.xml`

```
booking_id      : String    — Mã booking duy nhất         [NOT NULL]
hotel_id        : String    — Mã khách sạn                [NOT NULL]
user_id         : String    — Mã khách hàng
room_type       : String    — Loại phòng (Standard/Deluxe/Suite)
checkin_date    : String    — Ngày nhận phòng (yyyy-MM-dd)
checkout_date   : String    — Ngày trả phòng (yyyy-MM-dd)
total_amount    : String    — Tổng tiền (raw, cần clean)
currency        : String    — Tiền tệ (VND/USD/THB...)
booking_status  : String    — CONFIRMED/CANCELLED/PENDING/NO_SHOW
source_channel  : String    — Kênh đặt (web/app_ios/partner_xyz)
payment_method  : String    — CREDIT_CARD/BANK_TRANSFER/EWALLET
created_at      : String    — Thời điểm tạo booking
```

**Vấn đề thường gặp:**
- `total_amount` chứa ký tự tiền tệ: `"1,500,000 VND"` → cần strip
- `booking_status` không nhất quán: `"confirm"`, `"CONFIRMED"`, `"1"` → cần map
- File XML có thể lồng nhau: `<booking><rooms><room>...`
- Duplicate `booking_id` khi nhận từ nhiều partner

---

### 📋 Nguồn 2: Hotel (XML)

**Mô tả:** Thông tin master data khách sạn, đồng bộ từ Property Management System (PMS).

**File:** `raw/hotels/dt=YYYY-MM-DD/hotels_full.xml` *(full snapshot hàng ngày)*

```
hotel_id        : String    — Mã khách sạn                [NOT NULL]
hotel_name      : String    — Tên khách sạn               [NOT NULL]
hotel_tier      : String    — budget/standard/luxury
star_rating     : String    — 1–5
city            : String    — Thành phố
country         : String    — Quốc gia (ISO 3166)
region          : String    — Vùng địa lý (North/Central/South)
latitude        : String    — Tọa độ
longitude       : String    — Tọa độ
total_rooms     : String    — Tổng số phòng
facilities      : String    — JSON array: ["pool","gym","spa"]
partner_since   : String    — Ngày ký hợp đồng
is_active       : String    — true/false
```

**Ghi chú:** Full snapshot → cần SCD Type 2 để tracking lịch sử thay đổi.

---

### 📋 Nguồn 3: Payment (CSV)

**Mô tả:** Giao dịch thanh toán từ Payment Gateway.

**File:** `raw/payments/dt=YYYY-MM-DD/payments.csv`

```
payment_id      : String    — Mã giao dịch               [NOT NULL]
booking_id      : String    — FK → booking                [NOT NULL]
amount          : String    — Số tiền giao dịch
currency        : String    — Tiền tệ
payment_method  : String    — Phương thức thanh toán
payment_status  : String    — SUCCESS/FAILED/PENDING/REFUNDED
gateway_code    : String    — Mã phản hồi từ gateway
processed_at    : String    — Thời điểm xử lý
refund_amount   : String    — Số tiền hoàn (nếu có)
refund_at       : String    — Thời điểm hoàn tiền
```

---

### 📋 Nguồn 4: Customer / CRM (CSV)

**Mô tả:** Thông tin khách hàng từ CRM System, export hàng ngày.

**File:** `raw/customers/dt=YYYY-MM-DD/crm_export.csv`

```
user_id           : String  — Mã khách hàng              [NOT NULL]
full_name         : String  — Họ tên (cần PII masking)
email             : String  — Email (cần PII masking)
phone             : String  — SĐT (cần PII masking)
country           : String  — Quốc gia cư trú
user_segment      : String  — budget/standard/premium
loyalty_tier      : String  — Bronze/Silver/Gold/Platinum
loyalty_points    : String  — Điểm tích lũy
first_booking_date: String  — Ngày đặt phòng đầu tiên
total_bookings    : String  — Tổng số booking lịch sử
is_active         : String  — true/false
registered_at     : String  — Ngày đăng ký
```

**Lưu ý bảo mật:** `full_name`, `email`, `phone` phải được **hash/mask** trước khi vào Silver.

---

### 📋 Nguồn 5: Clickstream / Web Events (CSV)

**Mô tả:** Hành vi người dùng trên web/app từ tracking system.

**File:** `raw/clickstream/dt=YYYY-MM-DD/web_events.csv`

```
event_id        : String    — Mã sự kiện                 [NOT NULL]
session_id      : String    — Phiên truy cập
user_id         : String    — Mã user (nullable — chưa login)
event_type      : String    — page_view/search/click/booking_start/booking_complete
page_url        : String    — URL trang
hotel_id        : String    — Khách sạn đang xem (nếu có)
search_query    : String    — Từ khóa tìm kiếm
device_type     : String    — desktop/mobile/tablet
os              : String    — iOS/Android/Windows...
event_timestamp : String    — Thời điểm sự kiện
```

---

### 📊 Tổng Hợp Nguồn Dữ Liệu

| # | Nguồn | Format | Volume/ngày | Frequency | Loại load |
|---|-------|--------|-------------|-----------|-----------|
| 1 | Booking | CSV + XML | ~500 MB | Daily | Incremental |
| 2 | Hotel | XML | ~50 MB | Daily | Full snapshot (SCD2) |
| 3 | Payment | CSV | ~200 MB | Daily | Incremental |
| 4 | Customer/CRM | CSV | ~100 MB | Daily | Full snapshot (SCD2) |
| 5 | Clickstream | CSV | ~2 GB | Daily | Incremental |
| | **Tổng** | | **~3 GB/ngày** | | |

---

## 3. Fact Tables trong Data Warehouse

### 🏛️ Star Schema Tổng Thể

```
                    ┌──────────────┐
                    │  dim_dates   │
                    └──────┬───────┘
                           │
         ┌─────────────────┼─────────────────┐
         │                 │                 │
┌────────▼──────┐  ┌───────▼────────┐  ┌────▼────────────┐
│  dim_hotels   │  │ dim_customers  │  │  dim_channels   │
└────────┬──────┘  └───────┬────────┘  └────┬────────────┘
         │                 │                 │
         └────────┬────────┘        ┌────────┘
                  │                 │
          ┌───────▼─────────────────▼───────────┐
          │           FACT TABLES                │
          │  ┌──────────────────────────────┐   │
          │  │  fact_bookings          [F1] │   │
          │  │  fact_payments          [F2] │   │
          │  │  fact_hotel_inventory   [F3] │   │
          │  │  fact_reviews           [F4] │   │
          │  │  fact_web_sessions      [F5] │   │
          │  └──────────────────────────────┘   │
          └─────────────────────────────────────┘
                           │
                    ┌──────▼───────┐
                    │ dim_geography│
                    └──────────────┘
```

---

### 📐 Shared Dimensions (Dùng Chung Cho Tất Cả Fact Tables)

**`dim_dates`** — Bảng ngày tháng

```sql
CREATE TABLE analytics.dim_dates (
    date_id         UInt32,         -- YYYYMMDD format
    full_date       Date,
    year            UInt16,
    quarter         UInt8,          -- 1–4
    month           UInt8,          -- 1–12
    month_name      String,
    week_of_year    UInt8,
    day_of_week     UInt8,          -- 1=Mon, 7=Sun
    day_name        String,
    is_weekend      Bool,
    is_holiday      Bool,           -- Ngày lễ VN/TH/SG/ID
    season          String          -- high_season/low_season/shoulder
) ENGINE = ReplacingMergeTree()
ORDER BY (date_id);
```

**`dim_hotels`** — Thông tin khách sạn (SCD Type 2)

```sql
CREATE TABLE analytics.dim_hotels (
    hotel_sk        UInt64,         -- Surrogate key
    hotel_id        String,         -- Natural key
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
    valid_to        Date,
    is_current      Bool
) ENGINE = ReplacingMergeTree()
ORDER BY (hotel_sk);
```

**`dim_customers`** — Thông tin khách hàng (PII đã mask)

```sql
CREATE TABLE analytics.dim_customers (
    customer_sk     UInt64,
    user_id         String,
    email_hash      String,         -- SHA-256
    country         LowCardinality(String),
    user_segment    LowCardinality(String),
    loyalty_tier    LowCardinality(String),
    first_booking_date Date,
    is_active       Bool,
    valid_from      Date,
    valid_to        Date,
    is_current      Bool
) ENGINE = ReplacingMergeTree()
ORDER BY (customer_sk);
```

**`dim_channels`** + **`dim_geography`**

```sql
CREATE TABLE analytics.dim_channels (
    channel_id      UInt16,
    channel_name    String,         -- Web / Mobile App / Partner API / Other
    channel_type    LowCardinality(String)
) ENGINE = ReplacingMergeTree() ORDER BY (channel_id);

CREATE TABLE analytics.dim_geography (
    geo_id          UInt32,
    city            String,
    country         String,
    country_code    String,
    region          String,
    timezone        String
) ENGINE = ReplacingMergeTree() ORDER BY (geo_id);
```

---

### 📊 F1: `fact_bookings` — Giao Dịch Đặt Phòng

> **Grain:** 1 row = 1 booking

```sql
CREATE TABLE analytics.fact_bookings (
    booking_id          String,
    hotel_sk            UInt64,
    customer_sk         UInt64,
    checkin_date_id     UInt32,
    checkout_date_id    UInt32,
    booked_date_id      UInt32,
    channel_id          UInt16,
    nights              Int16,
    total_amount        Decimal(15,2),
    amount_usd          Decimal(15,2),
    currency            LowCardinality(String),
    booking_status      LowCardinality(String),
    room_type           LowCardinality(String),
    is_cancelled        Bool,
    is_no_show          Bool,
    _batch_date         Date
) ENGINE = ReplacingMergeTree(_batch_date)
PARTITION BY toYYYYMM(checkin_date_id)
ORDER BY (hotel_sk, checkin_date_id, booking_id);
```

---

### 📊 F2: `fact_payments` — Giao Dịch Thanh Toán

> **Grain:** 1 row = 1 payment transaction

```sql
CREATE TABLE analytics.fact_payments (
    payment_id          String,
    booking_id          String,
    hotel_sk            UInt64,
    customer_sk         UInt64,
    payment_date_id     UInt32,
    amount              Decimal(15,2),
    amount_usd          Decimal(15,2),
    currency            LowCardinality(String),
    payment_status      LowCardinality(String),
    payment_method      LowCardinality(String),
    refund_amount       Decimal(15,2),
    is_refunded         Bool,
    _batch_date         Date
) ENGINE = ReplacingMergeTree(_batch_date)
PARTITION BY toYYYYMM(payment_date_id)
ORDER BY (customer_sk, payment_date_id, payment_id);
```

---

### 📊 F3: `fact_hotel_inventory` — Tình Trạng Phòng

> **Grain:** 1 row = hotel × ngày × loại phòng

```sql
CREATE TABLE analytics.fact_hotel_inventory (
    hotel_sk            UInt64,
    date_id             UInt32,
    room_type           LowCardinality(String),
    total_rooms         UInt16,
    booked_rooms        UInt16,
    available_rooms     UInt16,
    occupancy_rate      Float32,    -- %
    adr                 Decimal(10,2),  -- Average Daily Rate
    revpar              Decimal(10,2),  -- Revenue Per Available Room
    _batch_date         Date
) ENGINE = ReplacingMergeTree(_batch_date)
PARTITION BY toYYYYMM(date_id)
ORDER BY (hotel_sk, date_id, room_type);
```

---

### 📊 F4: `fact_reviews` — Đánh Giá Khách Sạn

> **Grain:** 1 row = 1 review

```sql
CREATE TABLE analytics.fact_reviews (
    review_id           String,
    booking_id          String,
    hotel_sk            UInt64,
    customer_sk         UInt64,
    review_date_id      UInt32,
    overall_score       Float32,    -- 1.0–10.0
    cleanliness_score   Float32,
    service_score       Float32,
    location_score      Float32,
    value_score         Float32,
    has_text_review     Bool,
    sentiment           LowCardinality(String),  -- positive/neutral/negative
    _batch_date         Date
) ENGINE = ReplacingMergeTree(_batch_date)
PARTITION BY toYYYYMM(review_date_id)
ORDER BY (hotel_sk, review_date_id, review_id);
```

---

### 📊 F5: `fact_web_sessions` — Hành Vi Người Dùng

> **Grain:** 1 row = 1 session (tổng hợp từ clickstream)

```sql
CREATE TABLE analytics.fact_web_sessions (
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
    booking_id          String,
    device_type         LowCardinality(String),
    _batch_date         Date
) ENGINE = ReplacingMergeTree(_batch_date)
PARTITION BY toYYYYMM(session_date_id)
ORDER BY (session_date_id, session_id);
```

---

### 📑 Tổng Hợp Fact Tables

| Fact Table | Grain | Source | Câu hỏi phục vụ |
|-----------|-------|--------|-----------------|
| `fact_bookings` | 1 booking | Booking CSV/XML | Doanh thu, tỷ lệ hủy |
| `fact_payments` | 1 payment | Payment CSV | Thu chi thực tế, hoàn tiền |
| `fact_hotel_inventory` | Hotel × ngày × phòng | PMS XML | RevPAR, ADR, Occupancy |
| `fact_reviews` | 1 review | Review CSV | Chất lượng dịch vụ |
| `fact_web_sessions` | 1 session | Clickstream CSV | Conversion rate, funnel |

---

## 4. Pipeline Cơ Bản

### 🔄 Luồng Thực Thi

```
02:00 AM (cron)
    │
    ▼
[1. INGEST BRONZE]    Spark đọc CSV/XML → Iceberg Bronze (5 nguồn song song)
    ↓ fail → rejected/ + alert
    ▼
[2. VALIDATE BRONZE]  Null check, row count, format check
    ↓ fail → dừng pipeline + alert Slack
    ▼
[3. CLEANSE SILVER]   Spark: cast type, dedup, normalize, PII hash
    ▼
[4. LOAD STAGING]     Spark JDBC → ClickHouse staging.*
    ▼
[5. DBT GOLD]         dbt run: dim + fact + report tables
    ▼
[6. DBT TEST]         unique, not_null, relationships, custom rules
    ▼
[7. AUDIT LOG]        Ghi meta.pipeline_runs
    ▼
~05:30 AM ✅  Slack: SUCCESS + metrics
```

---

### ⚙️ Bước 1–2: Spark Ingest Bronze

```python
# spark_jobs/ingest_bronze.py
from pyspark.sql import SparkSession, functions as F

def create_spark():
    return SparkSession.builder \
        .appName("TravelHub_Bronze") \
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.lake.type",      "hadoop") \
        .config("spark.sql.catalog.lake.warehouse",
                "s3a://travelhub-lake/iceberg/") \
        .config("spark.hadoop.fs.s3a.endpoint",     "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()

def ingest_csv(spark, source: str, date: str):
    df = spark.read \
        .option("header", "true") \
        .option("encoding", "UTF-8") \
        .option("mode", "PERMISSIVE") \
        .option("columnNameOfCorruptRecord", "_corrupt_record") \
        .csv(f"s3a://travelhub-lake/raw/{source}/dt={date}/*.csv") \
        .withColumn("_ingested_at", F.current_timestamp()) \
        .withColumn("_source_file", F.input_file_name()) \
        .withColumn("_batch_date",  F.lit(date).cast("date"))

    # Tách hợp lệ / lỗi
    df_ok  = df.filter(F.col("_corrupt_record").isNull())
    df_err = df.filter(F.col("_corrupt_record").isNotNull())

    if df_err.count() > 0:
        df_err.write.mode("append") \
            .json(f"s3a://travelhub-lake/rejected/{source}/dt={date}/")

    df_ok.writeTo(f"lake.bronze.{source}") \
         .option("merge-schema", "true") \
         .overwritePartitions()
    return df_ok.count()

def ingest_xml(spark, source: str, row_tag: str, date: str):
    df = spark.read \
        .format("com.databricks.spark.xml") \
        .option("rowTag", row_tag) \
        .option("encoding", "UTF-8") \
        .load(f"s3a://travelhub-lake/raw/{source}/dt={date}/*.xml") \
        .withColumn("_ingested_at", F.current_timestamp()) \
        .withColumn("_source_file", F.input_file_name()) \
        .withColumn("_batch_date",  F.lit(date).cast("date"))

    df.writeTo(f"lake.bronze.{source}") \
      .option("merge-schema", "true") \
      .overwritePartitions()
    return df.count()
```

---

### ⚙️ Bước 3: Spark Cleanse Silver

```python
# spark_jobs/cleanse_silver.py
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
import hashlib

def cleanse_bookings(spark, date: str):
    df = spark.table("lake.bronze.bookings") \
              .filter(F.col("_batch_date") == date)

    df.withColumn("booking_id",    F.upper(F.trim("booking_id"))) \
      .withColumn("hotel_id",      F.upper(F.trim("hotel_id"))) \
      .withColumn("checkin_date",  F.to_date("checkin_date", "yyyy-MM-dd")) \
      .withColumn("checkout_date", F.to_date("checkout_date", "yyyy-MM-dd")) \
      .withColumn("total_amount",
          F.regexp_replace("total_amount", "[^0-9.]", "").cast(DecimalType(15,2))) \
      .withColumn("currency",      F.upper(F.trim("currency"))) \
      .withColumn("booking_status",
          F.when(F.upper("booking_status").isin(["CONFIRMED","CONFIRM"]),  "CONFIRMED")
           .when(F.upper("booking_status").isin(["CANCELLED","CANCEL"]),   "CANCELLED")
           .when(F.upper("booking_status") == "PENDING",                   "PENDING")
           .otherwise("UNKNOWN")) \
      .withColumn("booking_channel",
          F.when(F.lower("source_channel").isin(["app_ios","app_android"]), "Mobile App")
           .when(F.lower("source_channel").isin(["web","website"]),         "Web")
           .when(F.lower("source_channel").startswith("partner"),           "Partner API")
           .otherwise("Other")) \
      .withColumn("nights",
          F.datediff("checkout_date", "checkin_date")) \
      .filter(F.col("booking_id").isNotNull()) \
      .filter(F.col("nights") > 0) \
      .dropDuplicates(["booking_id"]) \
      .writeTo("lake.silver.bookings").overwritePartitions()

def cleanse_customers(spark, date: str):
    """Mask PII trước khi vào Silver."""
    sha256 = F.udf(lambda v: hashlib.sha256(v.encode()).hexdigest()
                   if v else None, "string")

    spark.table("lake.bronze.customers") \
         .filter(F.col("_batch_date") == date) \
         .withColumn("email_hash", sha256(F.col("email"))) \
         .withColumn("phone_hash", sha256(F.col("phone"))) \
         .drop("email", "phone", "full_name") \
         .dropDuplicates(["user_id"]) \
         .writeTo("lake.silver.customers").overwritePartitions()
```

---

### ⚙️ Bước 4: Load Spark → ClickHouse

```python
# spark_jobs/load_clickhouse.py
import os, clickhouse_driver

CH_URL   = "jdbc:clickhouse://clickhouse:8123/staging"
CH_PROPS = {
    "driver":   "com.clickhouse.jdbc.ClickHouseDriver",
    "user":     "travelhub",
    "password": os.environ["CLICKHOUSE_PASSWORD"],
}

def load(spark, silver_table: str, ch_table: str, date: str):
    # Xóa partition cũ (idempotent)
    client = clickhouse_driver.Client("clickhouse", user="travelhub",
                                      password=os.environ["CLICKHOUSE_PASSWORD"])
    client.execute(
        f"ALTER TABLE staging.{ch_table} DELETE WHERE _batch_date = '{date}'"
    )
    # Insert mới
    spark.table(f"lake.silver.{silver_table}") \
         .filter(F.col("_batch_date") == date) \
         .write.format("jdbc") \
         .option("url",     CH_URL) \
         .option("dbtable", f"staging.{ch_table}") \
         .options(**CH_PROPS) \
         .mode("append").save()
```

---

### ⚙️ Bước 5–6: dbt Gold

**`dbt/models/marts/fact_bookings.sql`:**

```sql
{{
  config(materialized='incremental', unique_key='booking_id',
         on_schema_change='sync_all_columns')
}}

SELECT
    b.booking_id,
    h.hotel_sk,
    c.customer_sk,
    CAST(REPLACE(CAST(b.checkin_date  AS String), '-', '') AS UInt32) AS checkin_date_id,
    CAST(REPLACE(CAST(b.checkout_date AS String), '-', '') AS UInt32) AS checkout_date_id,
    ch.channel_id,
    b.nights,
    b.total_amount,
    b.currency,
    b.booking_status,
    b.room_type,
    b.booking_status = 'CANCELLED' AS is_cancelled,
    b.booking_status = 'NO_SHOW'   AS is_no_show,
    b._batch_date
FROM {{ source('staging','bookings') }} b
LEFT JOIN {{ ref('dim_hotels') }}    h  ON b.hotel_id        = h.hotel_id    AND h.is_current
LEFT JOIN {{ ref('dim_customers') }} c  ON b.user_id         = c.user_id     AND c.is_current
LEFT JOIN {{ ref('dim_channels') }}  ch ON b.booking_channel = ch.channel_name
{% if is_incremental() %}
WHERE b._batch_date = '{{ var("execution_date") }}'
{% endif %}
```

**`dbt/models/reporting/rpt_daily_revenue.sql`:**

```sql
{{ config(materialized='table') }}

SELECT
    _batch_date                                         AS report_date,
    h.country, h.region, h.hotel_tier,
    ch.channel_name,
    COUNT(*)                                            AS total_bookings,
    COUNTIf(is_cancelled)                               AS cancelled_bookings,
    SUMIf(total_amount, booking_status = 'CONFIRMED')   AS confirmed_revenue,
    AVG(nights)                                         AS avg_nights,
    ROUND(100.0 * COUNTIf(is_cancelled) / NULLIF(COUNT(*),0), 2)
                                                        AS cancellation_rate_pct
FROM {{ ref('fact_bookings') }} f
LEFT JOIN {{ ref('dim_hotels') }}   h  ON f.hotel_sk   = h.hotel_sk
LEFT JOIN {{ ref('dim_channels') }} ch ON f.channel_id = ch.channel_id
WHERE _batch_date = '{{ var("execution_date") }}'
GROUP BY 1, 2, 3, 4, 5
```

**`dbt/models/marts/schema.yml` — Tests:**

```yaml
models:
  - name: fact_bookings
    columns:
      - name: booking_id
        tests: [unique, not_null]
      - name: nights
        tests:
          - dbt_utils.expression_is_true:
              expression: ">= 1"
      - name: total_amount
        tests:
          - dbt_utils.expression_is_true:
              expression: ">= 0"
```

---

### ⚙️ Script Điều Phối

```python
# run_pipeline.py
import subprocess, sys, logging
from datetime import date as dt

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

PACKAGES = (
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
    "com.databricks:spark-xml_2.12:0.18.0,"
    "com.clickhouse:clickhouse-jdbc:0.6.0"
)

def spark(script, *args):
    subprocess.run(
        ["spark-submit", "--packages", PACKAGES,
         f"spark_jobs/{script}.py", *args],
        check=True)

def dbt(command, date):
    subprocess.run(
        ["dbt", command, "--vars", f'{{"execution_date":"{date}"}}'],
        cwd="dbt/", check=True)

def run(date):
    log.info(f"🚀 Pipeline START [{date}]")

    # 1. Bronze
    spark("ingest_bronze", "--date", date)

    # 2. Silver
    spark("cleanse_silver", "--date", date)

    # 3. Load ClickHouse staging
    spark("load_clickhouse", "--date", date)

    # 4–5. dbt Gold + Test
    dbt("run",  date)
    dbt("test", date)

    log.info(f"🎉 Pipeline HOÀN THÀNH [{date}]")

if __name__ == "__main__":
    run(sys.argv[1] if len(sys.argv) > 1 else str(dt.today()))
```

```bash
# Chạy thủ công
python run_pipeline.py 2026-10-01

# Cron: 2AM mỗi ngày (Linux)
echo "0 2 * * * cd /app && python run_pipeline.py >> logs/cron.log 2>&1" | crontab -

# Windows Task Scheduler
# Program: python  Arguments: C:\app\run_pipeline.py  Trigger: Daily 02:00
```

---

## 5. Tech Stack & Cài Đặt

### 📦 Stack (100% Free & OSS)

| Layer | Tool | Ghi chú |
|-------|------|---------|
| Object Storage | **MinIO** | S3-compatible, self-hosted |
| Table Format | **Apache Iceberg** | ACID, time travel, schema evolution |
| Processing | **Apache Spark 3.5** | Bronze & Silver transformation |
| Transformation | **dbt Core + dbt-clickhouse** | Gold layer, tests |
| Data Warehouse | **ClickHouse 24.3** | OLAP engine, cực nhanh aggregation |
| Data Quality | **Great Expectations OSS** | Validation Bronze/Silver |
| BI Dashboard | **Apache Superset** | Kết nối trực tiếp ClickHouse |
| Monitoring | **Grafana + Prometheus** | Pipeline metrics |
| Scheduling | **cron / Airflow OSS** | Bắt đầu với cron |

### 🐳 Docker Compose

```yaml
# docker-compose.yml
services:
  minio:
    image: minio/minio:latest
    command: server /data --console-address ":9001"
    ports: ["9000:9000", "9001:9001"]
    environment:
      MINIO_ROOT_USER:     travelhub
      MINIO_ROOT_PASSWORD: ${MINIO_SECRET}
    volumes: [minio_data:/data]

  clickhouse:
    image: clickhouse/clickhouse-server:24.3
    ports: ["8123:8123"]
    volumes: [clickhouse_data:/var/lib/clickhouse]
    environment:
      CLICKHOUSE_DB:       analytics
      CLICKHOUSE_USER:     travelhub
      CLICKHOUSE_PASSWORD: ${CLICKHOUSE_PASSWORD}

  spark-master:
    image: bitnami/spark:3.5
    environment: [SPARK_MODE=master]
    ports: ["7077:7077", "8888:8080"]

  spark-worker:
    image: bitnami/spark:3.5
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
      - SPARK_WORKER_MEMORY=4G
    depends_on: [spark-master]

  superset:
    image: apache/superset:3.1.0
    ports: ["8088:8088"]
    environment:
      SUPERSET_SECRET_KEY: ${SUPERSET_SECRET}

  grafana:
    image: grafana/grafana:latest
    ports: ["3000:3000"]
    environment:
      GF_SECURITY_ADMIN_PASSWORD: ${GRAFANA_PASSWORD}

volumes:
  minio_data:
  clickhouse_data:
```

```bash
# Cài đặt dependencies
pip install pyspark==3.5.0 dbt-core dbt-clickhouse \
            great-expectations pandas lxml clickhouse-driver

# Khởi động platform
docker compose up -d

# Chạy pipeline ngày đầu tiên
python run_pipeline.py 2026-10-01
```

### 🔗 Truy Cập Dịch Vụ

| Service | URL | Credential |
|---------|-----|-----------|
| MinIO Console | http://localhost:9001 | travelhub / \${MINIO_SECRET} |
| Spark Master UI | http://localhost:8888 | — |
| ClickHouse HTTP | http://localhost:8123 | travelhub / \${CLICKHOUSE_PASSWORD} |
| Superset | http://localhost:8088 | admin / admin |
| Grafana | http://localhost:3000 | admin / \${GRAFANA_PASSWORD} |

---

## 📈 Lộ Trình Phát Triển

```
Giai đoạn 1 — MVP (Tuần 1–4):
  ✅ fact_bookings + dim_hotels + dim_dates
  ✅ rpt_daily_revenue
  ✅ Cron + python script

Giai đoạn 2 — Mở Rộng DWH (Tháng 2–3):
  ➕ fact_payments + fact_reviews
  ➕ dim_customers (PII masked)
  ➕ Thêm Airflow thay cron

Giai đoạn 3 — Data Warehouse Đầy Đủ (Tháng 4–6):
  ➕ fact_hotel_inventory + fact_web_sessions
  ➕ dim_geography
  ➕ Cross-domain reports
  ➕ OpenMetadata (data catalog & lineage)

Giai đoạn 4 — Realtime (Tháng 7+):
  ➕ Kafka + Spark Streaming
  ➕ ClickHouse ReplicatedMergeTree (HA)
```

---

*TravelHub Data Platform — Tài Liệu Tổng Hợp v1.0 — 2026-10-01*
