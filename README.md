# 🏨 TravelHub Data Platform

> **End-to-end Data Lake → Data Warehouse pipeline** cho nền tảng OTA Đông Nam Á.

---

## 📐 Kiến Trúc

```
  CSV/XML Sources          MinIO (Data Lake)           ClickHouse (DWH)
  ───────────────          ────────────────            ────────────────
  Booking Engine ──►  ┌──────────────────┐
  Hotel PMS      ──►  │   raw/ (landing) │──► Spark ──► staging.*
  Payment GW     ──►  │                  │                  │
  CRM System     ──►  └──────────────────┘             dbt run
  Web Tracking   ──►                                       │
                                                      analytics.*
                                                      ┌─────────┐
                                                      │ dim_*    │
                                                      │ fact_*   │
                                                      │ rpt_*    │
                                                      └─────────┘
                                                           │
                                                      BI Dashboard
                                                    (Superset/Metabase)
```

---

## 🚀 Quick Start

### 1. Chuẩn bị

```bash
# Clone project
cd DE/

# (Tùy chọn) Cấu hình secrets trong .env
cp .env .env.local
```

### 2. Khởi động tất cả services

```bash
docker compose up -d
```

Các services sẽ khởi động:

| Service | URL | Mô tả |
|---------|-----|--------|
| SeaweedFS Filer / UI | http://localhost:8888 | Object Storage (Data Lake) |
| SeaweedFS S3 API | http://localhost:8333 | S3 Gateway Endpoint |
| Spark Master | http://localhost:8880 | Processing Engine UI |
| ClickHouse | http://localhost:8123 | Data Warehouse |
| Superset | http://localhost:8088 | BI Dashboard |
| Grafana | http://localhost:3000 | Monitoring |

### 3. Sinh dữ liệu mẫu

```bash
# Sinh dữ liệu cho ngày cụ thể
python scripts/generate_sample_data.py 2026-10-01
```

### 4. Chạy pipeline

```bash
# Exec vào pipeline container
docker exec -it travelhub-pipeline bash

# Chạy pipeline đầy đủ
python /opt/scripts/run_pipeline.py 2026-10-01

# Hoặc chạy từng bước:
# Step 1: Generate data
python /opt/scripts/generate_sample_data.py 2026-10-01

# Step 2: Upload to MinIO
# (tự động trong run_pipeline.py)

# Step 3: Spark load to ClickHouse
spark-submit /opt/spark-jobs/load_clickhouse.py 2026-10-01

# Step 4: dbt run
cd /opt/dbt && dbt run --vars '{"execution_date":"2026-10-01"}'

# Step 5: dbt test
cd /opt/dbt && dbt test --vars '{"execution_date":"2026-10-01"}'
```

---

## 📂 Cấu Trúc Project

```
DE/
├── docker-compose.yml           # Orchestrate all containers
├── Dockerfile.pipeline          # Pipeline runner image
├── requirements.txt             # Python dependencies
├── .env                         # Environment variables
│
├── clickhouse/
│   └── init/
│       └── 01_init_schema.sql   # DDL: staging + analytics tables
│
├── spark_jobs/
│   ├── ingest_bronze.py         # Raw → Bronze ingestion
│   ├── cleanse_silver.py        # Bronze → Silver cleansing
│   └── load_clickhouse.py       # CSV/XML → ClickHouse staging
│
├── dbt_project/
│   ├── dbt_project.yml          # dbt configuration
│   ├── profiles.yml             # ClickHouse connection
│   ├── packages.yml             # dbt-utils dependency
│   └── models/
│       ├── staging/             # Source definitions + staging views
│       │   ├── sources.yml
│       │   ├── stg_bookings.sql
│       │   ├── stg_hotels.sql
│       │   ├── stg_payments.sql
│       │   ├── stg_customers.sql
│       │   └── stg_clickstream.sql
│       ├── dimensions/          # Dimension tables
│       │   ├── dim_dates.sql
│       │   ├── dim_hotels.sql
│       │   ├── dim_customers.sql
│       │   ├── dim_channels.sql
│       │   └── schema.yml
│       ├── marts/               # Fact tables (incremental)
│       │   ├── fact_bookings.sql
│       │   ├── fact_payments.sql
│       │   ├── fact_web_sessions.sql
│       │   ├── fact_hotel_inventory.sql
│       │   └── schema.yml
│       └── reporting/           # Report tables
│           ├── rpt_daily_revenue.sql
│           ├── rpt_hotel_performance.sql
│           └── rpt_conversion_funnel.sql
│
├── scripts/
│   ├── generate_sample_data.py  # Sample data generator
│   └── run_pipeline.py          # Pipeline orchestrator
│
└── data/
    └── sample/                  # Generated sample data
```

---

## 🔄 Pipeline Flow

```
 02:00 AM (cron / manual)
     │
     ▼
 [0. GENERATE DATA]    Python generates CSV/XML sample data
     │
     ▼
 [1. UPLOAD MinIO]     Upload raw files → MinIO s3://travelhub-lake/raw/
     │
     ▼
 [2. SPARK LOAD]       Spark reads CSV/XML → ClickHouse staging tables
     │                   • bookings (CSV+XML) → staging.bookings
     │                   • hotels (XML)       → staging.hotels
     │                   • payments (CSV)     → staging.payments
     │                   • customers (CSV)    → staging.customers
     │                   • clickstream (CSV)  → staging.clickstream
     │
     ▼
 [3. DBT RUN]          dbt transforms staging → analytics
     │                   • staging views (normalize, clean)
     │                   • dim_dates, dim_hotels, dim_customers, dim_channels
     │                   • fact_bookings, fact_payments, fact_web_sessions
     │                   • fact_hotel_inventory
     │                   • rpt_daily_revenue, rpt_hotel_performance
     │                   • rpt_conversion_funnel
     │
     ▼
 [4. DBT TEST]         Data quality checks
     │                   • unique, not_null on all keys
     │                   • relationship integrity
     │
     ▼
 ~05:30 AM ✅          Pipeline complete!
```

---

## 📊 Star Schema

### Dimensions
- **dim_dates** — Date spine (3 năm, season classification)
- **dim_hotels** — Hotel master data (SCD Type 2)
- **dim_customers** — Customer data (PII masked, SCD Type 2)
- **dim_channels** — Booking channels (Web/Mobile/Partner/Other)
- **dim_geography** — City/Country/Region reference

### Facts
- **fact_bookings** — Đặt phòng (grain: 1 booking)
- **fact_payments** — Thanh toán (grain: 1 transaction)
- **fact_hotel_inventory** — Tình trạng phòng (grain: hotel × day × room)
- **fact_web_sessions** — Hành vi web (grain: 1 session)

### Reports
- **rpt_daily_revenue** — Doanh thu ngày theo country/channel/tier
- **rpt_hotel_performance** — Hiệu suất khách sạn
- **rpt_conversion_funnel** — Conversion funnel theo device

---

## 🛠️ Tech Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| Object Storage | MinIO | Latest |
| Processing | Apache Spark | 3.5 |
| Table Format | Apache Iceberg | 1.5.0 |
| Data Warehouse | ClickHouse | 24.3 |
| Transformation | dbt Core + dbt-clickhouse | 1.7.x |
| BI Dashboard | Apache Superset | 3.1.0 |
| Monitoring | Grafana | Latest |
| Language | Python | 3.x |

---

## 📝 License

Internal project for TravelHub Data Platform.
