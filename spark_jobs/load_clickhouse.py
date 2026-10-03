"""
============================================================
 TravelHub — Load Silver → ClickHouse staging
 SILVER (s3a://travelhub-lake/silver)  →  ClickHouse staging.*

 - Đọc silver Parquet của đúng batch date từ Data Lake
 - Chọn cột theo contract của bảng staging (thứ tự & tên cố định)
 - Idempotent: xoá dữ liệu cùng _batch_date trước khi ghi (mutations_sync)
 - Ghi qua JDBC (append)
 - Reconciliation: đếm lại trong ClickHouse = số dòng silver, lệch → fail

 Usage: spark-submit load_clickhouse.py YYYY-MM-DD
============================================================
"""

import sys

from pyspark.sql import functions as F

from common import (CH_JDBC_PROPS, CH_JDBC_URL, ch_client, create_spark, emit_metrics,
                    get_logger, lake_path, parse_date_arg, path_exists)

log = get_logger("load_clickhouse")

META = ["_ingested_at", "_source_file", "_batch_date"]

# Contract silver → staging (phải khớp clickhouse/init/01_init_schema.sql)
STAGING_COLUMNS = {
    "bookings": ["booking_id", "hotel_id", "user_id", "room_type", "checkin_date",
                 "checkout_date", "total_amount", "currency", "booking_status",
                 "booking_channel", "payment_method", "nights", "created_at"] + META,
    "hotels": ["hotel_id", "hotel_name", "hotel_tier", "star_rating", "city", "country",
               "region", "latitude", "longitude", "total_rooms", "facilities",
               "partner_since", "is_active"] + META,
    "payments": ["payment_id", "booking_id", "amount", "currency", "payment_method",
                 "payment_status", "gateway_code", "processed_at", "refund_amount",
                 "refund_at"] + META,
    "customers": ["user_id", "email_hash", "phone_hash", "country", "user_segment",
                  "loyalty_tier", "loyalty_points", "first_booking_date", "total_bookings",
                  "is_active", "registered_at"] + META,
    "clickstream": ["event_id", "session_id", "user_id", "event_type", "page_url", "hotel_id",
                    "search_query", "device_type", "os", "event_timestamp"] + META,
}

# Cột NOT NULL ở ClickHouse cần giá trị mặc định khi silver để NULL
NON_NULL_DEFAULTS = {
    "bookings": {"user_id": "", "room_type": "", "booking_channel": "Other",
                 "payment_method": ""},
    "hotels": {"hotel_tier": "", "city": "", "country": "", "region": ""},
    "payments": {"currency": "", "payment_method": "", "payment_status": ""},
    "customers": {"country": "", "user_segment": "", "loyalty_tier": ""},
    "clickstream": {"device_type": ""},
}


def load_entity(spark, client, entity, date_str):
    src = lake_path("silver", entity, date_str)
    if not path_exists(spark, src):
        raise FileNotFoundError(f"Silver not found: {src}")

    df = spark.read.parquet(src)
    missing = [c for c in STAGING_COLUMNS[entity] if c not in df.columns]
    if missing:
        raise ValueError(f"Silver {entity} thiếu cột theo contract: {missing}")

    df = df.fillna(NON_NULL_DEFAULTS.get(entity, {})) \
           .select(*STAGING_COLUMNS[entity]) \
           .withColumn("_batch_date", F.col("_batch_date").cast("date")) \
           .cache()
    n_silver = df.count()

    # Idempotency: xoá batch cũ cùng ngày (partition theo tháng nên không DROP PARTITION được)
    client.command(
        f"ALTER TABLE staging.{entity} DELETE WHERE _batch_date = toDate('{date_str}') "
        f"SETTINGS mutations_sync = 2"
    )

    (df.write.format("jdbc")
       .option("url", CH_JDBC_URL)
       .option("dbtable", entity)
       .option("batchsize", 10000)
       .options(**CH_JDBC_PROPS)
       .mode("append")
       .save())
    df.unpersist()

    # Reconciliation silver ↔ staging
    n_ch = client.query(
        f"SELECT count() FROM staging.{entity} WHERE _batch_date = toDate('{date_str}')"
    ).result_rows[0][0]
    if n_ch != n_silver:
        raise RuntimeError(f"Reconciliation failed {entity}: silver={n_silver} clickhouse={n_ch}")

    log.info(f"   ✅ {entity}: {n_silver} rows silver → staging.{entity} (reconciled)")
    return {"silver_rows": n_silver, "staging_rows": n_ch}


def run(date_str):
    log.info(f"🚀 LOAD SILVER → CLICKHOUSE START [{date_str}]")
    spark = create_spark("TravelHub_Load_ClickHouse")
    client = ch_client()

    results, failed = {}, []
    for entity in STAGING_COLUMNS:
        try:
            results[entity] = load_entity(spark, client, entity, date_str)
        except Exception as e:  # noqa: BLE001
            log.error(f"❌ {entity} load failed: {e}")
            results[entity] = {"error": str(e)[:300]}
            failed.append(entity)

    spark.stop()
    total = sum(r.get("staging_rows", 0) for r in results.values())
    emit_metrics("load", {"rows": total, "entities": results, "failed": failed})
    log.info(f"📊 LOAD RESULTS: {results}")
    return not failed


if __name__ == "__main__":
    ok = run(parse_date_arg())
    sys.exit(0 if ok else 1)
