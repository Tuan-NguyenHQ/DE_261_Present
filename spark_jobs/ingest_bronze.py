"""
============================================================
 TravelHub — Spark Ingest Bronze
 RAW (s3a://travelhub-lake/raw)  →  BRONZE (Parquet trên lake)

 - Đọc CSV/XML từ landing zone, giữ nguyên toàn bộ cột dạng STRING
   (schema-on-read, không mất thông tin gốc)
 - Gắn metadata: _ingested_at, _source_file, _source_format, _batch_date, _run_id
 - Tách bản ghi malformed → rejected/<source>/dt=.../ (JSON)
 - Kiểm tra schema contract (cột bắt buộc)
 - Ghi s3a://travelhub-lake/bronze/<source>/dt=YYYY-MM-DD/ (overwrite → idempotent)

 Usage: spark-submit ingest_bronze.py YYYY-MM-DD
============================================================
"""

import os
import sys

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from common import create_spark, emit_metrics, get_logger, lake_path, parse_date_arg, path_exists

log = get_logger("ingest_bronze")

CORRUPT_COL = "_corrupt_record"

# source → danh sách (format, rowTag, optional)
SOURCES = {
    "bookings":    [("csv", None, True), ("xml", "booking", True)],   # cần ít nhất 1 format
    "hotels":      [("xml", "hotel", False)],
    "payments":    [("csv", None, False)],
    "customers":   [("csv", None, False)],
    "clickstream": [("csv", None, False)],
}

# Schema contract: cột bắt buộc phải có ở raw
REQUIRED_COLUMNS = {
    "bookings":    ["booking_id", "hotel_id", "user_id", "room_type", "checkin_date",
                    "checkout_date", "total_amount", "currency", "booking_status",
                    "source_channel", "payment_method", "created_at"],
    "hotels":      ["hotel_id", "hotel_name", "hotel_tier", "star_rating", "city", "country",
                    "region", "latitude", "longitude", "total_rooms", "facilities",
                    "partner_since", "is_active"],
    "payments":    ["payment_id", "booking_id", "amount", "currency", "payment_method",
                    "payment_status", "gateway_code", "processed_at", "refund_amount", "refund_at"],
    "customers":   ["user_id", "full_name", "email", "phone", "country", "user_segment",
                    "loyalty_tier", "loyalty_points", "first_booking_date", "total_bookings",
                    "is_active", "registered_at"],
    "clickstream": ["event_id", "session_id", "user_id", "event_type", "page_url", "hotel_id",
                    "search_query", "device_type", "os", "event_timestamp"],
}


# ──────────────── READERS ────────────────

def read_csv_raw(spark, path):
    """Đọc CSV toàn bộ là string + cột _corrupt_record để bắt dòng malformed."""
    header_cols = spark.read.option("header", "true").csv(path).columns
    schema = StructType(
        [StructField(c, StringType(), True) for c in header_cols]
        + [StructField(CORRUPT_COL, StringType(), True)]
    )
    return (
        spark.read
        .option("header", "true")
        .option("encoding", "UTF-8")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_COL)
        .schema(schema)
        .csv(path)
    )


def read_xml_raw(spark, path, row_tag):
    """Đọc XML, inferSchema=false → mọi leaf là string."""
    return (
        spark.read.format("com.databricks.spark.xml")
        .option("rowTag", row_tag)
        .option("encoding", "UTF-8")
        .option("inferSchema", "false")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_COL)
        .load(path)
    )


# ──────────────── INGEST 1 SOURCE ────────────────

def ingest_source(spark, source, date_str, run_id):
    raw_dir = lake_path("raw", source, date_str)
    frames = []

    for fmt, row_tag, optional in SOURCES[source]:
        path = f"{raw_dir}/*.{fmt}"
        if not path_exists(spark, path):
            if optional:
                log.info(f"   {source}: không có file *.{fmt} (optional) — bỏ qua")
                continue
            raise FileNotFoundError(f"Missing raw input: {path}")

        log.info(f"📂 {source}: đọc {path}")
        df = read_csv_raw(spark, path) if fmt == "csv" else read_xml_raw(spark, path, row_tag)
        if CORRUPT_COL not in df.columns:
            df = df.withColumn(CORRUPT_COL, F.lit(None).cast(StringType()))
        df = df.withColumn("_source_format", F.lit(fmt))
        frames.append(df)

    if not frames:
        raise FileNotFoundError(f"No raw files for {source} at {raw_dir}")

    df = frames[0]
    for other in frames[1:]:
        df = df.unionByName(other, allowMissingColumns=True)

    # Schema contract check
    missing = [c for c in REQUIRED_COLUMNS[source] if c not in df.columns]
    if missing:
        raise ValueError(f"Schema contract violated for {source}: missing columns {missing}")
    extra = [c for c in df.columns
             if c not in REQUIRED_COLUMNS[source] and c not in (CORRUPT_COL, "_source_format")]
    if extra:
        log.warning(f"   ⚠️  {source}: schema drift, cột mới {extra} (vẫn giữ ở bronze)")

    # Ép mọi cột dữ liệu về string (XML lồng nhau / kiểu lạ) để bronze đồng nhất
    data_cols = [c for c in df.columns if c not in (CORRUPT_COL, "_source_format")]
    df = df.select(
        *[F.col(c).cast(StringType()).alias(c) for c in data_cols],
        CORRUPT_COL, "_source_format",
    ).withColumn("_source_file", F.input_file_name()) \
     .withColumn("_ingested_at", F.current_timestamp()) \
     .withColumn("_batch_date", F.lit(date_str).cast("date")) \
     .withColumn("_run_id", F.lit(run_id))

    df = df.cache()
    df_ok = df.filter(F.col(CORRUPT_COL).isNull()).drop(CORRUPT_COL)
    df_bad = df.filter(F.col(CORRUPT_COL).isNotNull()) \
               .select(CORRUPT_COL, "_source_file", "_source_format", "_ingested_at", "_batch_date")

    n_ok, n_bad = df_ok.count(), df_bad.count()

    bronze_out = lake_path("bronze", source, date_str)
    rejected_out = lake_path("rejected", source, date_str)

    df_ok.write.mode("overwrite").parquet(bronze_out)
    # Luôn ghi (kể cả rỗng) để rerun xoá rejected cũ của cùng ngày
    df_bad.coalesce(1).write.mode("overwrite").json(rejected_out)

    log.info(f"   ✅ {source}: {n_ok} rows → {bronze_out}"
             + (f" | ⚠️ {n_bad} malformed → {rejected_out}" if n_bad else ""))
    df.unpersist()
    return {"bronze_rows": n_ok, "rejected_rows": n_bad}


# ──────────────── MAIN ────────────────

def run(date_str):
    run_id = os.environ.get("PIPELINE_RUN_ID", "manual")
    log.info(f"🚀 BRONZE INGEST START [{date_str}] run_id={run_id}")
    spark = create_spark("TravelHub_Bronze_Ingest")

    results, failed = {}, []
    for source in SOURCES:
        try:
            results[source] = ingest_source(spark, source, date_str, run_id)
        except Exception as e:  # noqa: BLE001
            log.error(f"❌ {source} failed: {e}")
            results[source] = {"error": str(e)[:300]}
            failed.append(source)

    spark.stop()
    total = sum(r.get("bronze_rows", 0) for r in results.values())
    emit_metrics("bronze", {"rows": total, "sources": results, "failed": failed})
    log.info(f"📊 BRONZE RESULTS: {results}")
    return not failed


if __name__ == "__main__":
    ok = run(parse_date_arg())
    sys.exit(0 if ok else 1)
