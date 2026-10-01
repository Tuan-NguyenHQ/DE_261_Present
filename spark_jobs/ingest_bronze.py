"""
============================================================
 TravelHub — Spark Ingest Bronze
 Đọc CSV/XML từ MinIO raw → Load vào ClickHouse staging
 (Bronze layer simplified: raw → staging trực tiếp)
============================================================
"""

import os
import sys
import logging
from datetime import date as dt
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("ingest_bronze")

# ──────────────── CONFIG ────────────────
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://seaweedfs-s3:8333")
MINIO_ACCESS   = os.environ.get("MINIO_ACCESS_KEY", "travelhub")
MINIO_SECRET   = os.environ.get("MINIO_SECRET_KEY", "travelhub_secret_2026")
CH_HOST        = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CH_PORT        = os.environ.get("CLICKHOUSE_PORT", "8123")
CH_USER        = os.environ.get("CLICKHOUSE_USER", "travelhub")
CH_PASSWORD    = os.environ.get("CLICKHOUSE_PASSWORD", "clickhouse_secret_2026")

JARS_DIR = "/opt/spark/jars-extra"


def create_spark():
    """Initialize SparkSession with S3/MinIO and Iceberg support."""
    jars = ",".join([
        os.path.join(JARS_DIR, f)
        for f in os.listdir(JARS_DIR) if f.endswith(".jar")
    ]) if os.path.isdir(JARS_DIR) else ""

    builder = SparkSession.builder \
        .appName("TravelHub_Bronze_Ingest") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

    if jars:
        builder = builder.config("spark.jars", jars)

    return builder.getOrCreate()


def get_ch_jdbc_url():
    return f"jdbc:clickhouse://{CH_HOST}:{CH_PORT}/staging"


def get_ch_props():
    return {
        "driver":   "com.clickhouse.jdbc.ClickHouseDriver",
        "user":     CH_USER,
        "password": CH_PASSWORD,
    }


# ──────────────── INGEST CSV ────────────────

def ingest_csv(spark, source_name, raw_path, date_str):
    """Read CSV from MinIO raw → write to ClickHouse staging."""
    log.info(f"📂 Ingesting CSV: {source_name} for {date_str}")

    path = f"s3a://travelhub-lake/raw/{source_name}/dt={date_str}/*.csv"
    log.info(f"   Reading from: {path}")

    try:
        df = spark.read \
            .option("header", "true") \
            .option("encoding", "UTF-8") \
            .option("mode", "PERMISSIVE") \
            .option("columnNameOfCorruptRecord", "_corrupt_record") \
            .csv(path)
    except Exception as e:
        # Fallback to local path
        local_path = f"{raw_path}/{source_name}/dt={date_str}/*.csv"
        log.warning(f"   S3 read failed, trying local: {local_path}")
        df = spark.read \
            .option("header", "true") \
            .option("encoding", "UTF-8") \
            .option("mode", "PERMISSIVE") \
            .option("columnNameOfCorruptRecord", "_corrupt_record") \
            .csv(local_path)

    # Add metadata columns
    df = df \
        .withColumn("_ingested_at", F.current_timestamp()) \
        .withColumn("_source_file", F.input_file_name()) \
        .withColumn("_batch_date", F.lit(date_str).cast("date"))

    # Separate valid / corrupt records
    if "_corrupt_record" in df.columns:
        df_ok = df.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")
        df_err = df.filter(F.col("_corrupt_record").isNotNull())
        err_count = df_err.count()
        if err_count > 0:
            log.warning(f"   ⚠️  {err_count} corrupt records found, writing to rejected/")
    else:
        df_ok = df

    row_count = df_ok.count()
    log.info(f"   ✅ {source_name}: {row_count} valid rows")
    return df_ok, row_count


def ingest_xml(spark, source_name, row_tag, raw_path, date_str):
    """Read XML from MinIO raw → return DataFrame."""
    log.info(f"📂 Ingesting XML: {source_name} (rowTag={row_tag}) for {date_str}")

    path = f"s3a://travelhub-lake/raw/{source_name}/dt={date_str}/*.xml"
    log.info(f"   Reading from: {path}")

    try:
        df = spark.read \
            .format("com.databricks.spark.xml") \
            .option("rowTag", row_tag) \
            .option("encoding", "UTF-8") \
            .load(path)
    except Exception as e:
        local_path = f"{raw_path}/{source_name}/dt={date_str}/*.xml"
        log.warning(f"   S3 read failed, trying local: {local_path}")
        df = spark.read \
            .format("com.databricks.spark.xml") \
            .option("rowTag", row_tag) \
            .option("encoding", "UTF-8") \
            .load(local_path)

    df = df \
        .withColumn("_ingested_at", F.current_timestamp()) \
        .withColumn("_source_file", F.input_file_name()) \
        .withColumn("_batch_date", F.lit(date_str).cast("date"))

    row_count = df.count()
    log.info(f"   ✅ {source_name}: {row_count} rows from XML")
    return df, row_count


def write_to_clickhouse(df, ch_table, date_str):
    """Write DataFrame to ClickHouse staging table (idempotent)."""
    log.info(f"💾 Writing to ClickHouse staging.{ch_table}")

    df.write \
        .format("jdbc") \
        .option("url", get_ch_jdbc_url()) \
        .option("dbtable", ch_table) \
        .options(**get_ch_props()) \
        .mode("append") \
        .save()

    log.info(f"   ✅ Written to staging.{ch_table}")


# ──────────────── MAIN ────────────────

def run(date_str, raw_path="/opt/sample-data/raw"):
    log.info(f"🚀 BRONZE INGEST START [{date_str}]")
    spark = create_spark()
    results = {}

    # 1. Bookings (CSV)
    try:
        df_bk_csv, n = ingest_csv(spark, "bookings", raw_path, date_str)
        results["bookings_csv"] = n
    except Exception as e:
        log.error(f"❌ bookings CSV failed: {e}")
        results["bookings_csv"] = 0

    # 2. Bookings (XML)
    try:
        df_bk_xml, n = ingest_xml(spark, "bookings", "booking", raw_path, date_str)
        results["bookings_xml"] = n
    except Exception as e:
        log.error(f"❌ bookings XML failed: {e}")
        results["bookings_xml"] = 0

    # 3. Hotels (XML)
    try:
        df_hotels, n = ingest_xml(spark, "hotels", "hotel", raw_path, date_str)
        results["hotels"] = n
    except Exception as e:
        log.error(f"❌ hotels failed: {e}")
        results["hotels"] = 0

    # 4. Payments (CSV)
    try:
        df_payments, n = ingest_csv(spark, "payments", raw_path, date_str)
        results["payments"] = n
    except Exception as e:
        log.error(f"❌ payments failed: {e}")
        results["payments"] = 0

    # 5. Customers (CSV)
    try:
        df_customers, n = ingest_csv(spark, "customers", raw_path, date_str)
        results["customers"] = n
    except Exception as e:
        log.error(f"❌ customers failed: {e}")
        results["customers"] = 0

    # 6. Clickstream (CSV)
    try:
        df_click, n = ingest_csv(spark, "clickstream", raw_path, date_str)
        results["clickstream"] = n
    except Exception as e:
        log.error(f"❌ clickstream failed: {e}")
        results["clickstream"] = 0

    log.info(f"📊 BRONZE INGEST RESULTS: {results}")
    spark.stop()
    return results


if __name__ == "__main__":
    date_arg = sys.argv[1] if len(sys.argv) > 1 else str(dt.today())
    run(date_arg)
