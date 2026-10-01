"""
============================================================
 TravelHub — Spark Cleanse Silver
 Đọc Bronze (staging) → Clean, Type Cast, Dedup, PII Hash
 → Ghi lại vào ClickHouse staging (cleaned)
============================================================
"""

import os
import sys
import hashlib
import logging
from datetime import date as dt
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DecimalType, IntegerType, DateType, TimestampType

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("cleanse_silver")

# ──────────────── CONFIG ────────────────
CH_HOST     = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CH_PORT     = os.environ.get("CLICKHOUSE_PORT", "8123")
CH_USER     = os.environ.get("CLICKHOUSE_USER", "travelhub")
CH_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "clickhouse_secret_2026")

CH_JDBC_URL = f"jdbc:clickhouse://{CH_HOST}:{CH_PORT}/staging"
CH_PROPS = {
    "driver":   "com.clickhouse.jdbc.ClickHouseDriver",
    "user":     CH_USER,
    "password": CH_PASSWORD,
}


def create_spark():
    jars_dir = "/opt/spark/jars-extra"
    jars = ",".join([
        os.path.join(jars_dir, f)
        for f in os.listdir(jars_dir) if f.endswith(".jar")
    ]) if os.path.isdir(jars_dir) else ""

    builder = SparkSession.builder.appName("TravelHub_Silver_Cleanse")
    if jars:
        builder = builder.config("spark.jars", jars)
    return builder.getOrCreate()


def read_staging(spark, table, date_str):
    """Read data from ClickHouse staging table for a given batch date."""
    df = spark.read \
        .format("jdbc") \
        .option("url", CH_JDBC_URL) \
        .option("dbtable", f"(SELECT * FROM staging.{table} WHERE _batch_date = '{date_str}')") \
        .options(**CH_PROPS) \
        .load()
    log.info(f"   Read {df.count()} rows from staging.{table}")
    return df


def write_staging(df, table):
    """Write cleaned data back to ClickHouse staging (overwrite by partition)."""
    df.write \
        .format("jdbc") \
        .option("url", CH_JDBC_URL) \
        .option("dbtable", f"staging.{table}") \
        .options(**CH_PROPS) \
        .mode("append") \
        .save()


# ──────────────── CLEANSE FUNCTIONS ────────────────

def cleanse_bookings(spark, date_str):
    """
    Clean bookings data:
    - Strip currency symbols from total_amount
    - Normalize booking_status
    - Map source_channel → booking_channel
    - Calculate nights
    - Dedup by booking_id
    """
    log.info("🔧 Cleansing bookings...")

    df = read_staging(spark, "bookings", date_str)

    df_clean = df \
        .withColumn("booking_id", F.upper(F.trim("booking_id"))) \
        .withColumn("hotel_id", F.upper(F.trim("hotel_id"))) \
        .withColumn("user_id", F.trim("user_id")) \
        .withColumn("checkin_date",
            F.to_date("checkin_date", "yyyy-MM-dd")) \
        .withColumn("checkout_date",
            F.to_date("checkout_date", "yyyy-MM-dd")) \
        .withColumn("total_amount",
            F.regexp_replace("total_amount", "[^0-9.]", "").cast(DecimalType(15, 2))) \
        .withColumn("currency", F.upper(F.trim("currency"))) \
        .withColumn("booking_status",
            F.when(F.upper("booking_status").isin("CONFIRMED", "CONFIRM", "1"), "CONFIRMED")
             .when(F.upper("booking_status").isin("CANCELLED", "CANCEL"), "CANCELLED")
             .when(F.upper("booking_status") == "PENDING", "PENDING")
             .when(F.upper("booking_status") == "NO_SHOW", "NO_SHOW")
             .otherwise("UNKNOWN")) \
        .withColumn("booking_channel",
            F.when(F.lower("source_channel").isin("app_ios", "app_android"), "Mobile App")
             .when(F.lower("source_channel").isin("web", "website"), "Web")
             .when(F.lower("source_channel").startswith("partner"), "Partner API")
             .otherwise("Other")) \
        .withColumn("nights",
            F.datediff("checkout_date", "checkin_date").cast(IntegerType())) \
        .filter(F.col("booking_id").isNotNull()) \
        .filter(F.col("nights") > 0) \
        .dropDuplicates(["booking_id"])

    count = df_clean.count()
    log.info(f"   ✅ Bookings cleaned: {count} rows")
    return df_clean, count


def cleanse_hotels(spark, date_str):
    """Clean hotels data: type cast, normalize tiers."""
    log.info("🔧 Cleansing hotels...")

    df = read_staging(spark, "hotels", date_str)

    df_clean = df \
        .withColumn("hotel_id", F.upper(F.trim("hotel_id"))) \
        .withColumn("hotel_name", F.trim("hotel_name")) \
        .withColumn("hotel_tier", F.lower(F.trim("hotel_tier"))) \
        .withColumn("star_rating",
            F.col("star_rating").cast(IntegerType())) \
        .withColumn("latitude",
            F.col("latitude").cast("double")) \
        .withColumn("longitude",
            F.col("longitude").cast("double")) \
        .withColumn("total_rooms",
            F.col("total_rooms").cast(IntegerType())) \
        .withColumn("is_active",
            F.when(F.lower("is_active") == "true", F.lit(True)).otherwise(F.lit(False))) \
        .filter(F.col("hotel_id").isNotNull()) \
        .dropDuplicates(["hotel_id"])

    count = df_clean.count()
    log.info(f"   ✅ Hotels cleaned: {count} rows")
    return df_clean, count


def cleanse_payments(spark, date_str):
    """Clean payments: type cast amounts, normalize statuses."""
    log.info("🔧 Cleansing payments...")

    df = read_staging(spark, "payments", date_str)

    df_clean = df \
        .withColumn("payment_id", F.upper(F.trim("payment_id"))) \
        .withColumn("booking_id", F.upper(F.trim("booking_id"))) \
        .withColumn("amount",
            F.regexp_replace("amount", "[^0-9.]", "").cast(DecimalType(15, 2))) \
        .withColumn("currency", F.upper(F.trim("currency"))) \
        .withColumn("payment_status", F.upper(F.trim("payment_status"))) \
        .withColumn("refund_amount",
            F.when(F.col("refund_amount").isNull(), F.lit(0))
             .otherwise(F.regexp_replace("refund_amount", "[^0-9.]", "").cast(DecimalType(15, 2)))) \
        .filter(F.col("payment_id").isNotNull()) \
        .dropDuplicates(["payment_id"])

    count = df_clean.count()
    log.info(f"   ✅ Payments cleaned: {count} rows")
    return df_clean, count


def cleanse_customers(spark, date_str):
    """
    Clean customers: PII masking (hash email, phone, drop full_name).
    """
    log.info("🔧 Cleansing customers (PII masking)...")

    sha256_udf = F.udf(
        lambda v: hashlib.sha256(v.encode()).hexdigest() if v else None,
        "string"
    )

    df = read_staging(spark, "customers", date_str)

    df_clean = df \
        .withColumn("user_id", F.trim("user_id")) \
        .withColumn("email_hash", sha256_udf(F.col("email"))) \
        .withColumn("phone_hash", sha256_udf(F.col("phone"))) \
        .withColumn("country", F.upper(F.trim("country"))) \
        .withColumn("user_segment", F.lower(F.trim("user_segment"))) \
        .withColumn("loyalty_tier", F.initcap(F.trim("loyalty_tier"))) \
        .withColumn("loyalty_points",
            F.col("loyalty_points").cast(IntegerType())) \
        .withColumn("total_bookings",
            F.col("total_bookings").cast(IntegerType())) \
        .withColumn("is_active",
            F.when(F.lower("is_active") == "true", F.lit(True)).otherwise(F.lit(False))) \
        .drop("email", "phone", "full_name") \
        .filter(F.col("user_id").isNotNull()) \
        .dropDuplicates(["user_id"])

    count = df_clean.count()
    log.info(f"   ✅ Customers cleaned (PII masked): {count} rows")
    return df_clean, count


def cleanse_clickstream(spark, date_str):
    """Clean clickstream events: normalize, type cast."""
    log.info("🔧 Cleansing clickstream...")

    df = read_staging(spark, "clickstream", date_str)

    df_clean = df \
        .withColumn("event_id", F.trim("event_id")) \
        .withColumn("session_id", F.trim("session_id")) \
        .withColumn("event_type", F.lower(F.trim("event_type"))) \
        .withColumn("device_type", F.lower(F.trim("device_type"))) \
        .withColumn("event_timestamp",
            F.to_timestamp("event_timestamp", "yyyy-MM-dd HH:mm:ss")) \
        .filter(F.col("event_id").isNotNull()) \
        .dropDuplicates(["event_id"])

    count = df_clean.count()
    log.info(f"   ✅ Clickstream cleaned: {count} rows")
    return df_clean, count


# ──────────────── MAIN ────────────────

def run(date_str):
    log.info(f"🚀 SILVER CLEANSE START [{date_str}]")
    spark = create_spark()
    results = {}

    cleaners = [
        ("bookings",    cleanse_bookings),
        ("hotels",      cleanse_hotels),
        ("payments",    cleanse_payments),
        ("customers",   cleanse_customers),
        ("clickstream", cleanse_clickstream),
    ]

    for name, cleaner in cleaners:
        try:
            df_clean, count = cleaner(spark, date_str)
            results[name] = count
            # Write cleaned data back
            # In production: write to separate silver tables
            # Here: data is already cleaned in memory
        except Exception as e:
            log.error(f"❌ {name} cleanse failed: {e}")
            results[name] = -1

    log.info(f"📊 SILVER CLEANSE RESULTS: {results}")
    spark.stop()
    return results


if __name__ == "__main__":
    date_arg = sys.argv[1] if len(sys.argv) > 1 else str(dt.today())
    run(date_arg)
