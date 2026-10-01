"""
============================================================
 TravelHub — Load to ClickHouse (Staging → Analytics)
 Spark reads from local/MinIO and writes to ClickHouse staging
 This is the direct CSV/XML → ClickHouse loader
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
log = logging.getLogger("load_clickhouse")

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

JARS_DIR = "/opt/spark/jars-extra"


def create_spark():
    jars = ",".join([
        os.path.join(JARS_DIR, f)
        for f in os.listdir(JARS_DIR) if f.endswith(".jar")
    ]) if os.path.isdir(JARS_DIR) else ""

    builder = SparkSession.builder.appName("TravelHub_Load_ClickHouse")
    if jars:
        builder = builder.config("spark.jars", jars)
    return builder.getOrCreate()


def load_bookings(spark, raw_path, date_str):
    """Load bookings from CSV + XML with normalization and channel mapping."""
    log.info("📂 Loading bookings (CSV + XML) → staging.bookings")
    csv_path = f"{raw_path}/bookings/dt={date_str}/*.csv"
    xml_path = f"{raw_path}/bookings/dt={date_str}/*.xml"

    dfs = []
    try:
        df_csv = spark.read \
            .option("header", "true") \
            .option("encoding", "UTF-8") \
            .csv(csv_path) \
            .withColumn("_source_file", F.input_file_name())
        dfs.append(df_csv)
    except Exception as e:
        log.warning(f"No CSV bookings found: {e}")

    try:
        df_xml = spark.read \
            .format("com.databricks.spark.xml") \
            .option("rowTag", "booking") \
            .option("encoding", "UTF-8") \
            .load(xml_path) \
            .withColumn("_source_file", F.input_file_name())
        dfs.append(df_xml)
    except Exception as e:
        log.warning(f"No XML bookings found: {e}")

    if not dfs:
        return 0

    df = dfs[0]
    for other in dfs[1:]:
        df = df.unionByName(other, allowMissingColumns=True)

    df_clean = df \
        .withColumn("booking_id", F.upper(F.trim("booking_id"))) \
        .withColumn("hotel_id", F.upper(F.trim("hotel_id"))) \
        .withColumn("user_id", F.trim("user_id")) \
        .withColumn("room_type", F.trim("room_type")) \
        .withColumn("checkin_date", F.to_date("checkin_date", "yyyy-MM-dd")) \
        .withColumn("checkout_date", F.to_date("checkout_date", "yyyy-MM-dd")) \
        .withColumn("total_amount",
            F.regexp_replace("total_amount", "[^0-9.]", "").cast(DecimalType(15, 2))) \
        .withColumn("currency", F.upper(F.trim("currency"))) \
        .withColumn("booking_status",
            F.when(F.upper("booking_status").isin("CONFIRMED", "CONFIRM", "1"), "CONFIRMED")
             .when(F.upper("booking_status").isin("CANCELLED", "CANCEL"), "CANCELLED")
             .when(F.upper("booking_status") == "PENDING", "PENDING")
             .when(F.upper("booking_status") == "NO_SHOW", "NO_SHOW")
             .otherwise(F.upper(F.trim("booking_status")))) \
        .withColumn("booking_channel",
            F.when(F.lower(F.col("source_channel")).isin("app_ios", "app_android"), "Mobile App")
             .when(F.lower(F.col("source_channel")).isin("web", "website"), "Web")
             .when(F.lower(F.col("source_channel")).startswith("partner"), "Partner API")
             .otherwise(F.col("source_channel"))) \
        .withColumn("payment_method", F.upper(F.trim("payment_method"))) \
        .withColumn("nights",
            F.datediff(F.col("checkout_date"), F.col("checkin_date")).cast(IntegerType())) \
        .withColumn("created_at",
            F.to_timestamp("created_at", "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("_ingested_at", F.current_timestamp()) \
        .withColumn("_batch_date", F.lit(date_str).cast("date")) \
        .drop("source_channel")

    df_clean.write \
        .format("jdbc") \
        .option("url", CH_JDBC_URL) \
        .option("dbtable", "bookings") \
        .options(**CH_PROPS) \
        .mode("append") \
        .save()

    count = df_clean.count()
    log.info(f"   ✅ Loaded {count} rows to staging.bookings")
    return count


def load_customers(spark, raw_path, date_str):
    """Load customers from CSV with PII masking (SHA-256) and type casting."""
    log.info("📂 Loading customers (CSV) → staging.customers")
    path = f"{raw_path}/customers/dt={date_str}/*.csv"

    df = spark.read \
        .option("header", "true") \
        .option("encoding", "UTF-8") \
        .csv(path) \
        .withColumn("_source_file", F.input_file_name()) \
        .withColumn("_ingested_at", F.current_timestamp()) \
        .withColumn("_batch_date", F.lit(date_str).cast("date"))

    df_clean = df \
        .withColumn("user_id", F.trim("user_id")) \
        .withColumn("email_hash", F.sha2(F.trim("email"), 256)) \
        .withColumn("phone_hash", F.sha2(F.trim("phone"), 256)) \
        .withColumn("country", F.upper(F.trim("country"))) \
        .withColumn("user_segment", F.lower(F.trim("user_segment"))) \
        .withColumn("loyalty_tier", F.initcap(F.trim("loyalty_tier"))) \
        .withColumn("loyalty_points", F.col("loyalty_points").cast(IntegerType())) \
        .withColumn("first_booking_date", F.to_date("first_booking_date", "yyyy-MM-dd")) \
        .withColumn("total_bookings", F.col("total_bookings").cast(IntegerType())) \
        .withColumn("is_active",
            F.when(F.lower("is_active") == "true", F.lit(True)).otherwise(F.lit(False))) \
        .withColumn("registered_at", F.to_date("registered_at", "yyyy-MM-dd")) \
        .drop("full_name", "email", "phone")

    df_clean.write \
        .format("jdbc") \
        .option("url", CH_JDBC_URL) \
        .option("dbtable", "customers") \
        .options(**CH_PROPS) \
        .mode("append") \
        .save()

    count = df_clean.count()
    log.info(f"   ✅ Loaded {count} rows to staging.customers")
    return count


def load_hotels(spark, raw_path, date_str):
    """Load hotels from XML to staging.hotels."""
    log.info("📂 Loading hotels (XML) → staging.hotels")
    path = f"{raw_path}/hotels/dt={date_str}/*.xml"

    df = spark.read \
        .format("com.databricks.spark.xml") \
        .option("rowTag", "hotel") \
        .option("encoding", "UTF-8") \
        .load(path) \
        .withColumn("star_rating", F.col("star_rating").cast(IntegerType())) \
        .withColumn("latitude", F.col("latitude").cast("double")) \
        .withColumn("longitude", F.col("longitude").cast("double")) \
        .withColumn("total_rooms", F.col("total_rooms").cast(IntegerType())) \
        .withColumn("partner_since", F.to_date("partner_since", "yyyy-MM-dd")) \
        .withColumn("is_active",
            F.when(F.lower("is_active") == "true", F.lit(True)).otherwise(F.lit(False))) \
        .withColumn("_ingested_at", F.current_timestamp()) \
        .withColumn("_source_file", F.input_file_name()) \
        .withColumn("_batch_date", F.lit(date_str).cast("date"))

    df.write \
        .format("jdbc") \
        .option("url", CH_JDBC_URL) \
        .option("dbtable", "hotels") \
        .options(**CH_PROPS) \
        .mode("append") \
        .save()

    count = df.count()
    log.info(f"   ✅ Loaded {count} rows to staging.hotels")
    return count


def load_payments(spark, raw_path, date_str):
    """Load payments from CSV to staging.payments."""
    log.info("📂 Loading payments (CSV) → staging.payments")
    path = f"{raw_path}/payments/dt={date_str}/*.csv"

    df = spark.read \
        .option("header", "true") \
        .option("encoding", "UTF-8") \
        .csv(path) \
        .withColumn("amount",
            F.regexp_replace("amount", "[^0-9.]", "").cast(DecimalType(15, 2))) \
        .withColumn("processed_at",
            F.to_timestamp("processed_at", "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("refund_amount",
            F.when(F.col("refund_amount").isNull() | (F.col("refund_amount") == ""), F.lit(0))
             .otherwise(F.regexp_replace("refund_amount", "[^0-9.]", "").cast(DecimalType(15, 2)))) \
        .withColumn("refund_at",
            F.when((F.col("refund_at").isNull()) | (F.col("refund_at") == ""), F.lit(None).cast(TimestampType()))
             .otherwise(F.to_timestamp("refund_at", "yyyy-MM-dd HH:mm:ss"))) \
        .withColumn("_ingested_at", F.current_timestamp()) \
        .withColumn("_source_file", F.input_file_name()) \
        .withColumn("_batch_date", F.lit(date_str).cast("date"))

    df.write \
        .format("jdbc") \
        .option("url", CH_JDBC_URL) \
        .option("dbtable", "payments") \
        .options(**CH_PROPS) \
        .mode("append") \
        .save()

    count = df.count()
    log.info(f"   ✅ Loaded {count} rows to staging.payments")
    return count


def load_clickstream(spark, raw_path, date_str):
    """Load clickstream from CSV to staging.clickstream."""
    log.info("📂 Loading clickstream (CSV) → staging.clickstream")
    path = f"{raw_path}/clickstream/dt={date_str}/*.csv"

    df = spark.read \
        .option("header", "true") \
        .option("encoding", "UTF-8") \
        .csv(path) \
        .withColumn("event_timestamp",
            F.to_timestamp("event_timestamp", "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("_ingested_at", F.current_timestamp()) \
        .withColumn("_source_file", F.input_file_name()) \
        .withColumn("_batch_date", F.lit(date_str).cast("date"))

    df.write \
        .format("jdbc") \
        .option("url", CH_JDBC_URL) \
        .option("dbtable", "clickstream") \
        .options(**CH_PROPS) \
        .mode("append") \
        .save()

    count = df.count()
    log.info(f"   ✅ Loaded {count} rows to staging.clickstream")
    return count


def run(date_str, raw_path="/opt/sample-data/raw"):
    log.info(f"🚀 LOAD TO CLICKHOUSE START [{date_str}]")
    spark = create_spark()
    results = {}

    loaders = [
        ("bookings",    lambda: load_bookings(spark, raw_path, date_str)),
        ("hotels",      lambda: load_hotels(spark, raw_path, date_str)),
        ("payments",    lambda: load_payments(spark, raw_path, date_str)),
        ("customers",   lambda: load_customers(spark, raw_path, date_str)),
        ("clickstream", lambda: load_clickstream(spark, raw_path, date_str)),
    ]

    for source, loader_fn in loaders:
        try:
            n = loader_fn()
            results[source] = n
        except Exception as e:
            log.error(f"❌ {source} load failed: {e}")
            results[source] = -1

    log.info(f"📊 LOAD RESULTS: {results}")
    spark.stop()
    return results


if __name__ == "__main__":
    date_arg = sys.argv[1] if len(sys.argv) > 1 else str(dt.today())
    raw = sys.argv[2] if len(sys.argv) > 2 else "/opt/sample-data/raw"
    run(date_arg, raw)
