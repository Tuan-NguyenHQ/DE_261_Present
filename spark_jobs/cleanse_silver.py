"""
============================================================
 TravelHub — Spark Cleanse Silver
 BRONZE (Parquet, all-string)  →  SILVER (Parquet, typed & clean)

 Cho từng entity:
   1. Chuẩn hoá & ép kiểu (trim/upper, Decimal, Date, Timestamp, Bool)
   2. Data Quality rules → bản ghi vi phạm kèm _dq_reason → quarantine/
   3. Dedup theo business key (giữ bản mới nhất) → bản trùng → quarantine/
   4. PII masking (customers): SHA-256 email/phone, drop full_name
   5. Ghi s3a://travelhub-lake/silver/<entity>/dt=YYYY-MM-DD/ (overwrite)
   6. Circuit breaker: tỉ lệ vi phạm DQ > DQ_MAX_REJECT_RATIO → fail job

 Silver schema = contract để load vào ClickHouse staging.*

 Usage: spark-submit cleanse_silver.py YYYY-MM-DD
============================================================
"""

import os
import sys

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DecimalType, IntegerType, StringType

from common import create_spark, emit_metrics, get_logger, lake_path, parse_date_arg, path_exists

log = get_logger("cleanse_silver")

DQ_MAX_REJECT_RATIO = float(os.environ.get("DQ_MAX_REJECT_RATIO", "0.2"))
META_COLS = ["_source_file", "_ingested_at", "_batch_date"]


# ──────────────── HELPERS ────────────────

def clean_str(col):
    """trim + chuỗi rỗng → NULL."""
    c = F.trim(F.col(col))
    return F.when(c == "", F.lit(None).cast(StringType())).otherwise(c)


def to_money(col):
    """'$23,245,166.00' / '44,501,545 VND' / ' 1,000 ' → Decimal(15,2). Không parse được → NULL."""
    digits = F.regexp_replace(F.col(col), r"[^0-9.]", "")
    return F.when(digits == "", None).otherwise(digits).cast(DecimalType(15, 2))


def to_bool(col):
    v = F.lower(F.trim(F.col(col)))
    return (F.when(v.isin("true", "1", "yes", "y"), F.lit(True))
             .when(v.isin("false", "0", "no", "n"), F.lit(False))
             .otherwise(F.lit(None).cast(BooleanType())))


def dq_reason(*rules):
    """rules: (condition_is_bad, code). Trả về chuỗi các mã lỗi, '' nếu hợp lệ."""
    return F.concat_ws(";", *[F.when(cond, F.lit(code)) for cond, code in rules])


def split_dq(df: DataFrame, rules, key, order_col):
    """
    → (valid_df, quarantine_df)
    - valid: không vi phạm rule, đã dedup theo key (giữ order_col mới nhất)
    - quarantine: vi phạm rule hoặc trùng key, kèm _dq_reason
    """
    df = df.withColumn("_dq_reason", dq_reason(*rules))
    invalid = df.filter(F.col("_dq_reason") != "")
    valid = df.filter(F.col("_dq_reason") == "")

    w = Window.partitionBy(key).orderBy(F.col(order_col).desc_nulls_last(),
                                        F.col("_source_file"))
    ranked = valid.withColumn("_rn", F.row_number().over(w))
    dups = ranked.filter("_rn > 1").drop("_rn").withColumn("_dq_reason", F.lit("DUPLICATE_KEY"))
    valid = ranked.filter("_rn = 1").drop("_rn", "_dq_reason")

    quarantine = invalid.unionByName(dups)
    return valid, quarantine


# ──────────────── ENTITY TRANSFORMS ────────────────

def transform_bookings(df):
    status = F.upper(F.trim("booking_status"))
    channel = F.lower(F.trim("source_channel"))
    out = df.select(
        F.upper(clean_str("booking_id")).alias("booking_id"),
        F.upper(clean_str("hotel_id")).alias("hotel_id"),
        F.upper(clean_str("user_id")).alias("user_id"),
        clean_str("room_type").alias("room_type"),
        F.to_date(clean_str("checkin_date"), "yyyy-MM-dd").alias("checkin_date"),
        F.to_date(clean_str("checkout_date"), "yyyy-MM-dd").alias("checkout_date"),
        to_money("total_amount").alias("total_amount"),
        F.upper(clean_str("currency")).alias("currency"),
        F.when(status.isin("CONFIRMED", "CONFIRM", "1"), "CONFIRMED")
         .when(status.isin("CANCELLED", "CANCELED", "CANCEL"), "CANCELLED")
         .when(status == "PENDING", "PENDING")
         .when(status == "NO_SHOW", "NO_SHOW")
         .otherwise("UNKNOWN").alias("booking_status"),
        # Giá trị chuẩn = tên kênh trong dim_channels
        F.when(channel.isin("app_ios", "app_android", "mobile app"), "Mobile App")
         .when(channel.isin("web", "website"), "Web")
         .when(channel.startswith("partner"), "Partner API")
         .otherwise("Other").alias("booking_channel"),
        F.upper(clean_str("payment_method")).alias("payment_method"),
        F.to_timestamp(clean_str("created_at"), "yyyy-MM-dd HH:mm:ss").alias("created_at"),
        *META_COLS,
    ).withColumn("nights", F.datediff("checkout_date", "checkin_date").cast(IntegerType()))

    rules = [
        (F.col("booking_id").isNull(), "MISSING_BOOKING_ID"),
        (F.col("hotel_id").isNull(), "MISSING_HOTEL_ID"),
        (F.col("checkin_date").isNull() | F.col("checkout_date").isNull(), "INVALID_DATE"),
        (F.coalesce(F.col("nights"), F.lit(0)) <= 0, "NON_POSITIVE_NIGHTS"),
        (F.col("total_amount").isNull(), "INVALID_AMOUNT"),
        (F.col("currency").isNull(), "MISSING_CURRENCY"),
    ]
    return out, rules, "booking_id", "created_at"


def transform_hotels(df):
    facilities = F.regexp_replace(F.col("facilities"), r"[\[\]'\" ]", "")
    out = df.select(
        F.upper(clean_str("hotel_id")).alias("hotel_id"),
        clean_str("hotel_name").alias("hotel_name"),
        F.lower(clean_str("hotel_tier")).alias("hotel_tier"),
        clean_str("star_rating").cast(IntegerType()).alias("star_rating"),
        clean_str("city").alias("city"),
        F.upper(clean_str("country")).alias("country"),
        clean_str("region").alias("region"),
        clean_str("latitude").cast("double").alias("latitude"),
        clean_str("longitude").cast("double").alias("longitude"),
        clean_str("total_rooms").cast(IntegerType()).alias("total_rooms"),
        # "['bar', 'beach']" → "bar,beach"  (dbt tách thành Array(String))
        F.lower(F.coalesce(facilities, F.lit(""))).alias("facilities"),
        F.to_date(clean_str("partner_since"), "yyyy-MM-dd").alias("partner_since"),
        F.coalesce(to_bool("is_active"), F.lit(False)).alias("is_active"),
        *META_COLS,
    )
    rules = [
        (F.col("hotel_id").isNull(), "MISSING_HOTEL_ID"),
        (F.col("hotel_name").isNull(), "MISSING_HOTEL_NAME"),
        (~F.coalesce(F.col("star_rating").between(1, 5), F.lit(False)), "INVALID_STAR_RATING"),
        (F.coalesce(F.col("total_rooms"), F.lit(0)) <= 0, "INVALID_TOTAL_ROOMS"),
    ]
    return out, rules, "hotel_id", "_ingested_at"


def transform_payments(df):
    refund = to_money("refund_amount")
    out = df.select(
        F.upper(clean_str("payment_id")).alias("payment_id"),
        F.upper(clean_str("booking_id")).alias("booking_id"),
        to_money("amount").alias("amount"),
        F.upper(clean_str("currency")).alias("currency"),
        F.upper(clean_str("payment_method")).alias("payment_method"),
        F.upper(clean_str("payment_status")).alias("payment_status"),
        F.coalesce(clean_str("gateway_code"), F.lit("")).alias("gateway_code"),
        F.to_timestamp(clean_str("processed_at"), "yyyy-MM-dd HH:mm:ss").alias("processed_at"),
        F.coalesce(refund, F.lit(0).cast(DecimalType(15, 2))).alias("refund_amount"),
        F.to_timestamp(clean_str("refund_at"), "yyyy-MM-dd HH:mm:ss").alias("refund_at"),
        *META_COLS,
    )
    rules = [
        (F.col("payment_id").isNull(), "MISSING_PAYMENT_ID"),
        (F.col("booking_id").isNull(), "MISSING_BOOKING_ID"),
        (F.col("amount").isNull(), "INVALID_AMOUNT"),
        (F.col("processed_at").isNull(), "INVALID_PROCESSED_AT"),
    ]
    return out, rules, "payment_id", "processed_at"


def transform_customers(df):
    email_norm = F.lower(clean_str("email"))
    phone_norm = F.regexp_replace(clean_str("phone"), r"[^0-9+]", "")
    out = df.select(
        F.upper(clean_str("user_id")).alias("user_id"),
        # PII masking — chỉ hash rời khỏi silver, full_name/email/phone bị loại bỏ
        F.coalesce(F.sha2(email_norm, 256), F.lit("")).alias("email_hash"),
        F.coalesce(F.sha2(phone_norm, 256), F.lit("")).alias("phone_hash"),
        F.upper(clean_str("country")).alias("country"),
        F.lower(clean_str("user_segment")).alias("user_segment"),
        F.initcap(clean_str("loyalty_tier")).alias("loyalty_tier"),
        F.coalesce(clean_str("loyalty_points").cast(IntegerType()), F.lit(0)).alias("loyalty_points"),
        F.to_date(clean_str("first_booking_date"), "yyyy-MM-dd").alias("first_booking_date"),
        F.coalesce(clean_str("total_bookings").cast(IntegerType()), F.lit(0)).alias("total_bookings"),
        F.coalesce(to_bool("is_active"), F.lit(False)).alias("is_active"),
        F.to_date(clean_str("registered_at"), "yyyy-MM-dd").alias("registered_at"),
        *META_COLS,
    )
    rules = [
        (F.col("user_id").isNull(), "MISSING_USER_ID"),
        (F.col("email_hash") == "", "MISSING_EMAIL"),
    ]
    return out, rules, "user_id", "_ingested_at"


def transform_clickstream(df):
    out = df.select(
        clean_str("event_id").alias("event_id"),
        clean_str("session_id").alias("session_id"),
        F.upper(clean_str("user_id")).alias("user_id"),            # nullable (anonymous)
        F.lower(clean_str("event_type")).alias("event_type"),
        F.coalesce(clean_str("page_url"), F.lit("")).alias("page_url"),
        F.upper(clean_str("hotel_id")).alias("hotel_id"),          # nullable
        clean_str("search_query").alias("search_query"),           # nullable
        F.lower(clean_str("device_type")).alias("device_type"),
        F.coalesce(clean_str("os"), F.lit("")).alias("os"),
        F.to_timestamp(clean_str("event_timestamp"), "yyyy-MM-dd HH:mm:ss").alias("event_timestamp"),
        *META_COLS,
    )
    rules = [
        (F.col("event_id").isNull(), "MISSING_EVENT_ID"),
        (F.col("session_id").isNull(), "MISSING_SESSION_ID"),
        (F.col("event_timestamp").isNull(), "INVALID_TIMESTAMP"),
        (F.col("event_type").isNull(), "MISSING_EVENT_TYPE"),
    ]
    return out, rules, "event_id", "event_timestamp"


ENTITIES = {
    "bookings":    transform_bookings,
    "hotels":      transform_hotels,
    "payments":    transform_payments,
    "customers":   transform_customers,
    "clickstream": transform_clickstream,
}


# ──────────────── RUN 1 ENTITY ────────────────

def cleanse_entity(spark, entity, date_str):
    src = lake_path("bronze", entity, date_str)
    if not path_exists(spark, src):
        raise FileNotFoundError(f"Bronze not found: {src}")

    bronze = spark.read.parquet(src)
    n_bronze = bronze.count()

    typed, rules, key, order_col = ENTITIES[entity](bronze)
    valid, quarantine = split_dq(typed, rules, key, order_col)
    valid, quarantine = valid.cache(), quarantine.cache()

    n_valid = valid.count()
    n_dup = quarantine.filter(F.col("_dq_reason") == "DUPLICATE_KEY").count()
    n_invalid = quarantine.count() - n_dup

    valid.write.mode("overwrite").parquet(lake_path("silver", entity, date_str))
    quarantine.coalesce(1).write.mode("overwrite").parquet(lake_path("quarantine", entity, date_str))

    reasons = {}
    if n_invalid:
        for r in (quarantine.filter(F.col("_dq_reason") != "DUPLICATE_KEY")
                  .groupBy("_dq_reason").count().collect()):
            reasons[r["_dq_reason"]] = r["count"]

    ratio = (n_invalid / n_bronze) if n_bronze else 0.0
    log.info(f"   ✅ {entity}: bronze={n_bronze} → silver={n_valid} "
             f"| dup={n_dup} | dq_invalid={n_invalid} ({ratio:.1%}) {reasons or ''}")

    valid.unpersist()
    quarantine.unpersist()

    if ratio > DQ_MAX_REJECT_RATIO:
        raise RuntimeError(
            f"DQ circuit breaker: {entity} invalid ratio {ratio:.1%} > {DQ_MAX_REJECT_RATIO:.0%}")

    return {"bronze_rows": n_bronze, "silver_rows": n_valid,
            "duplicates": n_dup, "dq_invalid": n_invalid, "dq_reasons": reasons}


# ──────────────── MAIN ────────────────

def run(date_str):
    log.info(f"🚀 SILVER CLEANSE START [{date_str}] (max reject ratio {DQ_MAX_REJECT_RATIO:.0%})")
    spark = create_spark("TravelHub_Silver_Cleanse")

    results, failed = {}, []
    for entity in ENTITIES:
        log.info(f"🔧 Cleansing {entity}...")
        try:
            results[entity] = cleanse_entity(spark, entity, date_str)
        except Exception as e:  # noqa: BLE001
            log.error(f"❌ {entity} cleanse failed: {e}")
            results[entity] = {"error": str(e)[:300]}
            failed.append(entity)

    spark.stop()
    total = sum(r.get("silver_rows", 0) for r in results.values())
    emit_metrics("silver", {"rows": total, "entities": results, "failed": failed})
    log.info(f"📊 SILVER RESULTS: {results}")
    return not failed


if __name__ == "__main__":
    ok = run(parse_date_arg())
    sys.exit(0 if ok else 1)
