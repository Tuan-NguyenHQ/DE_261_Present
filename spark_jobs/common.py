"""
============================================================
 TravelHub — Shared helpers for Spark jobs
 - SparkSession có cấu hình S3A → SeaweedFS (Data Lake)
 - Quy ước đường dẫn các layer trong lake
 - Kết nối ClickHouse (JDBC + clickhouse-connect)
 - Emit metrics cho orchestrator
============================================================

 Data Lake layout (bucket: travelhub-lake)
   raw/<source>/dt=YYYY-MM-DD/*.csv|*.xml      ← landing, bất biến (as-is)
   bronze/<source>/dt=YYYY-MM-DD/*.parquet     ← all-string + metadata
   rejected/<source>/dt=YYYY-MM-DD/*.json      ← bản ghi parse lỗi (malformed)
   silver/<entity>/dt=YYYY-MM-DD/*.parquet     ← typed, clean, dedup, PII masked
   quarantine/<entity>/dt=YYYY-MM-DD/*.parquet ← vi phạm DQ rule (+ _dq_reason)
"""

import json
import logging
import os
import sys
from datetime import datetime

from pyspark.sql import SparkSession

# ──────────────── CONFIG ────────────────
S3_ENDPOINT   = os.environ.get("MINIO_ENDPOINT", "http://seaweedfs-s3:8333")
S3_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "travelhub")
S3_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "travelhub_secret_2026")
LAKE_BUCKET   = os.environ.get("LAKE_BUCKET", "travelhub-lake")

CH_HOST     = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CH_PORT     = os.environ.get("CLICKHOUSE_PORT", "8123")
CH_USER     = os.environ.get("CLICKHOUSE_USER", "travelhub")
CH_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "clickhouse_secret_2026")

CH_JDBC_URL = f"jdbc:clickhouse://{CH_HOST}:{CH_PORT}/staging"
CH_JDBC_PROPS = {
    "driver":   "com.clickhouse.jdbc.ClickHouseDriver",
    "user":     CH_USER,
    "password": CH_PASSWORD,
}

JARS_DIR = "/opt/spark/jars-extra"

LAYERS = ("raw", "bronze", "rejected", "silver", "quarantine")


# ──────────────── LOGGING ────────────────
def get_logger(name):
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    return logging.getLogger(name)


# ──────────────── ARGS ────────────────
def parse_date_arg(argv=None):
    """Lấy execution date (YYYY-MM-DD) từ argv[1], validate format."""
    argv = argv if argv is not None else sys.argv
    date_str = argv[1] if len(argv) > 1 else datetime.today().strftime("%Y-%m-%d")
    datetime.strptime(date_str, "%Y-%m-%d")  # raise ValueError nếu sai format
    return date_str


# ──────────────── LAKE PATHS ────────────────
def lake_path(layer, dataset, date_str=None):
    """s3a://travelhub-lake/<layer>/<dataset>[/dt=<date>]"""
    if layer not in LAYERS:
        raise ValueError(f"Unknown lake layer: {layer}")
    path = f"s3a://{LAKE_BUCKET}/{layer}/{dataset}"
    if date_str:
        path += f"/dt={date_str}"
    return path


# ──────────────── SPARK ────────────────
def create_spark(app_name):
    """SparkSession với S3A trỏ vào SeaweedFS."""
    jars = ",".join(
        os.path.join(JARS_DIR, f) for f in sorted(os.listdir(JARS_DIR)) if f.endswith(".jar")
    ) if os.path.isdir(JARS_DIR) else ""

    builder = (
        SparkSession.builder.appName(app_name)
        # S3A → SeaweedFS
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.change.detection.mode", "none")
        # Ghi đè đúng 1 partition dt=..., không đụng partition khác
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.compression.codec", "snappy")
    )
    # Executor phải gọi ngược được driver (pipeline container) qua DNS docker
    driver_host = os.environ.get("SPARK_DRIVER_HOST")
    if driver_host:
        builder = builder.config("spark.driver.host", driver_host) \
                         .config("spark.driver.bindAddress", "0.0.0.0")
    if jars:
        builder = builder.config("spark.jars", jars)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def path_exists(spark, path):
    """Kiểm tra path (hoặc glob) có tồn tại trên filesystem Hadoop (s3a)."""
    jvm = spark._jvm
    hpath = jvm.org.apache.hadoop.fs.Path(path)
    fs = hpath.getFileSystem(spark._jsc.hadoopConfiguration())
    statuses = fs.globStatus(hpath)
    return statuses is not None and len(statuses) > 0


# ──────────────── CLICKHOUSE ────────────────
def ch_client():
    import clickhouse_connect
    return clickhouse_connect.get_client(
        host=CH_HOST, port=int(CH_PORT), username=CH_USER, password=CH_PASSWORD
    )


# ──────────────── METRICS ────────────────
def emit_metrics(job, metrics):
    """In 1 dòng JSON lên stdout để run_pipeline.py parse & ghi audit."""
    print("PIPELINE_METRICS=" + json.dumps({"job": job, **metrics}, default=str), flush=True)
