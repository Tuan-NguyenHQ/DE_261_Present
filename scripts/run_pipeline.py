"""
============================================================
 TravelHub — Pipeline Orchestrator
 Điều phối toàn bộ pipeline: Generate Data → Bronze → Silver → Gold
 Sử dụng: python run_pipeline.py [YYYY-MM-DD]
============================================================
"""

import subprocess
import sys
import os
import logging
import uuid
from datetime import date as dt, datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("/opt/logs/pipeline.log", mode="a"),
    ]
)
log = logging.getLogger("pipeline")

# ──────────────── CONFIG ────────────────
SPARK_MASTER = os.environ.get("SPARK_MASTER", "local[*]")
CH_HOST      = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CH_PORT      = os.environ.get("CLICKHOUSE_PORT", "8123")
CH_USER      = os.environ.get("CLICKHOUSE_USER", "travelhub")
CH_PASSWORD  = os.environ.get("CLICKHOUSE_PASSWORD", "clickhouse_secret_2026")

JARS_DIR     = "/opt/spark/jars-extra"
RAW_PATH     = "/opt/sample-data/raw"
DBT_DIR      = "/opt/dbt"


def get_jars():
    """Collect all JAR files for spark-submit."""
    if not os.path.isdir(JARS_DIR):
        return ""
    jars = [os.path.join(JARS_DIR, f)
            for f in os.listdir(JARS_DIR) if f.endswith(".jar")]
    return ",".join(jars)


def run_step(step_name, cmd, cwd=None):
    """Execute a pipeline step with logging."""
    log.info(f"▶ [{step_name}] Starting...")
    log.info(f"  Command: {' '.join(cmd) if isinstance(cmd, list) else cmd}")

    try:
        result = subprocess.run(
            cmd, check=True, cwd=cwd,
            capture_output=True, text=True, timeout=600
        )
        log.info(f"✅ [{step_name}] Completed successfully")
        if result.stdout:
            log.debug(result.stdout[-500:])
        return True
    except subprocess.CalledProcessError as e:
        log.error(f"❌ [{step_name}] Failed!")
        if e.stdout:
            log.error(f"  STDOUT: {e.stdout[-1500:]}")
        if e.stderr:
            log.error(f"  STDERR: {e.stderr[-1000:]}")
        return False
    except subprocess.TimeoutExpired:
        log.error(f"⏰ [{step_name}] Timed out!")
        return False


def audit_log(run_id, date_str, step, status, rows=0, error=""):
    """Write audit log to ClickHouse."""
    try:
        import clickhouse_connect
        client = clickhouse_connect.get_client(
            host=CH_HOST, port=int(CH_PORT),
            username=CH_USER, password=CH_PASSWORD
        )
        client.command(f"""
            INSERT INTO analytics.pipeline_runs
            (run_id, execution_date, step_name, status, rows_processed,
             started_at, finished_at, error_message)
            VALUES (
                '{run_id}', '{date_str}', '{step}', '{status}',
                {rows}, now(), now(), '{error}'
            )
        """)
    except Exception as e:
        log.warning(f"⚠️  Audit log failed: {e}")


# ──────────────── PIPELINE STEPS ────────────────

def step_generate_data(date_str):
    """Step 0: Generate sample data."""
    return run_step("GENERATE_DATA", [
        "python3", "/opt/scripts/generate_sample_data.py", date_str, RAW_PATH
    ])


def step_upload_to_s3(date_str):
    """Step 1: Upload raw files to SeaweedFS S3."""
    try:
        import boto3
        from botocore.config import Config

        s3_endpoint = os.environ.get("MINIO_ENDPOINT", "http://seaweedfs-s3:8333")
        s3_client = boto3.client(
            "s3",
            endpoint_url=s3_endpoint,
            aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY", "travelhub"),
            aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY", "travelhub_secret_2026"),
            config=Config(s3={"addressing_style": "path"}),
            region_name="us-east-1",
        )

        bucket = "travelhub-lake"
        # Create bucket if not exists
        try:
            s3_client.head_bucket(Bucket=bucket)
        except Exception:
            s3_client.create_bucket(Bucket=bucket)

        # Upload all raw files
        sources = ["bookings", "hotels", "payments", "customers", "clickstream"]
        total_files = 0

        for source in sources:
            local_dir = os.path.join(RAW_PATH, source, f"dt={date_str}")
            if not os.path.isdir(local_dir):
                log.warning(f"  ⚠️  No data for {source}")
                continue

            for filename in os.listdir(local_dir):
                filepath = os.path.join(local_dir, filename)
                object_name = f"raw/{source}/dt={date_str}/{filename}"
                s3_client.upload_file(filepath, bucket, object_name)
                total_files += 1
                log.info(f"  📤 Uploaded: {object_name}")

        log.info(f"✅ [UPLOAD_S3] {total_files} files uploaded")
        return True
    except Exception as e:
        log.error(f"❌ [UPLOAD_S3] Failed: {e}")
        return False


def step_spark_load(date_str):
    """Step 2: Spark loads CSV/XML → ClickHouse staging."""
    jars = get_jars()
    cmd = [
        "spark-submit",
        "--master", SPARK_MASTER,
    ]
    if jars:
        cmd.extend(["--jars", jars])
    cmd.extend([
        "/opt/spark-jobs/load_clickhouse.py",
        date_str, RAW_PATH
    ])
    return run_step("SPARK_LOAD_STAGING", cmd)


def step_dbt_run(date_str):
    """Step 3: dbt run → dimensions + facts + reports."""
    packages_dir = os.path.join(DBT_DIR, "dbt_packages")
    if not os.path.isdir(packages_dir):
        run_step("DBT_DEPS", ["dbt", "deps", "--profiles-dir", DBT_DIR, "--project-dir", DBT_DIR], cwd=DBT_DIR)
    return run_step("DBT_RUN", [
        "dbt", "run",
        "--profiles-dir", DBT_DIR,
        "--project-dir", DBT_DIR,
        "--vars", f"execution_date: '{date_str}'"
    ], cwd=DBT_DIR)


def step_dbt_test(date_str):
    """Step 4: dbt test → data quality validation."""
    return run_step("DBT_TEST", [
        "dbt", "test",
        "--profiles-dir", DBT_DIR,
        "--project-dir", DBT_DIR,
        "--vars", f"execution_date: '{date_str}'"
    ], cwd=DBT_DIR)


# ──────────────── MAIN ORCHESTRATOR ────────────────

def run(date_str):
    run_id = str(uuid.uuid4())[:8]
    log.info("=" * 60)
    log.info(f"🚀 TRAVELHUB PIPELINE START")
    log.info(f"   Run ID:         {run_id}")
    log.info(f"   Execution Date: {date_str}")
    log.info(f"   Started At:     {datetime.now().isoformat()}")
    log.info("=" * 60)

    steps = [
        ("generate_data",    step_generate_data),
        ("upload_s3",        step_upload_to_s3),
        ("spark_load",       step_spark_load),
        ("dbt_run",          step_dbt_run),
        ("dbt_test",         step_dbt_test),
    ]

    for step_name, step_fn in steps:
        success = step_fn(date_str)
        status = "SUCCESS" if success else "FAILED"
        audit_log(run_id, date_str, step_name, status)

        if not success:
            log.error(f"💥 Pipeline failed at step: {step_name}")
            log.error(f"   Subsequent steps will be skipped.")
            return False

    log.info("=" * 60)
    log.info(f"🎉 PIPELINE COMPLETED SUCCESSFULLY")
    log.info(f"   Run ID:         {run_id}")
    log.info(f"   Execution Date: {date_str}")
    log.info(f"   Finished At:    {datetime.now().isoformat()}")
    log.info("=" * 60)
    return True


if __name__ == "__main__":
    execution_date = sys.argv[1] if len(sys.argv) > 1 else str(dt.today())

    os.makedirs("/opt/logs", exist_ok=True)

    success = run(execution_date)
    sys.exit(0 if success else 1)
