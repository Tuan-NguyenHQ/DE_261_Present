"""
============================================================
 TravelHub — Pipeline Orchestrator
 Generate → RAW (lake) → BRONZE → SILVER → ClickHouse staging → dbt (Gold) → dbt test

 Usage:
   python run_pipeline.py [YYYY-MM-DD] [--skip-generate] [--from-step STEP]

   --skip-generate   Không sinh dữ liệu mẫu (dùng khi file thật đã nằm trong raw/)
   --from-step STEP  Chạy lại từ một step (vd: spark_silver) — các step trước bỏ qua
============================================================
"""

import argparse
import json
import logging
import os
import subprocess
import sys
import uuid
from datetime import date as dt, datetime

os.makedirs("/opt/logs", exist_ok=True)
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

S3_ENDPOINT  = os.environ.get("MINIO_ENDPOINT", "http://seaweedfs-s3:8333")
S3_ACCESS    = os.environ.get("MINIO_ACCESS_KEY", "travelhub")
S3_SECRET    = os.environ.get("MINIO_SECRET_KEY", "travelhub_secret_2026")
LAKE_BUCKET  = os.environ.get("LAKE_BUCKET", "travelhub-lake")

JARS_DIR     = "/opt/spark/jars-extra"
SPARK_JOBS   = "/opt/spark-jobs"
LOCAL_DROP   = "/opt/sample-data/raw"   # nơi "hệ thống nguồn" thả file trước khi lên lake
DBT_DIR      = "/opt/dbt"
SOURCES      = ["bookings", "hotels", "payments", "customers", "clickstream"]


def get_jars():
    """Collect all JAR files for spark-submit."""
    if not os.path.isdir(JARS_DIR):
        return ""
    jars = [os.path.join(JARS_DIR, f)
            for f in sorted(os.listdir(JARS_DIR)) if f.endswith(".jar")]
    return ",".join(jars)


def parse_metrics(stdout):
    """Tìm dòng PIPELINE_METRICS={json} do Spark job in ra."""
    for line in reversed((stdout or "").splitlines()):
        if line.startswith("PIPELINE_METRICS="):
            try:
                return json.loads(line.split("=", 1)[1])
            except json.JSONDecodeError:
                return {}
    return {}


def run_step(step_name, cmd, cwd=None, env=None, timeout=900):
    """Execute a pipeline step. Trả về (ok, metrics, error)."""
    log.info(f"▶ [{step_name}] Starting...")
    log.info(f"  Command: {' '.join(cmd)}")

    try:
        result = subprocess.run(
            cmd, check=True, cwd=cwd, env=env,
            capture_output=True, text=True, timeout=timeout
        )
        log.info(f"✅ [{step_name}] Completed successfully")
        tail = "\n".join(l for l in result.stdout.splitlines()[-15:] if l.strip())
        if tail:
            log.info(f"  ...stdout tail:\n{tail}")
        return True, parse_metrics(result.stdout), ""
    except subprocess.CalledProcessError as e:
        log.error(f"❌ [{step_name}] Failed (exit {e.returncode})")
        if e.stdout:
            log.error(f"  STDOUT: {e.stdout[-2500:]}")
        if e.stderr:
            log.error(f"  STDERR: {e.stderr[-2500:]}")
        return False, parse_metrics(e.stdout), (e.stderr or e.stdout or "")[-500:]
    except subprocess.TimeoutExpired:
        log.error(f"⏰ [{step_name}] Timed out after {timeout}s")
        return False, {}, f"timeout {timeout}s"


def audit_log(run_id, date_str, step, status, started_at, rows=0, error="", details=None):
    """Ghi audit vào analytics.pipeline_runs (parameterized, không ghép chuỗi SQL)."""
    try:
        import clickhouse_connect
        client = clickhouse_connect.get_client(
            host=CH_HOST, port=int(CH_PORT), username=CH_USER, password=CH_PASSWORD
        )
        client.insert(
            "analytics.pipeline_runs",
            [[run_id, datetime.strptime(date_str, "%Y-%m-%d").date(), step, status,
              int(rows or 0), started_at, datetime.now(), (error or "")[:1000],
              json.dumps(details or {}, default=str)[:5000]]],
            column_names=["run_id", "execution_date", "step_name", "status",
                          "rows_processed", "started_at", "finished_at",
                          "error_message", "details"],
        )
    except Exception as e:  # noqa: BLE001
        log.warning(f"⚠️  Audit log failed: {e}")


# ──────────────── PIPELINE STEPS ────────────────
# Mỗi step trả về (ok, metrics, error)

def step_generate_data(date_str, run_id):
    """Step 0: (demo) Sinh dữ liệu mẫu — giả lập hệ thống nguồn thả file."""
    return run_step("GENERATE_DATA", [
        "python3", "/opt/scripts/generate_sample_data.py", date_str, LOCAL_DROP
    ])


def step_upload_raw(date_str, run_id):
    """Step 1: Đưa file nguồn lên Data Lake landing zone: s3://travelhub-lake/raw/."""
    try:
        import boto3
        from botocore.config import Config

        s3 = boto3.client(
            "s3", endpoint_url=S3_ENDPOINT,
            aws_access_key_id=S3_ACCESS, aws_secret_access_key=S3_SECRET,
            config=Config(s3={"addressing_style": "path"}), region_name="us-east-1",
        )
        try:
            s3.head_bucket(Bucket=LAKE_BUCKET)
        except Exception:  # noqa: BLE001
            s3.create_bucket(Bucket=LAKE_BUCKET)

        uploaded, missing = {}, []
        for source in SOURCES:
            local_dir = os.path.join(LOCAL_DROP, source, f"dt={date_str}")
            prefix = f"raw/{source}/dt={date_str}/"
            if not os.path.isdir(local_dir):
                # Không có file local → chấp nhận nếu raw đã có sẵn trên lake (nguồn thả thẳng S3)
                existing = s3.list_objects_v2(Bucket=LAKE_BUCKET, Prefix=prefix).get("KeyCount", 0)
                if existing == 0:
                    missing.append(source)
                log.info(f"  ℹ️  {source}: không có file local, trên lake có {existing} object")
                continue

            for filename in sorted(os.listdir(local_dir)):
                key = prefix + filename
                s3.upload_file(os.path.join(local_dir, filename), LAKE_BUCKET, key,
                               ExtraArgs={"Metadata": {"run_id": run_id, "batch_date": date_str}})
                uploaded[key] = os.path.getsize(os.path.join(local_dir, filename))
                log.info(f"  📤 s3://{LAKE_BUCKET}/{key}")

        if missing:
            return False, {"files": len(uploaded)}, f"No raw data for sources: {missing}"
        log.info(f"✅ [UPLOAD_RAW] {len(uploaded)} files → s3://{LAKE_BUCKET}/raw/")
        return True, {"files": len(uploaded), "bytes": sum(uploaded.values())}, ""
    except Exception as e:  # noqa: BLE001
        log.error(f"❌ [UPLOAD_RAW] Failed: {e}")
        return False, {}, str(e)


def _spark_submit(step_name, script, date_str, run_id):
    jars = get_jars()
    cmd = ["spark-submit", "--master", SPARK_MASTER,
           "--py-files", f"{SPARK_JOBS}/common.py"]
    if jars:
        cmd.extend(["--jars", jars])
    cmd.extend([f"{SPARK_JOBS}/{script}", date_str])
    env = {**os.environ, "PIPELINE_RUN_ID": run_id}
    return run_step(step_name, cmd, cwd=SPARK_JOBS, env=env)


def step_spark_bronze(date_str, run_id):
    """Step 2: raw (CSV/XML) → bronze Parquet + rejected/."""
    return _spark_submit("SPARK_BRONZE", "ingest_bronze.py", date_str, run_id)


def step_spark_silver(date_str, run_id):
    """Step 3: bronze → silver Parquet (clean, dedup, PII) + quarantine/."""
    return _spark_submit("SPARK_SILVER", "cleanse_silver.py", date_str, run_id)


def step_load_staging(date_str, run_id):
    """Step 4: silver → ClickHouse staging.* (idempotent + reconciliation)."""
    return _spark_submit("LOAD_STAGING", "load_clickhouse.py", date_str, run_id)


def _dbt(step_name, args, date_str):
    return run_step(step_name, [
        "dbt", *args,
        "--profiles-dir", DBT_DIR, "--project-dir", DBT_DIR,
        "--vars", f"execution_date: '{date_str}'",
    ], cwd=DBT_DIR)


def step_dbt_seed(date_str, run_id):
    """Step 5: dbt deps (nếu thiếu) + dbt seed (fx_rates...)."""
    if not os.path.isdir(os.path.join(DBT_DIR, "dbt_packages", "dbt_utils")):
        ok, _, err = run_step("DBT_DEPS", ["dbt", "deps", "--profiles-dir", DBT_DIR,
                                           "--project-dir", DBT_DIR], cwd=DBT_DIR)
        if not ok:
            return False, {}, err
    return _dbt("DBT_SEED", ["seed"], date_str)


def step_dbt_run(date_str, run_id):
    """Step 6: dbt run → staging views, SCD2 dims, facts, reports."""
    return _dbt("DBT_RUN", ["run"], date_str)


def step_dbt_test(date_str, run_id):
    """Step 7: dbt test → data quality (error fail / warn chỉ cảnh báo)."""
    return _dbt("DBT_TEST", ["test"], date_str)


def step_export_lake(date_str, run_id):
    """Step 8: Đồng bộ toàn bộ Data Lake (raw, bronze, silver, quarantine, rejected) ra local filesystem."""
    return run_step("EXPORT_LAKE", [
        "python3", "/opt/scripts/export_lake.py", date_str
    ])


STEPS = [
    ("generate_data", step_generate_data),
    ("upload_raw",    step_upload_raw),
    ("spark_bronze",  step_spark_bronze),
    ("spark_silver",  step_spark_silver),
    ("load_staging",  step_load_staging),
    ("dbt_seed",      step_dbt_seed),
    ("dbt_run",       step_dbt_run),
    ("dbt_test",      step_dbt_test),
    ("export_lake",   step_export_lake),
]


# ──────────────── MAIN ORCHESTRATOR ────────────────

def run(date_str, skip_generate=False, from_step=None):
    run_id = str(uuid.uuid4())[:8]
    log.info("=" * 60)
    log.info("🚀 TRAVELHUB PIPELINE START")
    log.info(f"   Run ID:         {run_id}")
    log.info(f"   Execution Date: {date_str}")
    log.info(f"   Data Lake:      s3://{LAKE_BUCKET} @ {S3_ENDPOINT}")
    log.info(f"   Started At:     {datetime.now().isoformat()}")
    log.info("=" * 60)

    names = [n for n, _ in STEPS]
    start_idx = names.index(from_step) if from_step else 0

    for idx, (step_name, step_fn) in enumerate(STEPS):
        if idx < start_idx or (step_name == "generate_data" and skip_generate):
            log.info(f"⏭  [{step_name}] skipped")
            continue

        started = datetime.now()
        ok, metrics, error = step_fn(date_str, run_id)
        audit_log(run_id, date_str, step_name, "SUCCESS" if ok else "FAILED",
                  started, rows=metrics.get("rows", 0), error=error, details=metrics)

        if not ok:
            log.error(f"💥 Pipeline failed at step: {step_name} — subsequent steps skipped.")
            return False

    log.info("=" * 60)
    log.info("🎉 PIPELINE COMPLETED SUCCESSFULLY")
    log.info(f"   Run ID:         {run_id}")
    log.info(f"   Execution Date: {date_str}")
    log.info(f"   Finished At:    {datetime.now().isoformat()}")
    log.info("=" * 60)
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TravelHub pipeline orchestrator")
    parser.add_argument("date", nargs="?", default=str(dt.today()), help="YYYY-MM-DD")
    parser.add_argument("--skip-generate", action="store_true")
    parser.add_argument("--from-step", choices=[n for n, _ in STEPS])
    parser.add_argument("--dq-max-reject-ratio", type=float, default=None,
                        help="Ngưỡng tỷ lệ lỗi tối đa cho circuit breaker (mặc định 0.2)")
    args = parser.parse_args()
    datetime.strptime(args.date, "%Y-%m-%d")

    if args.dq_max_reject_ratio is not None:
        os.environ["DQ_MAX_REJECT_RATIO"] = str(args.dq_max_reject_ratio)
    elif args.skip_generate:
        os.environ.setdefault("DQ_MAX_REJECT_RATIO", "0.5")

    sys.exit(0 if run(args.date, args.skip_generate, args.from_step) else 1)
