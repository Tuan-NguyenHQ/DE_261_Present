"""
============================================================
 TravelHub — Export Data Lake (SeaweedFS S3 → Local Filesystem)
 Trích xuất / đồng bộ toàn bộ file và thư mục từ Data Lake:
   - raw/         (CSV, XML gốc)
   - bronze/      (Parquet all-string + metadata)
   - rejected/    (JSON các bản ghi malformed)
   - silver/      (Parquet đã clean, typed, dedup, PII masked)
   - quarantine/  (Parquet các bản ghi vi phạm DQ rules)
 
 Ra thư mục local trên máy tính (Host) để người dùng có thể
 duyệt, mở và kiểm tra file trực tiếp bằng Windows Explorer,
 VS Code hoặc các công cụ phân tích.

 Sử dụng:
   python3 scripts/export_lake.py [YYYY-MM-DD] [--layer LAYER] [--output-dir DIR]
============================================================
"""

import argparse
import os
import sys
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, EndpointConnectionError

DEFAULT_BUCKET = os.environ.get("LAKE_BUCKET", "travelhub-lake")
ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "travelhub")
SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "travelhub_secret_2026")

KNOWN_LAYERS = ["raw", "bronze", "silver", "quarantine", "rejected"]


def resolve_s3_endpoint():
    """Tự động phát hiện endpoint: trong container hoặc trên máy host."""
    env_endpoint = os.environ.get("MINIO_ENDPOINT")
    if env_endpoint:
        return env_endpoint

    # Thử kết nối trong mạng container trước
    candidates = [
        "http://seaweedfs-s3:8333",  # Trong Docker network
        "http://localhost:8333",     # Ngoài host Windows
        "http://127.0.0.1:8333",
    ]

    for ep in candidates:
        try:
            client = boto3.client(
                "s3", endpoint_url=ep,
                aws_access_key_id=ACCESS_KEY,
                aws_secret_access_key=SECRET_KEY,
                config=Config(s3={"addressing_style": "path"}, connect_timeout=2, retries={"max_attempts": 0}),
                region_name="us-east-1",
            )
            client.list_buckets()
            return ep
        except Exception:
            continue

    return "http://seaweedfs-s3:8333"


def resolve_output_dir(custom_dir=None):
    """Xác định thư mục xuất file local trên máy."""
    if custom_dir:
        os.makedirs(custom_dir, exist_ok=True)
        return os.path.abspath(custom_dir)

    # 1. Nếu có mount /opt/lake trong container
    if os.path.isdir("/opt/lake"):
        return "/opt/lake"

    # 2. Nếu trong container có /opt/sample-data (mount vào ./data/sample trên host)
    if os.path.isdir("/opt/sample-data"):
        target = "/opt/sample-data/lake"
        os.makedirs(target, exist_ok=True)
        return target

    # 3. Nếu chạy trực tiếp trên máy Host (Windows)
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    target = os.path.join(base_dir, "data", "lake")
    os.makedirs(target, exist_ok=True)
    return target


def export_lake(date_str=None, layer_filter=None, output_dir=None, endpoint=None):
    ep = endpoint or resolve_s3_endpoint()
    dest_root = resolve_output_dir(output_dir)

    print("=" * 65)
    print("🌊 TRAVELHUB DATA LAKE EXPORT")
    print(f"   S3 Endpoint:  {ep}")
    print(f"   S3 Bucket:    {DEFAULT_BUCKET}")
    print(f"   Thư mục đích: {dest_root}")
    if date_str:
        print(f"   Lọc theo ngày: dt={date_str}")
    if layer_filter:
        print(f"   Lọc layer:    {layer_filter}")
    print("=" * 65)

    s3 = boto3.client(
        "s3", endpoint_url=ep,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        config=Config(s3={"addressing_style": "path"}),
        region_name="us-east-1",
    )

    try:
        s3.head_bucket(Bucket=DEFAULT_BUCKET)
    except ClientError as e:
        print(f"❌ Không tìm thấy bucket '{DEFAULT_BUCKET}': {e}")
        return False, {}
    except EndpointConnectionError as e:
        print(f"❌ Không thể kết nối tới S3 endpoint '{ep}': {e}")
        return False, {}

    # Lấy danh sách object với pagination
    paginator = s3.get_paginator("list_objects_v2")
    prefix = f"{layer_filter}/" if layer_filter else ""
    page_iterator = paginator.paginate(Bucket=DEFAULT_BUCKET, Prefix=prefix)

    layer_stats = {layer: {"files": 0, "bytes": 0} for layer in KNOWN_LAYERS}
    layer_stats["other"] = {"files": 0, "bytes": 0}

    total_downloaded = 0
    total_skipped = 0
    total_bytes = 0

    print("\n📦 Bắt đầu tải và ánh xạ file ra ngoài filesystem...")

    for page in page_iterator:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            size = obj["Size"]

            # Lọc theo date nếu có
            if date_str and f"dt={date_str}" not in key:
                continue

            # Bỏ qua folder placeholder kết thúc bằng '/'
            if key.endswith("/"):
                continue

            # Phân loại layer
            first_part = key.split("/", 1)[0]
            cat = first_part if first_part in KNOWN_LAYERS else "other"
            layer_stats[cat]["files"] += 1
            layer_stats[cat]["bytes"] += size

            # Đường dẫn file đích
            local_path = os.path.join(dest_root, key.replace("/", os.sep))
            os.makedirs(os.path.dirname(local_path), exist_ok=True)

            # Kiểm tra nếu file đã tồn tại và cùng dung lượng thì bỏ qua để tăng tốc
            if os.path.isfile(local_path) and os.path.getsize(local_path) == size:
                total_skipped += 1
            else:
                s3.download_file(DEFAULT_BUCKET, key, local_path)
                total_downloaded += 1

            total_bytes += size

    # Hiển thị bảng tổng kết
    print("\n" + "─" * 65)
    print(f"{'STAGE / LAYER':<15} | {'SỐ FILE':<10} | {'DUNG LƯỢNG':<15} | {'TRẠNG THÁI'}")
    print("─" * 65)

    for layer in KNOWN_LAYERS:
        f_count = layer_stats[layer]["files"]
        b_count = layer_stats[layer]["bytes"]
        if b_count > 1024 * 1024:
            size_str = f"{b_count / (1024*1024):.2f} MB"
        elif b_count > 1024:
            size_str = f"{b_count / 1024:.2f} KB"
        else:
            size_str = f"{b_count} bytes"

        status = "✅ Có dữ liệu" if f_count > 0 else "⚪ Trống"
        print(f"{layer:<15} | {f_count:<10} | {size_str:<15} | {status}")

    print("─" * 65)
    print(f"🎉 Hoàn tất! Tải mới: {total_downloaded} | Đã đồng bộ: {total_skipped}")
    print(f"📁 Thư mục dữ liệu Data Lake ngoài máy host:")
    print(f"   👉 {dest_root}\n")

    return True, layer_stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export SeaweedFS Data Lake to Local Filesystem")
    parser.add_argument("date", nargs="?", default=None, help="Lọc theo ngày YYYY-MM-DD (tùy chọn)")
    parser.add_argument("--layer", choices=KNOWN_LAYERS, default=None, help="Chỉ xuất 1 layer cụ thể")
    parser.add_argument("--output-dir", default=None, help="Đường dẫn thư mục đích xuất file")
    args = parser.parse_args()

    ok, stats = export_lake(args.date, args.layer, args.output_dir)
    sys.exit(0 if ok else 1)
