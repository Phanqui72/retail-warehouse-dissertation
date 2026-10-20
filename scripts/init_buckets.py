"""Tạo các bucket cần thiết trên MinIO. Chạy lại nhiều lần vẫn an toàn (idempotent).

  lakehouse : nơi Iceberg lưu file Parquet + metadata của bảng (bronze/silver/gold)
  landing   : nơi "hệ thống nguồn" thả file CSV hằng ngày (dữ liệu thô, chưa xử lý)

Mọi tham số đọc từ biến môi trường (config/settings.env + .env) — không có giá trị mặc định trong code,
thiếu biến nào thì báo lỗi ngay để biết sửa file cấu hình.
"""
import os
import sys
import time

import boto3
from botocore.exceptions import ClientError, EndpointConnectionError


def env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        sys.exit(f"[init-buckets] thiếu biến môi trường {name} — kiểm tra config/settings.env và .env")
    return value


s3 = boto3.client(
    "s3",                                       # loại dịch vụ của boto3, không phải tham số cấu hình
    endpoint_url=env("S3_ENDPOINT"),
    region_name=env("AWS_REGION"),
    aws_access_key_id=env("MINIO_ROOT_USER"),
    aws_secret_access_key=env("MINIO_ROOT_PASSWORD"),
)
wanted = [env("LAKEHOUSE_BUCKET"), env("LANDING_BUCKET")]
max_retries, wait_s = int(env("INIT_MAX_RETRIES")), float(env("INIT_RETRY_SECONDS"))

for attempt in range(1, max_retries + 1):       # MinIO có thể chưa sẵn sàng ngay -> thử lại
    try:
        have = {b["Name"] for b in s3.list_buckets().get("Buckets", [])}
        for name in wanted:
            if name not in have:
                s3.create_bucket(Bucket=name)
                print(f"[init-buckets] đã tạo bucket '{name}'")
            else:
                print(f"[init-buckets] bucket '{name}' đã có")
        sys.exit(0)
    except (EndpointConnectionError, ClientError) as e:
        print(f"[init-buckets] chờ MinIO (lần {attempt}/{max_retries}): {e}")
        time.sleep(wait_s)
sys.exit("[init-buckets] không kết nối được MinIO")