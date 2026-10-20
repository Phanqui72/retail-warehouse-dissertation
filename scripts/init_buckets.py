"""Tạo các bucket cần thiết trên MinIO. Chạy lại nhiều lần vẫn an toàn (idempotent).

  lakehouse : nơi Iceberg lưu file Parquet + metadata của bảng (bronze/silver/gold)
  landing   : nơi "hệ thống nguồn" thả file CSV hằng ngày (dữ liệu thô, chưa xử lý)
"""
import os
import sys
import time

import boto3
from botocore.exceptions import ClientError, EndpointConnectionError

s3 = boto3.client(
    "s3",
    endpoint_url=os.environ["S3_ENDPOINT"],
    region_name=os.environ.get("AWS_REGION", "us-east-1"),
    aws_access_key_id=os.environ["MINIO_ROOT_USER"],
    aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
)
wanted = [os.environ["LAKEHOUSE_BUCKET"], os.environ["LANDING_BUCKET"]]

for attempt in range(60):                      # MinIO có thể chưa sẵn sàng ngay -> thử lại tối đa ~3 phút
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
        print(f"[init-buckets] chờ MinIO (lần {attempt + 1}): {e}")
        time.sleep(3)
sys.exit("[init-buckets] không kết nối được MinIO")