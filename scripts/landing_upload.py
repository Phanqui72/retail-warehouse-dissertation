"""Đưa dữ liệu VN Beauty (đã giải nén) từ máy lên bucket landing trên MinIO.

Script đóng vai "hệ thống nguồn xuất file mỗi ngày":
    master/snapshot_date=*/...   danh mục (products, listings, ...)
    daily/dt=YYYY-MM-DD/...      giao dịch mỗi ngày
Thứ tự trong 1 ngày: các file CSV -> _manifest.json -> _SUCCESS (cuối cùng)
=> Bronze/Airflow thấy _SUCCESS là chắc chắn cả ngày đã lên đủ.

Mặc định chỉ upload các ngày < REPLAY_START (giữ phần sau để demo replay hằng ngày).
Chạy lại an toàn: object đã có trên MinIO với cùng kích thước thì bỏ qua (resume khi đứt giữa chừng).

Mọi tham số đọc từ biến môi trường (config/settings.env + .env), không có giá trị mặc định trong code.

Ví dụ (trong container landing-upload):
    python /app/scripts/landing_upload.py --dry-run          # xem trước
    python /app/scripts/landing_upload.py                    # master + mọi ngày < REPLAY_START
    python /app/scripts/landing_upload.py --only-dt 2026-07-01 --skip-master   # replay 1 ngày
"""
import argparse
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ORDER = {"_manifest.json": 1, "_SUCCESS": 2}          # file CSV có thứ tự 0 -> lên trước


def env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        sys.exit(f"[landing-upload] thiếu biến môi trường {name} — kiểm tra config/settings.env và .env")
    return value


def s3_client(workers: int):
    return boto3.client(
        "s3",                                       # loại dịch vụ của boto3, không phải tham số cấu hình
        endpoint_url=env("S3_ENDPOINT"),
        region_name=env("AWS_REGION"),
        aws_access_key_id=env("MINIO_ROOT_USER"),
        aws_secret_access_key=env("MINIO_ROOT_PASSWORD"),
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 5, "mode": "standard"},
                      max_pool_connections=workers * 2),
    )


def existing_sizes(s3, bucket: str, prefix: str) -> dict:
    """Liệt kê MỘT lần mọi object dưới prefix -> {key: size}, để biết file nào đã upload."""
    out = {}
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for o in page.get("Contents", []):
            out[o["Key"]] = o["Size"]
    return out


def partitions(src: Path, a, until: str) -> list:
    """Danh sách thư mục cần upload, theo thứ tự thời gian: master trước, daily sau."""
    parts = [] if (a.only_dt or a.skip_master) else sorted((src / "master").glob("snapshot_date=*"))
    for d in sorted((src / "daily").glob("dt=*")):
        dt = d.name[3:]                                  # "dt=2023-03-08" -> "2023-03-08"
        if a.only_dt:
            if dt == a.only_dt:
                parts.append(d)
        elif (not a.since or dt >= a.since) and dt <= until:   # chuỗi ISO so sánh được như ngày
            parts.append(d)
    return parts


def upload_partition(s3, bucket, prefix, src: Path, part: Path, have: dict, dry: bool):
    rel = part.relative_to(src).as_posix()               # vd: daily/dt=2023-03-08
    files = sorted((f for f in part.iterdir() if f.is_file()), key=lambda f: (ORDER.get(f.name, 0), f.name))
    sent = skipped = nbytes = 0
    for f in files:                                      # tuần tự trong 1 ngày để _SUCCESS luôn lên sau cùng
        key = f"{prefix}/{rel}/{f.name}"
        size = f.stat().st_size
        if have.get(key) == size:
            skipped += 1
            continue
        if not dry:
            ctype = {".csv": "text/csv", ".json": "application/json"}.get(f.suffix, "application/octet-stream")
            s3.upload_file(str(f), bucket, key, ExtraArgs={"ContentType": ctype})
        sent += 1
        nbytes += size
    return rel, sent, skipped, nbytes


def main():
    p = argparse.ArgumentParser(description="Upload dữ liệu VNB lên bucket landing")
    p.add_argument("--since", help="chỉ upload dt >= ngày này (yyyy-mm-dd)")
    p.add_argument("--until", help="chỉ upload dt <= ngày này; bỏ trống = ngày trước REPLAY_START")
    p.add_argument("--only-dt", help="upload đúng 1 ngày (replay)")
    p.add_argument("--skip-master", action="store_true", help="không upload thư mục master/")
    p.add_argument("--dry-run", action="store_true", help="chỉ in ra sẽ làm gì")
    a = p.parse_args()

    src, bucket, prefix = Path(env("LANDING_SRC_DIR")), env("LANDING_BUCKET"), env("LANDING_PREFIX")
    workers = int(env("UPLOAD_WORKERS"))
    until = a.until or (date.fromisoformat(env("REPLAY_START")) - timedelta(days=1)).isoformat()
    if not (src / "daily").is_dir():
        sys.exit(f"[landing-upload] không thấy {src}/daily — đã giải nén vào ./data/landing chưa?")

    s3 = s3_client(workers)
    try:
        s3.head_bucket(Bucket=bucket)                    # bucket do minio-init tạo
    except ClientError:
        sys.exit(f"[landing-upload] chưa có bucket '{bucket}' — chạy: docker compose up minio-init")
    have = existing_sizes(s3, bucket, prefix + "/")
    parts = partitions(src, a, until)
    print(f"[landing-upload] {len(parts)} thư mục -> s3://{bucket}/{prefix}/ "
          f"(đến {a.only_dt or until}; đã có sẵn {len(have)} object){' [DRY-RUN]' if a.dry_run else ''}")

    t0, tot = time.time(), [0, 0, 0]
    masters = [x for x in parts if x.parent.name == "master"]
    dailies = [x for x in parts if x.parent.name == "daily"]
    for m in masters:                                    # master trước: daily tham chiếu tới danh mục
        rel, s, k, b = upload_partition(s3, bucket, prefix, src, m, have, a.dry_run)
        tot = [tot[0] + s, tot[1] + k, tot[2] + b]
        print(f"  {rel}: gửi {s}, bỏ qua {k}")
    with ThreadPoolExecutor(max_workers=workers) as ex:  # các NGÀY khác nhau chạy song song
        futs = [ex.submit(upload_partition, s3, bucket, prefix, src, d, have, a.dry_run) for d in dailies]
        for i, fu in enumerate(as_completed(futs), 1):
            rel, s, k, b = fu.result()
            tot = [tot[0] + s, tot[1] + k, tot[2] + b]
            if i % 100 == 0 or i == len(futs):
                print(f"  {i}/{len(futs)} ngày | gửi {tot[0]} file ({tot[2] / 1e6:.0f} MB) | bỏ qua {tot[1]} | {time.time() - t0:.0f}s")
    print(f"[landing-upload] XONG: gửi {tot[0]} file, {tot[2] / 1e6:.1f} MB, bỏ qua {tot[1]} (đã có). {time.time() - t0:.0f}s")


if __name__ == "__main__":
    try:
        main()
    except ClientError as e:
        sys.exit(f"[landing-upload] lỗi S3: {e}")