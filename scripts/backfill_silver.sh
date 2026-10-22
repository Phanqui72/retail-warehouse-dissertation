#!/usr/bin/env bash
# Backfill Silver (nhóm sales) theo từng tháng.  Dùng: bash scripts/backfill_silver.sh 2021-01 2026-06
# Yêu cầu: đã chạy  silver_load.py --group master --snapshot 2026-09-30  và Bronze đã có dữ liệu các tháng này.
set -uo pipefail
FROM=${1:?"tháng đầu yyyy-mm"}; TO=${2:?"tháng cuối yyyy-mm"}
mkdir -p logs
m="$FROM-01"
while [[ "${m:0:7}" < "$TO" || "${m:0:7}" == "$TO" ]]; do
  start=$m
  end=$(date -d "$m +1 month -1 day" +%F)
  log="logs/silver_${m:0:7}.log"
  echo "=== $start → $end"
  docker compose run --rm spark silver/silver_load.py --group sales --start "$start" --end "$end" > "$log" 2>&1
  rc=$?
  grep -E '\[silver\] [a-z_]+:|VƯỢT NGƯỠNG' "$log" | sed 's/.*\[silver\]/[silver]/' || true
  if [ $rc -ne 0 ]; then echo "LỖI tháng ${m:0:7} — xem $log"; exit $rc; fi
  m=$(date -d "$m +1 month" +%F)
done
echo "XONG backfill Silver $FROM → $TO"
