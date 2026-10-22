#!/usr/bin/env bash
# Backfill Bronze theo từng tháng.  Dùng: bash scripts/backfill_bronze.sh 2021-01 2026-06
set -uo pipefail
FROM=${1:?"tháng đầu yyyy-mm"}; TO=${2:?"tháng cuối yyyy-mm"}
mkdir -p logs
m="$FROM-01"
while [[ "${m:0:7}" < "$TO" || "${m:0:7}" == "$TO" ]]; do
  start=$m
  end=$(date -d "$m +1 month -1 day" +%F)
  log="logs/bronze_${m:0:7}.log"
  echo "=== $start → $end"
  docker compose run --rm spark bronze/bronze_load.py --kind daily --start "$start" --end "$end" > "$log" 2>&1
  rc=$?
  grep -E '^\[bronze\] xong|^   -' "$log" || true
  if [ $rc -ne 0 ]; then echo "LỖI tháng ${m:0:7} — xem $log"; exit $rc; fi
  m=$(date -d "$m +1 month" +%F)
done
echo "XONG backfill $FROM → $TO"
