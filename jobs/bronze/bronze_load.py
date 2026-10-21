"""Job Bronze: nạp file CSV từ bucket landing (MinIO) vào bảng Iceberg tầng Bronze theo KHOẢNG NGÀY.

Chạy (trong container spark, xem docs/02_landing_bronze.md):
    docker compose run --rm spark bronze/bronze_load.py --kind master --snapshot 2026-09-30
    docker compose run --rm spark bronze/bronze_load.py --kind daily --start 2023-03-01 --end 2023-03-31

Luồng xử lý (mỗi lần chạy):
  1. ĐỌC MANIFEST  : đọc daily/dt=*/_manifest.json trong khoảng ngày -> biết mỗi ngày phải có file nào, bao nhiêu dòng, md5.
  2. LIỆT KÊ FILE  : liệt kê file CSV thực có trên landing (kể cả file đến muộn "<dataset>__late_dt=YYYY-MM-DD.csv").
  3. KIỂM TRA FILE : md5 thật, header, encoding (UTF-8 hay Windows-1258) của từng file.
  4. ĐỌC CSV       : gom file cùng header + encoding -> mọi cột là chuỗi, PERMISSIVE (dòng hỏng giữ ở _corrupt_record).
  5. ĐỐI CHIẾU     : số dòng thật vs manifest -> loaded / loaded_with_warning / rejected / skipped / missing / not_ready.
  6. GHI BRONZE    : thêm cột metadata, ghi Iceberg bằng overwritePartitions() theo _dt -> chạy lại không bị trùng.
  7. GHI LOG       : mỗi file 1 dòng vào <catalog>.<OPS_NAMESPACE>.ingest_log.
Bronze KHÔNG làm sạch dữ liệu: giữ nguyên giá trị nguồn (dạng chuỗi) để có thể làm lại Silver bất cứ lúc nào.
Mọi tham số đọc từ biến môi trường qua env() (config/settings.env + .env), không có giá trị mặc định trong code.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))   # để import được package common/
from common.spark_session import env, get_spark                 # noqa: E402

LATE_RE = re.compile(r"^(?P<ds>[a-z_]+)__late_dt=(?P<dt>\d{4}-\d{2}-\d{2})\.csv$")
VN = timezone(timedelta(hours=7))


# ----------------------------------------------------------------------------- tham số
def parse_args():
    p = argparse.ArgumentParser(description="Nạp landing -> Bronze")
    p.add_argument("--kind", choices=["daily", "master"], required=True)
    p.add_argument("--start", help="ngày đầu (daily), yyyy-mm-dd")
    p.add_argument("--end", help="ngày cuối (daily), yyyy-mm-dd")
    p.add_argument("--snapshot", help="snapshot_date (master), yyyy-mm-dd")
    p.add_argument("--datasets", default="", help="chỉ nạp các dataset này, cách nhau dấu phẩy")
    p.add_argument("--dry-run", action="store_true", help="chỉ kiểm tra file, không ghi bảng")
    p.add_argument("--landing", help="(test) ghi đè đường dẫn landing, mặc định s3a://LANDING_BUCKET/LANDING_PREFIX")
    p.add_argument("--sink", choices=["iceberg", "parquet"], default="iceberg", help=argparse.SUPPRESS)
    p.add_argument("--parquet-out", help=argparse.SUPPRESS)
    a = p.parse_args()
    if a.kind == "daily" and not (a.start and a.end):
        p.error("--kind daily cần --start và --end")
    if a.kind == "master" and not a.snapshot:
        p.error("--kind master cần --snapshot")
    a.landing = a.landing or f"s3a://{env('LANDING_BUCKET')}/{env('LANDING_PREFIX')}"
    return a

# ----------------------------------------------------------------------------- tiện ích hệ thống file (Hadoop FS: chạy được cả s3a:// lẫn file://)
class FS:
    def __init__(self, spark: SparkSession, root: str):
        jvm = spark._jvm
        self.jvm, self.root = jvm, root
        self.fs = jvm.org.apache.hadoop.fs.Path(root).getFileSystem(spark._jsc.hadoopConfiguration())

    def path(self, p):
        return self.jvm.org.apache.hadoop.fs.Path(p)

    def exists(self, p) -> bool:
        return self.fs.exists(self.path(p))

    def ls(self, p) -> list[str]:
        if not self.exists(p):
            return []
        return [s.getPath().toString() for s in self.fs.listStatus(self.path(p))]

    def read_text(self, p) -> str:
        stream = self.fs.open(self.path(p))
        reader = self.jvm.java.io.BufferedReader(self.jvm.java.io.InputStreamReader(stream, "UTF-8"))
        lines, line = [], reader.readLine()
        while line is not None:
            lines.append(line)
            line = reader.readLine()
        reader.close()
        return "\n".join(lines)


def daterange(a: str, b: str):
    d, e = date.fromisoformat(a), date.fromisoformat(b)
    while d <= e:
        yield d.isoformat()
        d += timedelta(days=1)


# ----------------------------------------------------------------------------- bước 1–3: lập kế hoạch nạp (driver)
def plan_daily(spark, fs: FS, a, want: set[str]):
    """Trả về (danh sách file sẽ nạp, danh sách log cho file không nạp)."""
    plan, logs, manifests = [], [], {}
    days = list(daterange(a.start, a.end))
    for dt in days:
        d = f"{a.landing}/daily/dt={dt}"
        if not fs.exists(d):
            logs.append(dict(dt=dt, dataset="*", file=d, status="not_ready", message="thư mục ngày chưa có trên landing"))
            continue
        if not fs.exists(f"{d}/_SUCCESS"):           # FILE-08: nguồn chưa ghi xong -> không nạp nửa vời
            logs.append(dict(dt=dt, dataset="*", file=f"{d}/_SUCCESS", status="not_ready", message="thiếu _SUCCESS — bỏ qua cả ngày"))
            continue
        man = None
        if fs.exists(f"{d}/_manifest.json"):
            man = json.loads(fs.read_text(f"{d}/_manifest.json"))
            manifests[dt] = {f["file"]: f for f in man["files"]}
        files = [p for p in fs.ls(d) if p.endswith(".csv")]
        listed = manifests.get(dt)
        for p in files:
            name = p.rsplit("/", 1)[1]
            m = LATE_RE.match(name)
            if m:                                      # FILE-01: file của ngày trước đến muộn
                ds, target, expected = m["ds"], m["dt"], None
                if fs.exists(f"{a.landing}/daily/dt={target}/_manifest.json"):
                    old = json.loads(fs.read_text(f"{a.landing}/daily/dt={target}/_manifest.json"))
                    expected = {f["file"]: f for f in old["files"]}.get(f"{ds}.csv")
                if ds in want:
                    plan.append(dict(dt=target, dataset=ds, file=p, expected=expected, degraded=expected is None, note=f"file đến muộn từ dt={target}"))
                continue
            ds = name[:-4]
            if listed is not None and name not in listed:  # FILE-06: file lạ / giao trùng -> không nạp
                logs.append(dict(dt=dt, dataset=ds.split(" ")[0], file=p, status="skipped", message="file không có trong manifest (trùng / lạ)"))
                continue
            if ds not in want:
                continue
            plan.append(dict(dt=dt, dataset=ds, file=p, expected=(listed or {}).get(name), degraded=listed is None,
                             note="" if listed is not None else "thiếu _manifest.json — chế độ degraded"))  # FILE-09
        if listed is not None:                          # file có trong manifest nhưng không thấy trên landing (FILE-01 phía ngày gốc)
            have = {p.rsplit("/", 1)[1] for p in files}
            for name in listed:
                if name not in have and name[:-4] in want:
                    logs.append(dict(dt=dt, dataset=name[:-4], file=f"{d}/{name}", status="missing", message="manifest liệt kê nhưng file chưa đến"))
    late = {(p["dt"], p["dataset"]) for p in plan if p["note"].startswith("file đến muộn")}
    logs = [l for l in logs if not (l["status"] == "missing" and (l["dt"], l["dataset"]) in late)]  # file đã đến muộn trong cùng lần chạy
    return plan, logs


def plan_master(fs: FS, a, want: set[str]):
    d = f"{a.landing}/master/snapshot_date={a.snapshot}"
    if not fs.exists(f"{d}/_SUCCESS"):
        raise SystemExit(f"{d}/_SUCCESS không tồn tại")
    man = json.loads(fs.read_text(f"{d}/_manifest.json")) if fs.exists(f"{d}/_manifest.json") else {"files": []}
    listed = {f["file"]: f for f in man["files"]}
    return [dict(dt=a.snapshot, dataset=p.rsplit("/", 1)[1][:-4], file=p, expected=listed.get(p.rsplit("/", 1)[1]), degraded=not listed, note="")
            for p in fs.ls(d) if p.endswith(".csv") and p.rsplit("/", 1)[1][:-4] in want]


def inspect_files(spark, paths: list[str]) -> dict:
    """Đọc nhị phân từng file (song song trên executor): md5, header, encoding."""
    def info(content: bytearray):
        b = bytes(content)
        md5 = hashlib.md5(b).hexdigest()
        try:
            text, enc = b.decode("utf-8"), "UTF-8"
        except UnicodeDecodeError:                      # FILE-07: file không phải UTF-8
            text, enc = b.decode("cp1258", errors="replace"), "windows-1258"
        header = text.split("\n", 1)[0].lstrip("\ufeff").strip("\r")
        return (md5, header, enc)
    schema = T.StructType([T.StructField("md5", T.StringType()), T.StructField("header", T.StringType()), T.StructField("enc", T.StringType())])
    udf = F.udf(info, schema)
    rows = (spark.read.format("binaryFile").load(paths)
            .select(F.col("path"), udf("content").alias("i")).select("path", "i.*").collect())
    return {r.path: (r.md5, r.header, r.enc) for r in rows}


# ----------------------------------------------------------------------------- bước 4: đọc CSV theo nhóm (cùng header + encoding)
def read_group(spark, paths, header: str, enc: str) -> DataFrame:
    cols = [c.strip().strip('"') for c in header.split(",")]
    schema = T.StructType([T.StructField(c, T.StringType()) for c in cols] + [T.StructField("_corrupt_record", T.StringType())])
    # multiLine=False: mỗi dòng vật lý là 1 bản ghi -> 1 dòng hỏng (ngoặc kép không đóng) chỉ làm hỏng chính nó,
    # không "nuốt" các dòng sau (FILE-05). Nguồn VNB không có xuống dòng bên trong ô nên an toàn.
    return (spark.read.option("header", True).option("multiLine", False).option("escape", '"').option("encoding", enc)
            .option("mode", "PERMISSIVE").option("columnNameOfCorruptRecord", "_corrupt_record")
            .schema(schema).csv(paths)
            .withColumn("_source_file", F.input_file_name()))


def norm(p: str) -> str:
    """input_file_name() trả về URI (có thể mã hóa %20...); chuẩn hóa để so khớp với đường dẫn đã liệt kê."""
    from urllib.parse import unquote
    return unquote(p).replace("file:///", "file:/")


# ----------------------------------------------------------------------------- bước 6: ghi Bronze
def write_bronze(spark, df: DataFrame, table: str, part_col: str, a, cat: str):
    if a.sink == "parquet":
        df.write.mode("overwrite").partitionBy(part_col).parquet(f"{a.parquet_out}/{table}")
        return
    full = f"{cat}.{env('BRONZE_NAMESPACE')}.{table}"
    if not spark.catalog.tableExists(full):
        (df.writeTo(full).using("iceberg").partitionedBy(F.col(part_col))
         .tableProperty("format-version", env("ICEBERG_FORMAT_VERSION"))
         .tableProperty("write.parquet.compression-codec", env("ICEBERG_COMPRESSION")).create())
        return
    existing = {f.name for f in spark.table(full).schema.fields}
    for c in df.columns:                               # FILE-03: schema evolution — cột mới thì thêm vào bảng (kiểu string)
        if c not in existing:
            spark.sql(f"ALTER TABLE {full} ADD COLUMN `{c}` string")
    tbl_cols = [f.name for f in spark.table(full).schema.fields]
    out = df.select(*[F.col(c) if c in df.columns else F.lit(None).cast("string").alias(c) for c in tbl_cols])
    out.writeTo(full).overwritePartitions()           # chỉ thay các phân vùng _dt có trong lần chạy này -> idempotent


def write_log(spark, logs: list[dict], a, cat: str, run_id: str):
    if not logs:
        return
    now = datetime.now(VN).strftime("%Y-%m-%d %H:%M:%S")
    rows = [(run_id, a.kind, l.get("dt"), l.get("dataset"), l.get("file"), l.get("status"), l.get("manifest_rows"), l.get("loaded_rows"),
             l.get("corrupt_rows"), l.get("md5_ok"), l.get("encoding"), l.get("message", ""), now) for l in logs]
    schema = ("run_id string, kind string, dt string, dataset string, file string, status string, manifest_rows bigint, "
              "loaded_rows bigint, corrupt_rows bigint, md5_ok boolean, encoding string, message string, logged_at string")
    df = spark.createDataFrame(rows, schema)
    if a.sink == "parquet":
        df.write.mode("append").parquet(f"{a.parquet_out}/_ops_ingest_log")
        return
    ops = f"{cat}.{env('OPS_NAMESPACE')}"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ops}")
    if not spark.catalog.tableExists(f"{ops}.ingest_log"):
        df.writeTo(f"{ops}.ingest_log").using("iceberg").tableProperty("format-version", env("ICEBERG_FORMAT_VERSION")).create()
    else:
        df.writeTo(f"{ops}.ingest_log").append()


# ----------------------------------------------------------------------------- main
def main():
    a = parse_args()
    spark = get_spark(f"bronze_load_{a.kind}")
    cat = env("SPARK_CATALOG")
    run_id = f"{a.kind}_{datetime.now(VN).strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex[:6]}"
    fs = FS(spark, a.landing)
    want = set(filter(None, a.datasets.split(","))) or (set(env("BRONZE_DAILY_DATASETS").split(",")) if a.kind == "daily" else None)
    if a.kind == "daily":
        plan, logs = plan_daily(spark, fs, a, want)
    else:
        all_master = {p.rsplit("/", 1)[1][:-4] for p in fs.ls(f"{a.landing}/master/snapshot_date={a.snapshot}") if p.endswith(".csv")}
        plan, logs = plan_master(fs, a, want or all_master), []
    print(f"[bronze] run_id={run_id} files_planned={len(plan)} not_loaded={len(logs)}")
    if not plan:
        if not a.dry_run:
            write_log(spark, logs, a, cat, run_id)
        report(logs, run_id)
        spark.stop()
        return
    meta = inspect_files(spark, [p["file"] for p in plan])
    if a.sink == "iceberg" and not a.dry_run:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {cat}.{env('BRONZE_NAMESPACE')}")
    part_col = "_dt" if a.kind == "daily" else "_snapshot_date"

    by_ds: dict[str, list[dict]] = {}
    for p in plan:
        md5, header, enc = meta[p["file"]]
        p.update(md5=md5, header=header, enc=enc, md5_ok=(p["expected"] or {}).get("md5") == md5 if p["expected"] else None)
        by_ds.setdefault(p["dataset"], []).append(p)

    for ds, files in sorted(by_ds.items()):
        # bước 4: đọc theo nhóm header+encoding rồi ghép theo TÊN cột (FILE-04 đảo thứ tự cột vẫn đúng)
        groups: dict[tuple, list[str]] = {}
        for f in files:
            groups.setdefault((f["header"], f["enc"]), []).append(f["file"])
        parts = [read_group(spark, ps, h, e) for (h, e), ps in groups.items()]
        df = parts[0]
        for x in parts[1:]:
            df = df.unionByName(x, allowMissingColumns=True)
        # _row_number: thứ tự dòng trong file (đánh trước khi join để giữ đúng thứ tự đọc)
        df = (df.withColumn("_source_file", F.udf(norm)(F.col("_source_file")))
              .withColumn("_rid", F.monotonically_increasing_id())).cache()
        # bước 5: đếm dòng thật & dòng hỏng theo file, so với manifest
        stats = {r["_source_file"]: (r["n"], r["bad"]) for r in
                 df.groupBy("_source_file").agg(F.count("*").alias("n"), F.count("_corrupt_record").alias("bad")).collect()}
        ok_files, target = [], {}
        for f in files:
            n, bad = stats.get(norm(f["file"]), (0, 0))
            exp = (f["expected"] or {}).get("rows")
            entry = dict(dt=f["dt"], dataset=ds, file=f["file"], manifest_rows=exp, loaded_rows=n, corrupt_rows=bad,
                         md5_ok=f["md5_ok"], encoding=f["enc"])
            if exp is not None and exp != n:            # FILE-02: lệch số dòng -> từ chối cả file
                entry.update(status="rejected", loaded_rows=0, message=f"số dòng {n} ≠ manifest {exp}")
            else:
                warn = [w for w in [
                    f["note"],
                    "md5 khác manifest (file bị sửa sau khi xuất)" if f["md5_ok"] is False else "",
                    f"encoding {f['enc']} → đã đọc lại và lưu UTF-8" if f["enc"] != "UTF-8" else "",
                    f"{bad} dòng CSV hỏng giữ ở _corrupt_record" if bad else ""] if w]
                entry.update(status="loaded_with_warning" if warn else "loaded", message="; ".join(warn))
                ok_files.append(norm(f["file"]))
                target[norm(f["file"])] = f["dt"]
            logs.append(entry)
        if a.dry_run or not ok_files:
            df.unpersist()
            continue
        # bước 6: metadata + ghi
        md5_of = {norm(f["file"]): f["md5"] for f in files}
        tmap = spark.createDataFrame([(k, v, md5_of[k]) for k, v in target.items()],
                                     "_source_file string, _target_dt string, _source_md5 string")
        w = Window.partitionBy("_source_file").orderBy("_rid")
        out = (df.join(F.broadcast(tmap), "_source_file", "inner")      # inner join = chỉ giữ file được chấp nhận
               .withColumn(part_col, F.to_date("_target_dt")).drop("_target_dt")
               .withColumn("_row_number", F.row_number().over(w)).drop("_rid")
               .withColumn("_ingested_at", F.lit(datetime.now(VN).strftime("%Y-%m-%d %H:%M:%S")))
               .withColumn("_batch_id", F.lit(run_id)))
        write_bronze(spark, out, ds, part_col, a, cat)
        print(f"[bronze] {ds}: files={len(ok_files)} rows={sum(stats.get(p, (0, 0))[0] for p in ok_files)}")
        df.unpersist()
    if not a.dry_run:
        write_log(spark, logs, a, cat, run_id)
    report(logs, run_id)
    spark.stop()


def report(logs, run_id):
    from collections import Counter
    c = Counter(l["status"] for l in logs)
    print(f"[bronze] xong run_id={run_id}: " + ", ".join(f"{k}={v}" for k, v in sorted(c.items())))
    for l in logs:
        if l["status"] != "loaded":
            print(f"   - {l['dt']} {l['dataset']:<26} {l['status']:<20} {l.get('message', '')}")


if __name__ == "__main__":
    main()