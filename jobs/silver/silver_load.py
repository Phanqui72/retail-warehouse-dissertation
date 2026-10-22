"""Job Silver: Bronze → Silver theo FILE CẤU HÌNH YAML (config/silver/<dataset>.yml).

Chạy (trong container spark, xem docs/03_silver.md):
    docker compose run --rm spark silver/silver_load.py --group master --snapshot 2026-09-30
    docker compose run --rm spark silver/silver_load.py --group sales  --start 2023-03-01 --end 2023-03-31
    docker compose run --rm spark silver/silver_load.py --datasets orders,order_items --start ... --end ...

Mỗi dataset đi qua 8 bước (thứ tự cố định, nội dung do YAML quyết định):
  1. ĐỌC BRONZE   : lát dữ liệu theo _dt (daily) hoặc _snapshot_date (master).
  2. ÉP KIỂU      : theo `columns:`; giữ bản gốc ở __raw_<cột> để kiểm tra và ghi quarantine.
                    Kiểu `timestamp_vn` đọc được 3 định dạng (VN, ISO-UTC 'Z', dd/MM/yyyy) -> giờ VN.
  3. TRA CỨU      : `lookups:` LEFT JOIN sang bảng Silver khác / từ điển -> cột __<alias>_hit, __<alias>_<cột>.
  4. LUẬT CHẤT LƯỢNG (`rules:` theo đúng thứ tự khai báo). Mỗi luật:
        when   : điều kiện SQL, TRUE = vi phạm
        action : quarantine | fix | flag | fix+flag | count
        fix    : {cột: biểu thức SQL thay thế}      flag: tên cột cờ boolean (is_*)
  5. KHỬ TRÙNG    : bỏ bản ghi trùng hoàn toàn (exact_rule) rồi giữ 1 bản / khóa theo order_by.
  6. GHI SILVER   : MERGE theo khóa (daily, idempotent; chỉ cập nhật khi version_col mới hơn) hoặc ghi đè (master).
  7. QUARANTINE   : dòng bị loại -> lake.ops.quarantine (xóa bản cũ của cùng dataset + khoảng ngày trước khi ghi).
  8. ĐIỂM DQ      : mỗi luật 1 dòng -> lake.ops.dq_results (số vi phạm, tỷ lệ, ngưỡng, đạt/không đạt).
Mọi tham số đọc qua env() (config/settings.env + .env); không có giá trị mặc định trong code.
"""
from __future__ import annotations

import argparse
import re
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))   # để import được package common/
from common.spark_session import env, get_spark                 # noqa: E402

VN = timezone(timedelta(hours=7))
LINEAGE_DAILY = {"_dt": "_bronze_dt", "_source_file": "_source_file", "_batch_id": "_bronze_batch_id"}
LINEAGE_MASTER = {"_snapshot_date": "_snapshot_date", "_source_file": "_source_file", "_batch_id": "_bronze_batch_id"}


# ============================================================================ cấu hình
def load_yaml(path: Path) -> dict:
    text = path.read_text(encoding="utf-8")
    text = re.sub(r"\$\{([A-Z0-9_]+)\}", lambda m: env(m.group(1)), text)   # ${BIẾN} -> giá trị trong settings.env
    return yaml.safe_load(text)


def load_configs(cfg_dir: Path) -> tuple[dict, dict]:
    defaults = load_yaml(cfg_dir / "_defaults.yml")
    cfgs = {}
    for p in sorted(cfg_dir.glob("*.yml")):
        if p.name.startswith("_"):
            continue
        c = load_yaml(p)
        cfgs[c["dataset"]] = c
    return defaults, cfgs


def parse_args():
    p = argparse.ArgumentParser(description="Bronze -> Silver theo cấu hình YAML")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--group", help="nhóm dataset trong YAML (vd: master, sales)")
    g.add_argument("--datasets", help="danh sách dataset, cách nhau dấu phẩy")
    p.add_argument("--start", help="ngày đầu (daily) yyyy-mm-dd")
    p.add_argument("--end", help="ngày cuối (daily) yyyy-mm-dd")
    p.add_argument("--snapshot", help="snapshot_date (master) yyyy-mm-dd")
    p.add_argument("--dry-run", action="store_true", help="chạy hết các bước, in kết quả DQ nhưng không ghi bảng")
    p.add_argument("--local", help=argparse.SUPPRESS)       # (test) thư mục parquet thay cho Iceberg
    return p.parse_args()


# ============================================================================ lớp lưu trữ
class IcebergStore:
    """Đọc/ghi bảng Iceberg qua catalog REST (môi trường thật)."""

    def __init__(self, spark: SparkSession):
        self.spark, self.cat = spark, env("SPARK_CATALOG")
        self.fmt, self.codec = env("ICEBERG_FORMAT_VERSION"), env("ICEBERG_COMPRESSION")

    def name(self, ns, t):
        return f"{self.cat}.{ns}.{t}"

    def exists(self, ns, t):
        return self.spark.catalog.tableExists(self.name(ns, t))

    def read(self, ns, t) -> DataFrame:
        return self.spark.table(self.name(ns, t))

    def _create(self, df, ns, t, partition):
        self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {self.cat}.{ns}")
        if self.exists(ns, t):
            return
        view = f"__new_{t}"
        df.limit(0).createOrReplaceTempView(view)
        part = f"PARTITIONED BY ({partition})" if partition else ""
        self.spark.sql(f"CREATE TABLE {self.name(ns, t)} USING iceberg {part} "
                       f"TBLPROPERTIES ('format-version'='{self.fmt}', 'write.parquet.compression-codec'='{self.codec}') "
                       f"AS SELECT * FROM {view}")

    def merge(self, df, ns, t, key, version_col, partition, on_match):
        self._create(df, ns, t, partition)
        existing = {f.name for f in self.read(ns, t).schema.fields}
        for c in df.columns:                                  # cột mới trong YAML -> thêm vào bảng
            if c not in existing:
                self.spark.sql(f"ALTER TABLE {self.name(ns, t)} ADD COLUMN `{c}` {df.schema[c].dataType.simpleString()}")
        view = f"__src_{t}"
        df.createOrReplaceTempView(view)
        on = " AND ".join(f"t.`{k}` = s.`{k}`" for k in key)
        if on_match == "ignore":
            matched = ""
        elif version_col:
            matched = f"WHEN MATCHED AND s.`{version_col}` >= t.`{version_col}` THEN UPDATE SET *"
        else:
            matched = "WHEN MATCHED THEN UPDATE SET *"
        self.spark.sql(f"MERGE INTO {self.name(ns, t)} t USING {view} s ON {on} {matched} WHEN NOT MATCHED THEN INSERT *")

    def overwrite(self, df, ns, t, partition):
        self._create(df, ns, t, partition)
        df.writeTo(self.name(ns, t)).overwrite(F.lit(True))  # thay toàn bộ (danh mục = full snapshot)

    def append(self, df, ns, t):
        if not self.exists(ns, t):
            self._create(df, ns, t, None)
        df.writeTo(self.name(ns, t)).append()

    def delete(self, ns, t, cond):
        if self.exists(ns, t):
            self.spark.sql(f"DELETE FROM {self.name(ns, t)} WHERE {cond}")


class LocalStore:
    """(Chỉ để test cục bộ) bảng = thư mục parquet; MERGE mô phỏng bằng union + chọn bản mới."""

    def __init__(self, spark, base):
        self.spark, self.base = spark, base

    def path(self, ns, t):
        return f"{self.base}/{ns}/{t}"

    def exists(self, ns, t):
        return Path(self.path(ns, t)).exists()

    def read(self, ns, t):
        return self.spark.read.option("mergeSchema", True).parquet(self.path(ns, t))

    def _write(self, df, ns, t):
        tmp = self.path(ns, t) + "__tmp"
        df.write.mode("overwrite").parquet(tmp)
        import shutil
        shutil.rmtree(self.path(ns, t), ignore_errors=True)
        Path(tmp).rename(self.path(ns, t))

    def merge(self, df, ns, t, key, version_col, partition, on_match):
        if not self.exists(ns, t):
            return self._write(df, ns, t)
        old = self.read(ns, t)
        new = df.withColumn("__new", F.lit(1))
        both = old.withColumn("__new", F.lit(0)).unionByName(new, allowMissingColumns=True)
        if on_match == "ignore":
            order = [F.col("__new").asc()]
        elif version_col:
            order = [F.col(version_col).desc_nulls_last(), F.col("__new").desc()]
        else:
            order = [F.col("__new").desc()]
        w = Window.partitionBy(*key).orderBy(*order)
        self._write(both.withColumn("__rn", F.row_number().over(w)).where("__rn = 1").drop("__rn", "__new").cache(), ns, t)

    def overwrite(self, df, ns, t, partition):
        self._write(df, ns, t)

    def append(self, df, ns, t):
        df.write.mode("append").parquet(self.path(ns, t))

    def delete(self, ns, t, cond):
        if self.exists(ns, t):
            self._write(self.read(ns, t).where(f"NOT ({cond})").cache(), ns, t)


# ============================================================================ bước 2: ép kiểu
TS_FORMATS = ("yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss'Z'", "dd/MM/yyyy HH:mm")


def typed_expr(col: str, typ: str) -> str:
    raw = f"NULLIF(TRIM(`__raw_{col}`), '')"
    if typ == "timestamp_vn":            # ORD-05: nhiều định dạng; 'Z' = giờ UTC -> đổi sang giờ VN
        return (f"COALESCE(to_timestamp({raw}, '{TS_FORMATS[0]}'), "
                f"from_utc_timestamp(to_timestamp({raw}, \"{TS_FORMATS[1]}\"), 'Asia/Ho_Chi_Minh'), "
                f"to_timestamp({raw}, '{TS_FORMATS[2]}'))")
    if typ == "date":
        return f"to_date({raw}, 'yyyy-MM-dd')"
    if typ == "boolean":
        return f"CASE WHEN lower({raw}) IN ('true','1','t','yes') THEN true WHEN lower({raw}) IN ('false','0','f','no') THEN false END"
    if typ == "string":
        return f"`__raw_{col}`"                                # chuỗi giữ nguyên, luật sẽ chuẩn hóa
    return f"CAST({raw} AS {typ})"                             # bigint, int, double, decimal(…)


def cast_columns(df: DataFrame, cfg: dict) -> DataFrame:
    cols = cfg["columns"]
    keep = [c for c in df.columns if c not in cols]            # cột kỹ thuật của Bronze (_dt, _source_file, …)
    raws = [(F.col(c).cast("string") if c in df.columns else F.lit(None).cast("string")).alias(f"__raw_{c}")
            for c in cols]                                     # cột chưa có ở Bronze (vd voucher_code trước 07/2025) -> NULL
    df = df.select(*keep, *raws)
    return df.select("*", *[F.expr(typed_expr(c, t)).alias(c) for c, t in cols.items()])


# ============================================================================ bước 3: tra cứu
def apply_lookups(spark, store, df: DataFrame, cfg: dict) -> DataFrame:
    dicts = cfg.get("dictionaries", {})
    for alias, lk in (cfg.get("lookups") or {}).items():
        if "dict" in lk:
            rows = [(str(k), str(v)) for k, v in dicts[lk["dict"]].items()]
            ref = spark.createDataFrame(rows, "k string, v string")
            ref_key, sel = "k", ["v"]
        else:
            ns = env("SILVER_NAMESPACE")
            if not store.exists(ns, lk["table"]):
                raise SystemExit(f"[silver] cần bảng {ns}.{lk['table']} trước (lookup '{alias}' của {cfg['dataset']}) "
                                 f"— chạy nhóm master / dataset đó trước")
            ref = store.read(ns, lk["table"])
            ref_key, sel = lk.get("ref", lk["src"]), lk.get("select", [])
        ref = (ref.select(F.expr(ref_key).cast("string").alias("__k"), *[F.col(c).alias(f"__{alias}_{c}") for c in sel])
               .where("__k IS NOT NULL").dropDuplicates(["__k"]).withColumn(f"__{alias}_hit", F.lit(True)))
        if lk.get("broadcast", True):
            ref = F.broadcast(ref)
        df = (df.withColumn("__lkkey", F.expr(lk["src"]).cast("string"))
              .join(ref, F.col("__lkkey") == ref["__k"], "left").drop("__k", "__lkkey"))
    return df


# ============================================================================ bước 4: luật
def apply_rules(df: DataFrame, cfg: dict) -> tuple[DataFrame, list, list]:
    df = df.withColumn("__q", F.lit(False)).withColumn("__q_rule", F.lit(None).cast("string"))
    flags, meta = [], []
    for r in cfg.get("rules") or []:
        rid, v = r["id"], f"__v_{r['id'].replace('-', '_')}"
        df = df.withColumn(v, F.expr(f"COALESCE(({r['when']}), false) AND NOT __q"))
        act = r["action"]
        if "quarantine" in act:
            df = (df.withColumn("__q_rule", F.when(F.col(v) & F.col("__q_rule").isNull(), F.lit(rid)).otherwise(F.col("__q_rule")))
                  .withColumn("__q", F.col("__q") | F.col(v)))
        if "fix" in act:
            for col, expr in r["fix"].items():
                df = df.withColumn(col, F.expr(f"CASE WHEN `{v}` THEN {expr} ELSE `{col}` END"))
        if "flag" in act:
            f = r["flag"]
            if f not in flags:
                flags.append(f)
                df = df.withColumn(f, F.lit(False))
            df = df.withColumn(f, F.col(f) | F.col(v))
        meta.append(dict(rule=r, col=v))
    return df, flags, meta


# ============================================================================ bước 5: khử trùng
def sort_cols(spec: str):
    """'updated_at DESC, _ingested_at DESC' -> [col.desc_nulls_last(), …] (F.expr không hiểu ASC/DESC)."""
    out = []
    for part in spec.split(","):
        part = part.strip()
        m = re.match(r"^(.*?)\s+(ASC|DESC)$", part, re.I)
        expr, direction = (m.group(1), m.group(2).upper()) if m else (part, "ASC")
        c = F.expr(expr)
        out.append(c.desc_nulls_last() if direction == "DESC" else c.asc_nulls_last())
    return out


def dedupe(df: DataFrame, cfg: dict, stats: dict) -> tuple[DataFrame, DataFrame | None]:
    d = cfg.get("dedupe") or {}
    data_cols = list(cfg["columns"])
    if d.get("exact_rule"):                                    # bản ghi trùng hoàn toàn (ORD-01, HIS-01, CUS-03…)
        w = Window.partitionBy(*data_cols).orderBy(F.col("_ingested_at").desc())
        df = df.withColumn("__exact", F.row_number().over(w) > 1)
        stats[d["exact_rule"]] = df.where("__exact").count()
        df = df.where("NOT __exact").drop("__exact")
    key_exprs = d.get("key", cfg["key"])
    w = Window.partitionBy(*[F.expr(k) for k in key_exprs]).orderBy(*sort_cols(d.get("order_by", "_ingested_at DESC")))
    df = df.withColumn("__rn", F.row_number().over(w))
    if d.get("rule"):                                          # trùng KHÓA là lỗi (vd PRD-01)
        stats[d["rule"]] = df.where("__rn > 1").count()
    else:                                                      # nhiều phiên bản của cùng thực thể -> giữ bản mới nhất
        stats["VERSIONS_COLLAPSED"] = df.where("__rn > 1").count()
    mapping = None
    if d.get("mapping"):                                       # vd product_id trùng -> product_id được giữ
        m = d["mapping"]["from"]
        w2 = Window.partitionBy(*[F.expr(k) for k in key_exprs])
        mapping = (df.withColumn("__keep", F.first(F.when(F.col("__rn") == 1, F.col(m)), ignorenulls=True).over(w2))
                   .where("__rn > 1").select(F.col(m).alias(f"duplicate_{m}"), F.col("__keep").alias(f"canonical_{m}")))
    return df.where("__rn = 1").drop("__rn"), mapping


# ============================================================================ xử lý 1 dataset
def process(spark, store, cfg, defaults, a, run_id, now) -> list[dict]:
    ds, kind = cfg["dataset"], cfg["kind"]
    bronze_ns, silver_ns, ops_ns = env("BRONZE_NAMESPACE"), env("SILVER_NAMESPACE"), env("OPS_NAMESPACE")
    if not store.exists(bronze_ns, cfg["source"]):
        raise SystemExit(f"[silver] chưa có bảng bronze.{cfg['source']}")
    src = store.read(bronze_ns, cfg["source"])
    if kind == "daily":
        src = src.where(F.col("_dt").between(F.lit(a.start).cast("date"), F.lit(a.end).cast("date")))
        lineage, batch_col, batch_rng = LINEAGE_DAILY, "_dt", (a.start, a.end)
    else:
        src = src.where(F.col("_snapshot_date") == F.lit(a.snapshot).cast("date"))
        lineage, batch_col, batch_rng = LINEAGE_MASTER, "_snapshot_date", (a.snapshot, a.snapshot)

    # ---- 1-2. đọc + ép kiểu; dòng CSV hỏng ở Bronze -> quarantine ngay
    df = cast_columns(src, cfg)
    df = df.withColumn("__corrupt", F.col("_corrupt_record").isNotNull() if "_corrupt_record" in src.columns else F.lit(False))
    key_null = " OR ".join(f"`{k}` IS NULL" for k in cfg["key"])
    base_rules = [
        dict(id="BRONZE-CORRUPT", dimension="Validity", severity="High", action="quarantine",
             when="__corrupt", desc="Dòng CSV hỏng ở Bronze (_corrupt_record)"),
        dict(id="KEY-NULL", dimension="Completeness", severity="High", action="quarantine",
             when=key_null, desc="Khóa chính rỗng hoặc sai kiểu"),
        dict(id="CAST-FAIL", dimension="Validity", severity="Medium", action="count", desc="Giá trị không ép được kiểu (thành NULL)",
             when=" OR ".join(f"(NULLIF(TRIM(`__raw_{c}`),'') IS NOT NULL AND `{c}` IS NULL)"
                              for c, t in cfg["columns"].items() if t != "string") or "false"),
    ]
    cfg = {**cfg, "rules": base_rules + (cfg.get("rules") or [])}

    # ---- 3-4. tra cứu + luật
    df = apply_lookups(spark, store, df, cfg)
    df, flags, meta = apply_rules(df, cfg)
    df = df.cache()
    rows_in = df.count()
    agg = df.agg(*[F.sum(F.col(m["col"]).cast("int")).alias(m["rule"]["id"]) for m in meta]).first().asDict()
    quarantine = df.where("__q")

    # ---- 5. khử trùng (trên các dòng còn lại)
    stats = {}
    good, mapping = dedupe(df.where("NOT __q"), cfg, stats)

    # ---- 6. cột đầu ra = cột YAML + cờ + lineage
    out = good.select(*[F.col(c) for c in cfg["columns"]], *[F.col(f) for f in flags],
                      *[F.col(s).alias(t) for s, t in lineage.items()],
                      F.lit(run_id).alias("_silver_run_id"), F.lit(now).cast("timestamp").alias("_silver_loaded_at")).cache()
    rows_out = out.count()
    n_q = quarantine.count()

    if not a.dry_run:
        if kind == "daily":
            store.merge(out, silver_ns, cfg["target"], cfg["key"], cfg.get("version_col"), cfg.get("partition"), cfg.get("on_match"))
        else:
            store.overwrite(out, silver_ns, cfg["target"], cfg.get("partition"))
        if mapping is not None:
            store.overwrite(mapping, silver_ns, cfg["dedupe"]["mapping"]["table"], None)
        # ---- 7. quarantine (idempotent: xóa bản ghi cũ cùng dataset + khoảng ngày)
        raw_cols = [f"__raw_{c}" for c in cfg["columns"]]
        q = quarantine.select(
            F.lit(run_id).alias("run_id"), F.lit(ds).alias("dataset"), F.col("__q_rule").alias("rule_id"),
            F.concat_ws("|", *[F.col(f"__raw_{k}") for k in cfg["key"]]).alias("record_key"),
            F.to_json(F.struct(*[F.col(c).alias(c[6:]) for c in raw_cols], F.col("_corrupt_record") if "_corrupt_record" in src.columns
                               else F.lit(None).alias("_corrupt_record"))).alias("record"),
            F.col(batch_col).cast("string").alias("batch_date"), F.col("_source_file").alias("source_file"),
            F.lit(now).cast("timestamp").alias("logged_at"))
        store.delete(ops_ns, "quarantine", f"dataset = '{ds}' AND batch_date BETWEEN '{batch_rng[0]}' AND '{batch_rng[1]}'")
        if n_q:
            store.append(q, ops_ns, "quarantine")
    df.unpersist(); out.unpersist()

    # ---- 8. kết quả DQ
    thr = defaults["max_fail_rate"]
    res = []
    for m in meta:
        r = m["rule"]
        failed = int(agg.get(r["id"]) or 0)
        res.append(dict(dataset=ds, rule_id=r["id"], dimension=r.get("dimension"), severity=r.get("severity"),
                        action=r["action"], description=r.get("desc", ""), checked_rows=rows_in, failed_rows=failed,
                        max_fail_rate=float(r.get("max_fail_rate", thr[r.get("severity", "Medium")]))))
    d = cfg.get("dedupe") or {}
    for rid in [x for x in (d.get("exact_rule"), d.get("rule")) if x]:
        res.append(dict(dataset=ds, rule_id=rid, dimension="Uniqueness", severity=d.get("severity", "High"),
                        action="dedupe", description="Bản ghi trùng (đã loại)", checked_rows=rows_in,
                        failed_rows=int(stats.get(rid, 0)), max_fail_rate=float(thr[d.get("severity", "High")])))
    for rid, val in (("ROWS_IN", rows_in), ("QUARANTINED", n_q), ("VERSIONS_COLLAPSED", stats.get("VERSIONS_COLLAPSED", 0)),
                     ("ROWS_OUT", rows_out)):
        res.append(dict(dataset=ds, rule_id=rid, dimension="Summary", severity=None, action="info", description="",
                        checked_rows=rows_in, failed_rows=int(val), max_fail_rate=None))
    print(f"[silver] {ds}: vào={rows_in} quarantine={n_q} gộp_phiên_bản={stats.get('VERSIONS_COLLAPSED', 0)} ra={rows_out}")
    return res


def write_dq(spark, store, results, a, run_id, now, kind):
    rows = []
    for r in results:
        rate = (r["failed_rows"] / r["checked_rows"]) if r["checked_rows"] and r["dimension"] != "Summary" else None
        passed = None if r["max_fail_rate"] is None else (rate or 0) <= r["max_fail_rate"]
        rows.append((run_id, kind, a.start or a.snapshot, a.end or a.snapshot, r["dataset"], r["rule_id"], r["dimension"],
                     r["severity"], r["action"], r["description"], r["checked_rows"], r["failed_rows"], rate,
                     r["max_fail_rate"], passed, now))
    schema = ("run_id string, kind string, start_date string, end_date string, dataset string, rule_id string, dimension string, "
              "severity string, action string, description string, checked_rows bigint, failed_rows bigint, fail_rate double, "
              "max_fail_rate double, passed boolean, logged_at string")
    df = spark.createDataFrame(rows, schema).withColumn("logged_at", F.col("logged_at").cast("timestamp"))
    if not a.dry_run:
        store.append(df, env("OPS_NAMESPACE"), "dq_results")
    return rows


def main():
    a = parse_args()
    spark = get_spark("silver_load")
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")   # định dạng sai -> NULL thay vì lỗi
    store = LocalStore(spark, a.local) if a.local else IcebergStore(spark)
    defaults, cfgs = load_configs(Path(env("SILVER_CONFIG_DIR")))
    if a.group:
        chosen = [c for c in cfgs.values() if c.get("group") == a.group]
    else:
        names = [x.strip() for x in a.datasets.split(",") if x.strip()]
        missing = [n for n in names if n not in cfgs]
        if missing:
            raise SystemExit(f"[silver] không có cấu hình cho: {missing}")
        chosen = [cfgs[n] for n in names]
    chosen.sort(key=lambda c: c.get("order", 100))
    kinds = {c["kind"] for c in chosen}
    if len(kinds) != 1:
        raise SystemExit("[silver] không trộn dataset daily và master trong một lần chạy")
    kind = kinds.pop()
    if kind == "daily" and not (a.start and a.end):
        raise SystemExit("[silver] dataset daily cần --start và --end")
    if kind == "master" and not a.snapshot:
        raise SystemExit("[silver] dataset master cần --snapshot")

    run_id = f"silver_{datetime.now(VN).strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex[:6]}"
    now = datetime.now(VN).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[silver] run_id={run_id} datasets={[c['dataset'] for c in chosen]}{' [DRY-RUN]' if a.dry_run else ''}")
    results = []
    for cfg in chosen:
        results += process(spark, store, cfg, defaults, a, run_id, now)
    rows = write_dq(spark, store, results, a, run_id, now, kind)

    print(f"[silver] xong run_id={run_id}. Luật có vi phạm:")
    for r in rows:
        if r[6] != "Summary" and r[11]:
            mark = "OK " if r[14] else "VƯỢT NGƯỠNG"
            print(f"   {r[4]:<22} {r[5]:<16} {r[8]:<16} {r[11]:>7} / {r[10]:<8} ({(r[12] or 0) * 100:6.3f}% ≤ {r[13] * 100:.2f}%?) {mark}")
    spark.stop()


if __name__ == "__main__":
    main()