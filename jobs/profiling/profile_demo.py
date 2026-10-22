"""DEMO profiling dữ liệu thô (Bronze) — bước 1 trước khi họp chốt data contract.

KHÔNG nằm trong pipeline chính. Script chỉ dùng các phép kiểm tra CHUNG (không biết trước dữ liệu bị gài lỗi gì)
để trả lời: "mỗi bảng có những dấu hiệu bất thường nào?". Kết quả in ra dạng bảng Markdown dễ đọc.

Chạy:
    docker compose run --rm spark profiling/profile_demo.py --start 2023-03-01 --end 2023-03-31 > report/profiling/profiling_2023-03.md
    (thêm --datasets orders,order_items để chỉ quét vài bảng)

8 phép kiểm tra cho mỗi cột (mọi cột ở Bronze đều là chuỗi):
  P1 Thiếu giá trị      : NULL, rỗng, hoặc chữ giả rỗng ('null', 'N/A', 'none', '-')
  P2 Lệch định dạng     : đoán kiểu từng giá trị (số nguyên, số thực, ngày, ngày-giờ, ISO 'Z', dd/MM/yyyy, chữ);
                          giá trị KHÁC kiểu chiếm đa số -> nghi sai định dạng
  P3 Khoảng trắng thừa  : giá trị ≠ TRIM(giá trị)
  P4 Hoa/thường lẫn lộn : cùng một giá trị sau LOWER(TRIM()) nhưng viết nhiều kiểu
  P5 Số âm              : cột số có giá trị < 0
  P6 Giá trị hiếm       : cột ít giá trị khác nhau (≤ 30) có giá trị xuất hiện < 0,1% số dòng (nghi mã lạ)
  P7 Ngày tương lai     : ngày/giờ lớn hơn ngày giao file (_dt) + 1 ngày
  P8 Trùng              : dòng trùng hoàn toàn; cột đầu tiên (khóa ứng viên) bị lặp
"""
import argparse
import sys
from pathlib import Path

from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.spark_session import env, get_spark  # noqa: E402

TECH = {"_dt", "_snapshot_date", "_source_file", "_source_md5", "_row_number", "_ingested_at", "_batch_id", "_corrupt_record"}
FAKE_NULL = ("null", "n/a", "na", "none", "-", "")
PATTERNS = {                                           # thứ tự = ưu tiên khi đoán kiểu
    "số nguyên": r"^-?[0-9]+$",
    "số thực": r"^-?[0-9]+\.[0-9]+$",
    "ngày yyyy-MM-dd": r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$",
    "ngày-giờ yyyy-MM-dd HH:mm:ss": r"^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$",
    "ISO-8601 UTC ...Z": r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$",
    "dd/MM/yyyy HH:mm": r"^[0-9]{2}/[0-9]{2}/[0-9]{4} [0-9]{2}:[0-9]{2}$",
    "true/false": r"^(?i)(true|false)$",
}


def kind_expr(c):
    """Biểu thức SQL trả về tên 'kiểu' của giá trị (dùng regex)."""
    cases = " ".join(f"WHEN `{c}` RLIKE '{p}' THEN '{k}'" for k, p in PATTERNS.items())
    return f"CASE WHEN `{c}` IS NULL OR lower(trim(`{c}`)) IN {FAKE_NULL} THEN NULL {cases} ELSE 'chữ' END"


def profile(spark, name, df, n):
    findings = []                                       # (cột, mã, mô tả, số dòng, ví dụ)
    cols = [c for c in df.columns if c not in TECH]

    if "_corrupt_record" in df.columns:
        bad = df.where("_corrupt_record IS NOT NULL").count()
        if bad:
            findings.append(("*", "P0", "Dòng CSV hỏng (sai số cột / ngoặc kép)", bad, ""))

    # ---- P8 trùng
    dup = n - df.select(*cols).distinct().count()
    if dup:
        findings.append(("*", "P8", "Dòng trùng hoàn toàn", dup, ""))
    k = cols[0]
    kdup = n - df.select(k).distinct().count()
    if kdup and not dup >= kdup:
        findings.append((k, "P8", f"Khóa ứng viên `{k}` lặp (có thể là nhiều phiên bản / cần hỏi)", kdup, ""))

    for c in cols:
        s = df.select(F.col(c).alias("v"), F.col("_dt").alias("dt") if "_dt" in df.columns else F.lit(None).alias("dt"))
        s = s.withColumn("kind", F.expr(kind_expr("v")))
        kinds = {r["kind"]: r["count"] for r in s.groupBy("kind").count().collect()}
        filled = {k_: v for k_, v in kinds.items() if k_ is not None}

        # P1 thiếu giá trị (NULL + chữ giả rỗng)
        miss = kinds.get(None, 0)
        fake = s.where(F.lower(F.trim("v")).isin(*FAKE_NULL) & F.col("v").isNotNull()).count()
        if miss and miss / n >= 0.001:
            ex = f"{fake} dòng là chữ giả rỗng ('N/A', 'null'…)" if fake else ""
            findings.append((c, "P1", "Thiếu giá trị", miss, ex))
        if not filled:
            continue
        main = max(filled, key=filled.get)

        # P2 lệch định dạng: kiểu khác kiểu chiếm đa số (bỏ qua cột chữ thuần)
        if main != "chữ":
            for k_, cnt in sorted(filled.items(), key=lambda x: -x[1]):
                if k_ != main and not (main == "số nguyên" and k_ == "số thực"):
                    ex = ", ".join(r["v"] for r in s.where(F.col("kind") == k_).select("v").limit(2).collect())
                    findings.append((c, "P2", f"Lệch định dạng: '{k_}' trong cột chủ yếu là '{main}'", cnt, ex))

        # P3 khoảng trắng thừa
        ws = s.where(F.col("v") != F.trim("v"))
        nws = ws.count()
        if nws:
            findings.append((c, "P3", "Khoảng trắng thừa đầu/cuối", nws, repr(ws.first()["v"])))

        # P4 hoa/thường lẫn lộn (chỉ cột chữ)
        if main == "chữ":
            g = (s.where(F.col("v").isNotNull()).groupBy(F.lower(F.trim("v")).alias("norm"))
                 .agg(F.countDistinct("v").alias("forms"), F.count("*").alias("rows"), F.collect_set("v").alias("ex"))
                 .where("forms > 1"))
            r = g.agg(F.sum("rows").alias("rows"), F.first("ex").alias("ex")).first()
            if r["rows"]:
                findings.append((c, "P4", "Cùng giá trị nhưng viết nhiều kiểu (hoa/thường, khoảng trắng)", r["rows"],
                                 " | ".join(repr(x) for x in r["ex"][:3])))

        # P5 số âm
        if main in ("số nguyên", "số thực"):
            neg = s.where(F.col("v").cast("double") < 0)
            nneg = neg.count()
            if nneg:
                findings.append((c, "P5", "Giá trị âm", nneg, neg.first()["v"]))

        # P6 giá trị hiếm trong cột ít giá trị
        dist = len(s.select("v").distinct().limit(31).collect())
        if 1 < dist <= 30:
            rare = [r for r in s.where(F.col("v").isNotNull()).groupBy("v").count().collect() if r["count"] / n < 0.001]
            if rare and len(rare) < dist / 2:
                findings.append((c, "P6", "Giá trị hiếm (nghi mã ngoài danh mục)", sum(r["count"] for r in rare),
                                 ", ".join(f"'{r['v']}'×{r['count']}" for r in rare[:4])))

        # P7 ngày tương lai so với ngày giao file
        if "ngày" in main or "giờ" in main or main.startswith("ISO"):
            fut = s.where(F.to_date(F.substring("v", 1, 10)) > F.date_add("dt", 1))
            nf = fut.count()
            if nf:
                findings.append((c, "P7", "Ngày lớn hơn ngày giao file + 1 (nghi ngày tương lai)", nf, fut.first()["v"]))
    return findings


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--start", required=True)
    p.add_argument("--end", required=True)
    p.add_argument("--datasets", default="", help="mặc định: mọi bảng daily trong BRONZE_DAILY_DATASETS")
    p.add_argument("--local", help=argparse.SUPPRESS)
    a = p.parse_args()
    spark = get_spark("profile_demo")
    names = [x for x in (a.datasets or env("BRONZE_DAILY_DATASETS")).split(",") if x]

    print(f"# Báo cáo profiling Bronze — {a.start} → {a.end}\n")
    print("> Kiểm tra chung, không dùng danh sách lỗi đã biết. Mỗi dòng là một DẤU HIỆU cần xác nhận trong buổi họp.\n")
    summary = []
    for name in names:
        if a.local:
            df = spark.read.option("mergeSchema", True).parquet(f"{a.local}/{name}")
        else:
            df = spark.table(f"{env('SPARK_CATALOG')}.{env('BRONZE_NAMESPACE')}.{name}")
        df = df.where(F.col("_dt").between(a.start, a.end)).cache()
        n = df.count()
        if not n:
            continue
        f = profile(spark, name, df, n)
        df.unpersist()
        summary.append((name, n, len(f), sum(1 for x in f if x[1] in ("P0", "P2", "P7", "P8"))))
        print(f"## {name} — {n:,} dòng, {len(df.columns) - len(TECH & set(df.columns))} cột, {len(f)} dấu hiệu\n")
        if not f:
            print("Không phát hiện dấu hiệu bất thường.\n")
            continue
        print("| Cột | Mã | Dấu hiệu | Số dòng | % | Ví dụ |")
        print("|---|---|---|---:|---:|---|")
        for c, code, desc, cnt, ex in sorted(f, key=lambda x: (x[1], x[0])):
            ex = str(ex).replace("|", "/")[:60]
            print(f"| `{c}` | {code} | {desc} | {cnt:,} | {cnt / n * 100:.2f}% | {ex} |")
        print()

    print("## Tổng hợp\n")
    print("| Bảng | Số dòng | Số dấu hiệu | Trong đó nghiêm trọng (P0/P2/P7/P8) |")
    print("|---|---:|---:|---:|")
    for name, n, k, hard in summary:
        print(f"| {name} | {n:,} | {k} | {hard} |")
    spark.stop()


if __name__ == "__main__":
    main()
