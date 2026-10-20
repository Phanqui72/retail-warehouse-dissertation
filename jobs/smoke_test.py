"""Kiểm tra hạ tầng: Spark có ghi/đọc được Iceberg (bucket lakehouse) và file thô (bucket landing) không.

Chạy:  docker compose run --rm spark /opt/jobs/smoke_test.py
Thành công khi in ra 2 dòng "OK".
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))   # để import được package common/
from common.spark_session import env, get_spark            # noqa: E402

spark = get_spark("smoke_test")
cat = env("SPARK_CATALOG")

# 1) Iceberg: tạo namespace + bảng, ghi 1 dòng, đọc lại
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {cat}.smoke")
spark.sql(f"CREATE TABLE IF NOT EXISTS {cat}.smoke.hello (id INT, msg STRING, created_at TIMESTAMP) USING iceberg")
spark.sql(f"INSERT INTO {cat}.smoke.hello VALUES (1, 'xin chao lakehouse', current_timestamp())")
spark.table(f"{cat}.smoke.hello").show(truncate=False)
print(f"OK iceberg: ghi + đọc bảng {cat}.smoke.hello")

# 2) s3a: ghi 1 file CSV nhỏ vào bucket landing rồi đọc lại
path = f"s3a://{env('LANDING_BUCKET')}/_smoke/hello"
spark.createDataFrame([(1, "a"), (2, "b")], "id int, v string").coalesce(1).write.mode("overwrite").option("header", True).csv(path)
n = spark.read.option("header", True).csv(path).count()
print(f"OK s3a: đọc lại {n} dòng từ {path}")
spark.stop()