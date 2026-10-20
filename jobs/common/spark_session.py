"""Tạo SparkSession dùng chung cho mọi job (bronze, silver, gold...).

Hai "đường" tới MinIO được cấu hình ở đây:
  1. Catalog Iceberg (tên lấy từ SPARK_CATALOG) -> hỏi Iceberg REST "bảng X nằm ở đâu", rồi đọc/ghi file bằng S3FileIO.
  2. s3a://  -> đọc file thô (CSV ở bucket landing) như một hệ thống file thường.
Mọi tham số lấy từ biến môi trường (config/settings.env + .env) — không có giá trị mặc định trong code.
"""
import os

from pyspark.sql import SparkSession


def env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"thiếu biến môi trường {name} — kiểm tra config/settings.env và .env")
    return value


def get_spark(app_name: str) -> SparkSession:
    cat = env("SPARK_CATALOG")
    endpoint = env("S3_ENDPOINT")
    user, pwd = env("MINIO_ROOT_USER"), env("MINIO_ROOT_PASSWORD")
    builder = (
        SparkSession.builder.appName(app_name)
        # ---- (1) Iceberg ----
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{cat}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{cat}.type", "rest")
        .config(f"spark.sql.catalog.{cat}.uri", env("ICEBERG_REST_URI"))
        .config(f"spark.sql.catalog.{cat}.warehouse", f"s3://{env('LAKEHOUSE_BUCKET')}/")
        .config(f"spark.sql.catalog.{cat}.io-impl", env("CATALOG_IO__IMPL"))
        .config(f"spark.sql.catalog.{cat}.s3.endpoint", endpoint)
        .config(f"spark.sql.catalog.{cat}.s3.path-style-access", env("CATALOG_S3_PATH__STYLE__ACCESS"))
        .config(f"spark.sql.catalog.{cat}.s3.access-key-id", user)
        .config(f"spark.sql.catalog.{cat}.s3.secret-access-key", pwd)
        .config(f"spark.sql.catalog.{cat}.client.region", env("AWS_REGION"))
        .config("spark.sql.defaultCatalog", cat)
        # ---- (2) s3a ----
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access", env("CATALOG_S3_PATH__STYLE__ACCESS"))
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(endpoint.startswith("https")).lower())
        .config("spark.hadoop.fs.s3a.access.key", user)
        .config("spark.hadoop.fs.s3a.secret.key", pwd)
        # ---- chung ----
        .config("spark.sql.session.timeZone", env("TZ"))
        .config("spark.sql.shuffle.partitions", env("SPARK_SHUFFLE_PARTITIONS"))
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(env("SPARK_LOG_LEVEL"))
    return spark