-- Chạy 1 LẦN DUY NHẤT khi volume Postgres còn trống (lần up đầu tiên).
-- Người chạy là POSTGRES_USER (superuser) -> trở thành chủ sở hữu 2 database.
CREATE DATABASE iceberg_catalog;   -- Iceberg REST catalog lưu danh sách namespace/bảng + con trỏ metadata
CREATE DATABASE airflow;           -- dành cho Airflow ở giai đoạn sau