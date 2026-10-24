# Container chạy dbt (dbt-core + adapter dbt-trino) — biến đổi Silver -> Gold bằng SQL chạy trên Trino.
FROM python:3.12.7-slim
# git: dbt cần cho "dbt deps" (cài package) và "dbt debug" kiểm tra
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir dbt-core==1.10.13 dbt-trino==1.10.6
WORKDIR /usr/app/dbt