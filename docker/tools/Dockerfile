# Container Python nhỏ để chạy các script tiện ích (tạo bucket, upload landing...).
FROM python:3.12.7-slim
WORKDIR /app
COPY docker/tools/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt