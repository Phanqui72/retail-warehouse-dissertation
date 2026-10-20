# Spark 3.5.3 + thư viện cần cho lakehouse, tải SẴN lúc build (không phải tải lại mỗi lần chạy job).
FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root
# Iceberg cho Spark (đọc/ghi bảng Iceberg qua REST catalog)
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.1/iceberg-spark-runtime-3.5_2.12-1.6.1.jar /opt/spark/jars/
# S3FileIO của Iceberg (ghi file Parquet/metadata lên MinIO)
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.6.1/iceberg-aws-bundle-1.6.1.jar /opt/spark/jars/
# s3a:// — để Spark đọc file CSV thô ở bucket landing (khớp Hadoop 3.3.4 có sẵn trong Spark 3.5.3)
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/
RUN chmod 644 /opt/spark/jars/*.jar

ENV PYSPARK_PYTHON=python3
WORKDIR /opt/jobs