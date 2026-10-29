# Image chung cho spark-master, spark-worker và spark-connect (driver), mục 8.
# PYTHON_VERSION, PYSPARK_VERSION lấy từ x-spark-build-args trong docker-compose.yml (dùng chung với
# image Airflow): UDF Python được pickle ở client rồi chạy trên executor nên hai bên phải khớp.
# Ghim digest (bản đa kiến trúc): Docker Hub phát hành lại tag python:3.13-slim-bookworm nhiều lần mỗi
# tháng, tag trôi thì mất cache build và phải tải lại Java, PySpark (~430 MB). Đổi PYTHON_VERSION thì
# lấy digest mới: docker buildx imagetools inspect python:<bản>-slim-bookworm
ARG PYTHON_VERSION
FROM python:${PYTHON_VERSION}-slim-bookworm@sha256:e32be0e55814a08977501ca863271d9bd02443710db8f2afaf8372e2c4508530

RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jre-headless curl procps \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

ARG PYSPARK_VERSION
RUN pip install --no-cache-dir "pyspark==${PYSPARK_VERSION}"

# Jar Iceberg (cùng bản 1.10.1 với Trino 480), AWS bundle cho S3FileIO, JDBC PostgreSQL; kiểm tra SHA-1
ARG ICEBERG_VERSION=1.10.1
ARG PG_JDBC_VERSION=42.7.8
RUN set -eu; \
    JARS="$(python -c 'import os, pyspark; print(os.path.join(pyspark.__path__[0], "jars"))')"; \
    M=https://repo1.maven.org/maven2; \
    for p in \
      "org/apache/iceberg/iceberg-spark-runtime-4.0_2.13/${ICEBERG_VERSION}/iceberg-spark-runtime-4.0_2.13-${ICEBERG_VERSION}.jar" \
      "org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
      "org/postgresql/postgresql/${PG_JDBC_VERSION}/postgresql-${PG_JDBC_VERSION}.jar"; do \
      f="$JARS/$(basename "$p")"; \
      curl -fsSL -o "$f" "$M/$p"; \
      echo "$(curl -fsSL "$M/$p.sha1")  $f" | sha1sum -c -; \
    done

RUN useradd --create-home --uid 50000 spark && install -d -o spark /opt/spark/work
USER spark
ENV SPARK_WORKER_DIR=/opt/spark/work \
    SPARK_CONF_DIR=/opt/spark-conf
