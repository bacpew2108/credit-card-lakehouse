# Airflow + Java + PySpark: mỗi task gọi spark-submit --master local[*] trong container (mục 8).
FROM apache/airflow:3.3.1

ARG PYSPARK_VERSION=4.0.4
ARG ICEBERG_VERSION=1.10.1
ARG PG_JDBC_VERSION=42.7.8

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jre-headless \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" "pyspark==${PYSPARK_VERSION}"

# Jar Iceberg (cùng bản 1.10.1 với Trino 480), AWS bundle cho S3FileIO, JDBC PostgreSQL; kiểm tra SHA-1
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
