# Airflow + pyspark-client: task chạy `python job.py`, gửi kế hoạch xử lý tới spark-connect
# qua SPARK_REMOTE (infra/spark, mục 8). PYTHON_VERSION, PYSPARK_VERSION lấy từ
# x-spark-build-args trong docker-compose.yml, cùng giá trị với image Spark.
ARG PYTHON_VERSION
FROM apache/airflow:3.3.1-python${PYTHON_VERSION}

ARG PYSPARK_VERSION
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" "pyspark-client==${PYSPARK_VERSION}"
