"""Phần dùng Spark cho lớp Bronze: session, DDL, đọc JDBC, checksum, ops.ingestion_log."""

import os
import time
import traceback
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

LOG_TABLE = "lakehouse.ops.ingestion_log"


def get_spark(app):
    spark = SparkSession.builder.appName(app).getOrCreate()
    for ns in ("bronze", "ops"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS lakehouse.{ns}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {LOG_TABLE} (
            run_id STRING, source STRING, batch_id STRING,
            source_count BIGINT, row_count BIGINT, checksum STRING,
            status STRING, message STRING,
            started_at TIMESTAMP, finished_at TIMESTAMP
        ) USING iceberg
        TBLPROPERTIES ('format-version' = '2')
    """)
    return spark


def ensure_bronze_table(spark, table, cols, partition=None):
    """Bảng Bronze: mọi cột nghiệp vụ là STRING, thêm 3 cột metadata.

    Chỉ CREATE IF NOT EXISTS: không dùng REPLACE chỉ có DDL vì với Lakekeeper
    nó không xóa dữ liệu cũ (docs/week2.md).
    """
    body = ",\n            ".join(f"`{c}` STRING" for c in cols)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            {body},
            _ingested_at TIMESTAMP, _source STRING, _batch_id STRING
        ) USING iceberg
        {f"PARTITIONED BY ({partition})" if partition else ""}
        TBLPROPERTIES ('format-version' = '2')
    """)


def with_metadata(df, source, batch_id):
    return (df.withColumn("_ingested_at", F.current_timestamp())
              .withColumn("_source", F.lit(source))
              .withColumn("_batch_id", F.lit(batch_id)))


def read_jdbc(spark, sql):
    """Đọc core_card qua JDBC; thông tin kết nối lấy từ biến môi trường của container."""
    return (spark.read.format("jdbc")
            .option("url", os.environ["SOURCE_JDBC_URL"])
            .option("user", os.environ["SOURCE_DB_USER"])
            .option("password", os.environ["SOURCE_DB_PASSWORD"])
            .option("driver", "org.postgresql.Driver")
            .option("query", sql)
            .option("fetchsize", 10000)
            .load())


def count_jdbc(spark, table, where=""):
    sql = f"SELECT count(*) AS n FROM {table}" + (f" WHERE {where}" if where else "")
    return int(read_jdbc(spark, sql).first()["n"])


def checksum(df, cols):
    """Tổng xxhash64 các cột nghiệp vụ: không phụ thuộc thứ tự dòng, dùng để so hai lần nạp."""
    row = df.agg(F.count(F.lit(1)).alias("n"),
                 F.sum(F.xxhash64(*cols).cast("decimal(38,0)")).alias("s")).first()
    return int(row["n"]), str(row["s"] or 0)


def run_id():
    # BashOperator của Airflow đặt sẵn biến này; chạy tay thì sinh theo thời gian
    return os.environ.get("AIRFLOW_CTX_DAG_RUN_ID") or f"manual__{datetime.now(timezone.utc):%Y%m%dT%H%M%S}"


def loaded_batches(spark, source):
    """Các batch của nguồn có lần nạp gần nhất thành công."""
    rows = spark.sql(f"""
        SELECT batch_id, max_by(status, finished_at) AS status
        FROM {LOG_TABLE} WHERE source = '{source}' GROUP BY batch_id
    """).collect()
    return {r.batch_id for r in rows if r.status == "SUCCESS"}


LOG_SCHEMA = T.StructType([
    T.StructField("run_id", T.StringType()),
    T.StructField("source", T.StringType()),
    T.StructField("batch_id", T.StringType()),
    T.StructField("source_count", T.LongType()),
    T.StructField("row_count", T.LongType()),
    T.StructField("checksum", T.StringType()),
    T.StructField("status", T.StringType()),
    T.StructField("message", T.StringType()),
    T.StructField("started_at", T.TimestampType()),
    T.StructField("finished_at", T.TimestampType()),
])


def run_batch(spark, rid, source, batch_id, load, done=frozenset(), force=False):
    """Nạp một batch có ghi ops.ingestion_log.

    load() ghi dữ liệu và trả về (source_count, row_count, checksum). Batch đã SUCCESS thì bỏ
    qua, trừ khi force. Lệch số dòng nguồn/Bronze (R10) thì ghi FAILED và báo lỗi.
    Trả về True nếu đã nạp, False nếu bỏ qua.
    """
    if batch_id in done and not force:
        print(f"[skip] {source} {batch_id}: đã nạp thành công (dùng --force để nạp lại)")
        return False
    started, t0 = datetime.now(timezone.utc), time.time()
    src_n = row_n = None
    cks = None
    try:
        src_n, row_n, cks = load()
        if src_n != row_n:
            raise RuntimeError(f"R10: nguồn {src_n} dòng, Bronze {row_n} dòng")
        status, msg = "SUCCESS", None
    except Exception as e:
        status, msg = "FAILED", "".join(traceback.format_exception_only(e)).strip()[:1000]
        raise
    finally:
        row = (rid, source, batch_id, src_n, row_n, cks, status, msg,
               started, datetime.now(timezone.utc))
        spark.createDataFrame([row], LOG_SCHEMA).writeTo(LOG_TABLE).append()
        print(f"[{status.lower()}] {source} {batch_id}: nguồn {src_n}, bronze {row_n}, "
              f"checksum {cks}, {time.time() - t0:.1f}s")
    return True
