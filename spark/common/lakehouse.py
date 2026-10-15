"""Phần dùng Spark chung: session, DDL, đọc JDBC, checksum, ops.ingestion_log, MERGE cho Silver."""

import os
import time
import traceback
from datetime import datetime, timezone
from functools import reduce
from operator import and_

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

LOG_TABLE = "lakehouse.ops.ingestion_log"


def get_spark(app):
    spark = SparkSession.builder.appName(app).getOrCreate()
    for ns in ("bronze", "silver", "ops"):
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


def ensure_silver_table(spark, table, cols_ddl, partition=None):
    """Bảng Silver: các cột nghiệp vụ đã ép kiểu, thêm _batch_id và _updated_at (data_model.md mục 2)."""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            {cols_ddl},
            _batch_id STRING, _updated_at TIMESTAMP
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


def latest_status(spark, source):
    """{batch_id: trạng thái lần nạp gần nhất} của một nguồn trong ops.ingestion_log."""
    rows = spark.sql(f"""
        SELECT batch_id, max_by(status, finished_at) AS status
        FROM {LOG_TABLE} WHERE source = '{source}' GROUP BY batch_id
    """).collect()
    return {r.batch_id: r.status for r in rows}


def loaded_batches(spark, source):
    """Các batch của nguồn có lần nạp gần nhất thành công."""
    return {b for b, st in latest_status(spark, source).items() if st == "SUCCESS"}


def require_bronze(spark, source, batch_id=None):
    """Silver chỉ đọc Bronze đã nạp thành công. Không truyền batch_id thì xét lần nạp gần
    nhất của nguồn (bảng tham chiếu ghi đè toàn bảng). Trả về batch_id đã kiểm tra."""
    if batch_id is None:
        row = spark.sql(f"""
            SELECT max_by(batch_id, finished_at) AS batch_id, max_by(status, finished_at) AS status
            FROM {LOG_TABLE} WHERE source = '{source}'
        """).first()
        batch_id, status = row.batch_id, row.status
    else:
        status = latest_status(spark, source).get(batch_id)
    if status != "SUCCESS":
        raise RuntimeError(f"Bronze {source} {batch_id or ''}: lần nạp gần nhất là {status or 'chưa nạp'}, "
                           "không dựng Silver")
    return batch_id


def merge_changes(spark, src, table, key, cols, scope=None):
    """Đồng bộ bảng Silver với src bằng MERGE INTO, chỉ ghi các dòng thực sự thay đổi.

    src có cột key, cols và _batch_id. scope là điều kiện SQL trên bảng đích, viết với tiền tố
    {t} trước tên cột (VD "{t}transaction_ts >= …"), để giới hạn phạm vi đồng bộ và để Iceberg
    chỉ quét các partition liên quan; None là cả bảng. Trong phạm vi đó, sau khi chạy bảng đích
    trùng src: dòng mới → INSERT, dòng khác → UPDATE, dòng không còn trong src → DELETE.

    Phần chênh lệch được tính trước (full outer join src với dữ liệu hiện có), nên chạy lại khi
    không có gì thay đổi thì không ghi gì, không tạo snapshot. Trả về {"I": n, "U": n, "D": n}.
    """
    src = src.persist()   # src thường là phép tính nặng (VD merchants): chỉ tính một lần
    n, n_key, n_distinct = src.agg(F.count(F.lit(1)), F.count(key), F.countDistinct(key)).first()
    if not n == n_key == n_distinct:
        src.unpersist()
        raise ValueError(f"{table}: src có {n - n_key} dòng {key} NULL, {n_key - n_distinct} dòng trùng {key}")
    cur = spark.table(table)
    if scope:
        cur = cur.where(scope.format(t=""))
    s, c = src.alias("s"), cur.select(key, *cols).alias("c")
    same = reduce(and_, [F.col(f"s.{x}").eqNullSafe(F.col(f"c.{x}")) for x in cols])
    op = (F.when(F.col(f"c.{key}").isNull(), "I")
           .when(F.col(f"s.{key}").isNull(), "D")
           .when(~same, "U"))
    diff = (s.join(c, F.col(f"s.{key}") == F.col(f"c.{key}"), "full_outer")
             .select(F.coalesce(F.col(f"s.{key}"), F.col(f"c.{key}")).alias(key),
                     *[F.col(f"s.{x}").alias(x) for x in cols + ["_batch_id"]],
                     op.alias("_op"))
             .where(F.col("_op").isNotNull())
             .withColumn("_updated_at", F.current_timestamp())
             .persist())
    counts = {"I": 0, "U": 0, "D": 0}
    counts.update({r["_op"]: r["count"] for r in diff.groupBy("_op").count().collect()})
    if any(counts.values()):
        diff.createOrReplaceTempView("_changes")
        all_cols = [key, *cols, "_batch_id", "_updated_at"]
        on = f"t.{key} = s.{key}" + (f" AND {scope.format(t='t.')}" if scope else "")
        spark.sql(f"""
            MERGE INTO {table} t USING _changes s
            ON {on}
            WHEN MATCHED AND s._op = 'D' THEN DELETE
            WHEN MATCHED AND s._op = 'U' THEN
                UPDATE SET {", ".join(f"{x} = s.{x}" for x in all_cols[1:])}
            WHEN NOT MATCHED AND s._op = 'I' THEN
                INSERT ({", ".join(all_cols)}) VALUES ({", ".join(f"s.{x}" for x in all_cols)})
        """)
    diff.unpersist()
    src.unpersist()
    return counts


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
