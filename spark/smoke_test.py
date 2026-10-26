"""Smoke test tuần 2: Spark (qua Spark Connect) ghi bảng Iceberg qua Lakekeeper → Trino đọc cùng bảng.

    python /opt/airflow/spark/smoke_test.py write   # tạo namespace + ghi bảng
    python /opt/airflow/spark/smoke_test.py check   # đọc lại, kể cả dòng do Trino ghi
"""

import sys
import time

from pyspark.sql import SparkSession

NAMESPACES = ["bronze", "silver", "gold", "ops"]
TABLE = "lakehouse.ops.smoke_test"


def write(spark):
    for ns in NAMESPACES:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS lakehouse.{ns}")
    # DROP + CREATE thay vì CREATE OR REPLACE: với Lakekeeper, REPLACE chỉ có DDL
    # không xóa dữ liệu cũ (xem docs/week2.md)
    spark.sql(f"DROP TABLE IF EXISTS {TABLE}")
    spark.sql(f"""
        CREATE TABLE {TABLE} (
            id BIGINT, writer STRING, created_at TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (days(created_at))
        TBLPROPERTIES ('format-version' = '2')
    """)
    spark.sql(f"""
        INSERT INTO {TABLE} VALUES
            (1, 'spark', TIMESTAMP '2019-03-01 08:00:00'),
            (2, 'spark', TIMESTAMP '2019-03-02 09:30:00'),
            (3, 'spark', TIMESTAMP '2019-03-03 18:45:00')
    """)


def check(spark):
    rows = spark.sql(f"SELECT writer, count(*) AS n FROM {TABLE} GROUP BY writer ORDER BY writer").collect()
    counts = {r.writer: r.n for r in rows}
    print(f"Spark đọc {TABLE}: {counts}")
    assert counts.get("spark") == 3, "thiếu dòng do Spark ghi"
    assert counts.get("trino", 0) >= 1, "chưa thấy dòng do Trino ghi"


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "write"
    t0 = time.time()
    spark = SparkSession.builder.appName(f"smoke_test_{mode}").getOrCreate()
    t_start = time.time() - t0
    {"write": write, "check": check}[mode](spark)
    spark.sql(f"SELECT snapshot_id, operation, committed_at FROM {TABLE}.snapshots ORDER BY committed_at").show(truncate=False)
    print(f"[{mode}] Spark {spark.version}: khởi động {t_start:.1f}s, tổng {time.time() - t0:.1f}s")
    spark.stop()


if __name__ == "__main__":
    main()
