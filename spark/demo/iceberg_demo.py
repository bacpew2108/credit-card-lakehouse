"""Phần chạy trên Spark của bộ demo 8 kịch bản mục 11.1 Design Doc (sql/run_demo.py gọi qua docker exec).

    python iceberg_demo.py sql "CALL gold.system.rollback_to_snapshot('card.fact_transaction', 123)" …
    python iceberg_demo.py gold-rewrite --from 2010-01 --to 2019-10
    python iceberg_demo.py objects gold.card.fact_transaction

sql: chạy lần lượt từng câu Spark SQL, in kết quả mỗi câu thành một dòng "[json] {…}".
gold-rewrite: dựng lại fact của cả khoảng tháng từ Silver bằng đúng logic của gold_fact.py nhưng ghi
trong MỘT commit (overwritePartitions), để kịch bản ACID có một lần ghi đủ dài mà dừng giữa chừng.
Dữ liệu ghi ra trùng dữ liệu đang có, nên chạy hết cũng không làm đổi số liệu.
objects: liệt kê object trên MinIO dưới thư mục của bảng (tài khoản chỉ đọc pan-scanner, không qua
catalog) rồi so với file mà metadata tham chiếu: file của snapshot hiện tại, file của mọi snapshot
còn giữ, và file mồ côi (có trên MinIO nhưng không snapshot nào dùng).
"""

import argparse
import json
import sys
import time
from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import functions as F

from common import helpers as h
from common import lakehouse as lh
from transform import gold_transforms as gt


def emit(obj):
    def conv(v):
        if isinstance(v, Decimal):
            return str(v)
        if isinstance(v, (date, datetime)):
            return v.isoformat()
        return str(v)
    print("[json] " + json.dumps(obj, default=conv, ensure_ascii=False), flush=True)


def run_sql(spark, statements):
    for stmt in statements:
        t0 = time.time()
        df = spark.sql(stmt)
        rows = df.limit(200).collect()
        emit({"sql": stmt, "columns": df.columns, "rows": [list(r) for r in rows],
              "seconds": round(time.time() - t0, 2)})


def gold_rewrite(spark, months):
    start, end = h.month_bounds(months[0])[0], h.month_bounds(months[-1])[1]
    ts = F.col("transaction_ts")
    txn = spark.table(h.SILVER_TXN).where((ts >= F.lit(start).cast("timestamp_ntz"))
                                          & (ts < F.lit(end).cast("timestamp_ntz")))
    dims = {name: spark.table(table) for name, table in h.GOLD_DIMS.items()}
    print(f"[demo] bắt đầu ghi fact {h.batch_label(months)} trong một commit", flush=True)
    t0 = time.time()
    gt.fact(txn, dims).writeTo(h.GOLD_FACT).overwritePartitions()
    print(f"[demo] đã commit, {time.time() - t0:.0f}s", flush=True)


def location(spark, table):
    rows = spark.sql(f"DESCRIBE TABLE EXTENDED {table}").collect()
    return next(r.data_type for r in rows if r.col_name == "Location").rstrip("/")


def objects(spark, table):
    from quality import pan_scan
    loc = location(spark, table)
    bucket, prefix = loc.removeprefix("s3://").split("/", 1)
    s3, objs = pan_scan.s3_client(), {}
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix + "/"):
        objs.update({f"s3://{bucket}/{o['Key']}": o["Size"] for o in page.get("Contents", [])})
    current = {r.file_path for r in spark.table(f"{table}.files").select("file_path").collect()}
    any_snap = {r.file_path for t in ("all_data_files", "all_delete_files")
                for r in spark.table(f"{table}.{t}").select("file_path").distinct().collect()}
    data = {p: n for p, n in objs.items() if p.startswith(f"{loc}/data/")}
    orphans = {p: n for p, n in data.items() if p not in any_snap}
    emit({"table": table, "location": loc, "objects": len(objs), "bytes": sum(objs.values()),
          "data_objects": len(data), "data_bytes": sum(data.values()),
          "current_files": len(current), "snapshot_files": len(any_snap),
          "orphans": len(orphans), "orphan_bytes": sum(orphans.values()),
          "snapshots": spark.table(f"{table}.snapshots").count()})


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("sql")
    s.add_argument("statements", nargs="+")
    g = sub.add_parser("gold-rewrite")
    h.add_month_args(g)
    o = sub.add_parser("objects")
    o.add_argument("tables", nargs="+")
    args = ap.parse_args()
    months = None
    if args.cmd == "gold-rewrite":
        try:
            months = h.resolve_months(args)
        except ValueError as e:
            ap.error(str(e))

    spark = lh.get_spark(f"demo_{args.cmd}")
    try:
        if args.cmd == "sql":
            run_sql(spark, args.statements)
        elif args.cmd == "gold-rewrite":
            gold_rewrite(spark, months)
        else:
            for t in args.tables:
                objects(spark, t)
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
