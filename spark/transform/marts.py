"""6 bảng mart của lớp Gold (mục 5.4 Design Doc, docs/data_model.md mục 3).

    python marts.py --month 2019-03
    python marts.py --from 2010-01 --to 2019-10

Tổng hợp từ gold.card.fact_transaction của các tháng đã cho join các dimension. Mỗi mart partition
theo month (ngày đầu tháng) và được ghi đè bằng điều kiện trên month, khớp trọn các partition của
các tháng đó, nên chạy lại không nhân đôi. Khác overwritePartitions (chỉ thay partition có trong dữ
liệu mới), tháng không còn dòng nào của một mart (VD mart_error_monthly của tháng không có lỗi)
cũng được xóa. Khoảng nhiều tháng được tính và ghi trong một lượt cho mỗi mart.
"""

import argparse
import sys
import time

from pyspark.sql import functions as F

from common import helpers as h
from common import lakehouse as lh
from transform import gold_transforms as gt


def ensure_mart(spark, table, cols):
    body = ", ".join(f"{c} {t}" for c, t in cols)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table} ({body}
        ) USING iceberg
        PARTITIONED BY (month)
        TBLPROPERTIES ('format-version' = '2')
    """)


def run(spark, months):
    ts = F.col("transaction_ts")
    start, end = h.month_bounds(months[0])[0], h.month_bounds(months[-1])[1]
    # Không cache fact: cả 118 tháng là 13 triệu dòng, đọc lại từ Iceberg cho từng mart vẫn nhanh
    fact = (spark.table(h.GOLD_FACT)
            .where((ts >= F.lit(start).cast("timestamp_ntz")) & (ts < F.lit(end).cast("timestamp_ntz"))))
    if not fact.limit(1).count():
        raise RuntimeError(f"{h.GOLD_FACT} chưa có dữ liệu {months[0]} → {months[-1]} (make gold)")
    in_months = (F.col("month") >= F.lit(start).cast("date")) & (F.col("month") < F.lit(end).cast("date"))
    dims = {name: spark.table(table).persist() for name, table in h.GOLD_DIMS.items()}
    for name, (build, cols) in gt.MARTS.items():
        t0 = time.time()
        table = h.MARTS[name]
        ensure_mart(spark, table, cols)
        build(fact, dims).select(*[c for c, _ in cols]).writeTo(table).overwrite(in_months)
        n = spark.table(table).where(in_months).count()
        print(f"[gold] {table}: {n} dòng cho {len(months)} tháng; {time.time() - t0:.1f}s")
    for df in dims.values():
        df.unpersist()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    h.add_month_args(ap)
    args = ap.parse_args()
    try:
        months = h.resolve_months(args)
    except ValueError as e:
        ap.error(str(e))

    spark = lh.get_spark("marts")
    try:
        t0 = time.time()
        run(spark, months)
        print(f"Đã dựng 6 mart {len(months)} tháng ({months[0]} → {months[-1]}), {time.time() - t0:.0f}s")
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
