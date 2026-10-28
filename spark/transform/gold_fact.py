"""Fact của lớp Gold theo từng tháng (mục 5.4 Design Doc, docs/data_model.md mục 3).

    python gold_fact.py --month 2019-03
    python gold_fact.py --from 2010-01 --to 2019-10

1. Dừng nếu có tháng mà một rule từ nguồn đến Silver đang BLOCK trong ops.pipeline.dq_results
   (mục 4.1: chỉ dựng Gold khi không có rule BLOCK), hoặc dimension tham chiếu chưa được dựng.
2. Cập nhật dim_merchant, dim_location, dim_txn_profile theo giao dịch của các tháng, trong cùng
   session (gold_dimensions.run_transactions).
3. Mỗi tháng M: tra khóa mọi dimension cho giao dịch Silver của M rồi ghi đè partition của M
   trong gold.card.fact_transaction (overwritePartitions), nên chạy lại không nhân đôi.
"""

import argparse
import sys
import time

from pyspark.sql import functions as F

from common import helpers as h
from common import lakehouse as lh
from quality import rules as dq
from transform import gold_dimensions
from transform import gold_transforms as gt

FACT_DDL = """
    transaction_id BIGINT, date_key INT, customer_key INT, card_key INT, merchant_key INT,
    location_key INT, mcc_key INT, txn_profile_key INT, transaction_ts TIMESTAMP_NTZ, txn_hour INT,
    customer_age_at_txn INT, amount DECIMAL(12,2), batch_id STRING"""


def ensure_fact(spark):
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {h.GOLD_FACT} ({FACT_DDL}
        ) USING iceberg
        PARTITIONED BY (months(transaction_ts))
        TBLPROPERTIES ('format-version' = '2')
    """)


def check_ready(spark, months):
    blocked = dq.blocked_batches(spark, months, dq.SILVER_RULES)
    if blocked:
        raise RuntimeError("rule BLOCK, không dựng Gold: "
                           + "; ".join(f"{m}: {', '.join(r)}" for m, r in blocked.items()))
    for name in gold_dimensions.REFERENCE_DIMS:
        key = gold_dimensions.DIMS[name][0]
        if not spark.catalog.tableExists(h.GOLD_DIMS[name]) or \
                not spark.table(h.GOLD_DIMS[name]).where(F.col(key) > 0).limit(1).count():
            raise RuntimeError(f"{h.GOLD_DIMS[name]} chưa có dữ liệu (make gold-ref)")


def process_month(spark, month, dims):
    t0 = time.time()
    scope = h.month_scope(month).format(t="")
    txn = spark.table(h.SILVER_TXN).where(scope)
    if not txn.limit(1).count():
        raise RuntimeError(f"{h.SILVER_TXN} không có giao dịch tháng {month}")
    gt.fact(txn, dims).writeTo(h.GOLD_FACT).overwritePartitions()
    row = spark.table(h.GOLD_FACT).where(scope).agg(
        F.count(F.lit(1)).alias("n"), *[F.count_if(F.col(k) < 0).alias(k) for k in gt.KEYS]).first()
    unknown = ", ".join(f"{k} {row[k]}" for k in gt.KEYS if row[k])
    print(f"[gold] {month}: fact {row.n} dòng; khóa −1: {unknown or 'không có'}; {time.time() - t0:.1f}s")
    return row.n


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    h.add_month_args(ap)
    args = ap.parse_args()
    try:
        months = h.resolve_months(args)
    except ValueError as e:
        ap.error(str(e))

    spark = lh.get_spark("gold_fact")
    try:
        t0 = time.time()
        check_ready(spark, months)
        ensure_fact(spark)
        gold_dimensions.run_transactions(spark, months)
        dims = {name: spark.table(table).persist() for name, table in h.GOLD_DIMS.items()}
        n = sum(process_month(spark, m, dims) for m in months)
        for d in dims.values():
            d.unpersist()
        print(f"Đã dựng fact {len(months)} tháng ({months[0]} → {months[-1]}), {n} dòng, "
              f"{time.time() - t0:.0f}s")
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
