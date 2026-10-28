"""Bronze → Silver cho giao dịch, theo từng tháng (mục 5.3 và 6 Design Doc).

    python silver_transactions.py --month 2019-03
    python silver_transactions.py --from 2010-01 --to 2019-10

Với mỗi tháng M:
1. Chỉ đọc bronze.card.transactions_raw của M nếu lần nạp Bronze gần nhất của M là SUCCESS.
2. Tách bản ghi vi phạm rule BLOCK (R01–R03, kể cả id đã có ở tháng khác trong Silver) vào
   ops.pipeline.quarantine_transactions (ghi đè phần của M).
3. MERGE INTO silver.card.transactions trong phạm vi tháng M, chỉ ghi các dòng thay đổi.
4. Ghi ops.pipeline.dq_results: số bản ghi bị cách ly theo R01–R03 và đối soát R10.2
   (Bronze M = Silver M + quarantine M); lệch thì báo lỗi.
Sau các tháng, tính lại silver.card.merchants trong cùng session (bỏ qua bằng --skip-merchants).
"""

import argparse
import sys
import time
from datetime import datetime, timezone

from pyspark.sql import functions as F

from common import helpers as h
from common import lakehouse as lh
from quality import reconcile
from quality import rules as dq
from transform import silver_merchants
from transform import silver_transforms as st

SILVER = h.SILVER_TXN
QUARANTINE = h.QUARANTINE

SILVER_DDL = """
    transaction_id BIGINT, transaction_ts TIMESTAMP_NTZ, customer_id INT, card_id INT,
    amount DECIMAL(12,2), channel STRING, merchant_id INT, merchant_city STRING,
    merchant_state STRING, zip STRING, mcc_code INT, error_list ARRAY<STRING>, has_error BOOLEAN,
    is_refund BOOLEAN, is_online BOOLEAN, is_foreign BOOLEAN"""


def ensure_tables(spark):
    lh.ensure_silver_table(spark, SILVER, SILVER_DDL, partition="months(transaction_ts)")
    lh.ensure_bronze_table(spark, QUARANTINE, h.TXN_COLS, partition="_batch_id",
                           extra_ddl="rule_id STRING, reason STRING, run_id STRING, quarantined_at TIMESTAMP")


def process_month(spark, rid, month, statuses, block_ratio):
    t0 = time.time()
    lh.require_bronze(spark, h.SRC_TXN, month, statuses)
    bronze = spark.table(h.BRONZE_TXN).where(F.col("_batch_id") == month)
    scope = h.month_scope(month)
    other_ids = spark.table(SILVER).where(f"NOT ({scope.format(t='')})").select("transaction_id")
    # Tính rule một lần rồi cache: cả phần ghi quarantine lẫn MERGE đều dùng lại
    marked = st.mark_batch(bronze, month, other_ids).persist()
    bronze_n = marked.count()
    valid, quarantine = st.split_marked(marked, bronze.columns)

    # Ghi đè theo điều kiện (không dùng overwritePartitions): khi tháng M không còn bản ghi lỗi,
    # DataFrame rỗng vẫn phải xóa được các bản cách ly cũ của M. Không có gì cũ lẫn mới thì
    # bỏ qua, để không tạo snapshot rỗng.
    in_month = F.col("_batch_id") == month
    if quarantine.limit(1).count() or spark.table(QUARANTINE).where(in_month).limit(1).count():
        (quarantine.withColumn("run_id", F.lit(rid))
                   .withColumn("quarantined_at", F.lit(datetime.now(timezone.utc)))
                   .writeTo(QUARANTINE).overwrite(in_month))

    src = st.transactions(valid).withColumn("_batch_id", F.lit(month))
    cols = [c for c in st.TXN_COLS if c != "transaction_id"]
    counts = lh.merge_changes(spark, src, SILVER, "transaction_id", cols, scope=scope)
    marked.unpersist()

    silver_n, q_n, status = reconcile.bronze_silver(spark, rid, month, block_ratio, bronze_n)
    msg = (f"{month}: bronze {bronze_n}, silver {silver_n}, quarantine {q_n}; "
           f"thêm {counts['I']}, sửa {counts['U']}, xóa {counts['D']}; {time.time() - t0:.1f}s")
    if status["R10.2"] == "BLOCK":
        raise RuntimeError(f"R10 lệch – {msg}")
    print(f"[silver] {msg}")
    return q_n


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    h.add_month_args(ap)
    ap.add_argument("--skip-merchants", action="store_true", help="không tính lại silver.card.merchants")
    dq.add_block_ratio_arg(ap)
    args = ap.parse_args()
    try:
        months = h.resolve_months(args)
    except ValueError as e:
        ap.error(str(e))

    spark = lh.get_spark("silver_transactions")
    try:
        ensure_tables(spark)
        rid, t0 = lh.run_id(), time.time()
        statuses = lh.latest_status(spark, h.SRC_TXN)   # đọc ingestion_log một lần cho mọi tháng
        quarantined = sum(process_month(spark, rid, m, statuses, args.block_ratio) for m in months)
        print(f"Đã dựng Silver {len(months)} tháng ({months[0]} → {months[-1]}), "
              f"cách ly {quarantined} dòng, {time.time() - t0:.0f}s")
        if not args.skip_merchants:
            silver_merchants.run(spark, rid, months[-1])
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
