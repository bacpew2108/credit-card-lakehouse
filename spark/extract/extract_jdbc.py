"""Trích xuất core_card (PostgreSQL) qua JDBC vào lớp Bronze (mục 5.2 Design Doc).

    python extract_jdbc.py transactions --month 2019-03 [--force]
    python extract_jdbc.py transactions --from 2010-01 --to 2019-10 [--force]
    python extract_jdbc.py reference [--batch-id ref_20261012] [--force]

Giao dịch: lọc theo date trong tháng M, ghi đè đúng partition _batch_id = M.
Tham chiếu: customers, cards ghi đè toàn bảng; số thẻ được token hóa bằng HMAC ngay tại đây,
cvv không được SELECT. PAN không bao giờ được ghi xuống warehouse.
"""

import argparse
import os
import sys

from pyspark.sql import functions as F
from pyspark.sql import types as T

from common import helpers as h
from common import lakehouse as lh

def extract_month(spark, month):
    where = h.month_filter(month)
    src_n = lh.count_jdbc(spark, "transactions", where)
    if src_n == 0:
        raise RuntimeError(f"nguồn không có giao dịch tháng {month}")
    df = lh.with_metadata(lh.read_jdbc(spark, h.select_text_sql("transactions", where)),
                          h.SRC_TXN, month)
    df.writeTo(h.BRONZE_TXN).overwritePartitions()
    row_n, cks = lh.checksum(spark.table(h.BRONZE_TXN).where(F.col("_batch_id") == month), h.TXN_COLS)
    return src_n, row_n, cks


def run_transactions(spark, rid, months, force):
    lh.ensure_bronze_table(spark, h.BRONZE_TXN, h.TXN_COLS, partition="_batch_id")
    done = lh.loaded_batches(spark, h.SRC_TXN)
    n = sum(lh.run_batch(spark, rid, h.SRC_TXN, m,
                         lambda m=m: extract_month(spark, m), done, force)
            for m in months)
    print(f"Đã nạp {n}/{len(months)} tháng ({months[0]} → {months[-1]})")


def extract_customers(spark, batch_id):
    src_n = lh.count_jdbc(spark, "customers")
    df = lh.with_metadata(lh.read_jdbc(spark, h.select_text_sql("customers")),
                          h.SRC_CUSTOMERS, batch_id)
    return (src_n, *lh.overwrite_table(spark, df, h.BRONZE_CUSTOMERS, h.CUSTOMER_COLS))


def extract_cards(spark, batch_id, secret):
    pan_udf = F.udf(lambda pan: h.protect_pan(pan, secret), T.StructType([
        T.StructField("card_token", T.StringType()),
        T.StructField("card_bin", T.StringType()),
        T.StructField("card_last4", T.StringType()),
    ]))
    src_n = lh.count_jdbc(spark, "cards")
    df = (lh.read_jdbc(spark, h.select_text_sql("cards"))
            .withColumn("_pan", pan_udf("card_number"))
            .select(*[F.col(f"_pan.{c}").alias(c) if c in ("card_token", "card_bin", "card_last4")
                      else F.col(c) for c in h.CARD_COLS]))
    assert "card_number" not in df.columns and "cvv" not in df.columns
    df = lh.with_metadata(df, h.SRC_CARDS, batch_id)
    return (src_n, *lh.overwrite_table(spark, df, h.BRONZE_CARDS, h.CARD_COLS))


def run_reference(spark, rid, batch_id, force):
    secret = h.require_secret(os.environ.get("HMAC_SECRET"))
    lh.ensure_bronze_table(spark, h.BRONZE_CUSTOMERS, h.CUSTOMER_COLS)
    lh.ensure_bronze_table(spark, h.BRONZE_CARDS, h.CARD_COLS)
    lh.run_batch(spark, rid, h.SRC_CUSTOMERS, batch_id,
                 lambda: extract_customers(spark, batch_id),
                 lh.loaded_batches(spark, h.SRC_CUSTOMERS), force)
    lh.run_batch(spark, rid, h.SRC_CARDS, batch_id,
                 lambda: extract_cards(spark, batch_id, secret),
                 lh.loaded_batches(spark, h.SRC_CARDS), force)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    t = sub.add_parser("transactions", help="giao dịch theo tháng → bronze.card.transactions_raw")
    h.add_month_args(t)
    t.add_argument("--force", action="store_true")
    r = sub.add_parser("reference", help="customers, cards → bronze.*_raw")
    r.add_argument("--batch-id", default=h.ref_batch_id())
    r.add_argument("--force", action="store_true")
    args = ap.parse_args()

    try:
        if args.cmd == "transactions":
            months = h.resolve_months(args)
        else:
            h.check_ref_batch_id(args.batch_id)
    except ValueError as e:
        ap.error(str(e))

    spark = lh.get_spark(f"extract_jdbc_{args.cmd}")
    rid = lh.run_id()
    try:
        if args.cmd == "transactions":
            run_transactions(spark, rid, months, args.force)
        else:
            run_reference(spark, rid, args.batch_id, args.force)
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
