"""Trích xuất core_card (PostgreSQL) qua JDBC vào lớp Bronze (mục 5.2 Design Doc).

    spark-submit extract_jdbc.py transactions --month 2019-03 [--force]
    spark-submit extract_jdbc.py transactions --from 2010-01 --to 2019-10 [--force]
    spark-submit extract_jdbc.py reference [--batch-id ref_20261012] [--force]

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

TXN_TABLE = "lakehouse.bronze.transactions_raw"
CUSTOMER_TABLE = "lakehouse.bronze.customers_raw"
CARD_TABLE = "lakehouse.bronze.cards_raw"


def extract_month(spark, month):
    where = h.month_filter(month)
    src_n = lh.count_jdbc(spark, "transactions", where)
    if src_n == 0:
        raise RuntimeError(f"nguồn không có giao dịch tháng {month}")
    df = lh.with_metadata(lh.read_jdbc(spark, h.select_text_sql("transactions", where)),
                          "core_card.transactions", month)
    df.writeTo(TXN_TABLE).overwritePartitions()
    row_n, cks = lh.checksum(spark.table(TXN_TABLE).where(F.col("_batch_id") == month), h.TXN_COLS)
    return src_n, row_n, cks


def run_transactions(spark, rid, months, force):
    lh.ensure_bronze_table(spark, TXN_TABLE, h.TXN_COLS, partition="_batch_id")
    done = lh.loaded_batches(spark, "core_card.transactions")
    n = sum(lh.run_batch(spark, rid, "core_card.transactions", m,
                         lambda m=m: extract_month(spark, m), done, force)
            for m in months)
    print(f"Đã nạp {n}/{len(months)} tháng ({months[0]} → {months[-1]})")


def overwrite_table(spark, df, table, cols):
    df.writeTo(table).overwrite(F.lit(True))
    return lh.checksum(spark.table(table), cols)


def extract_customers(spark, batch_id):
    src_n = lh.count_jdbc(spark, "customers")
    df = lh.with_metadata(lh.read_jdbc(spark, h.select_text_sql("customers")),
                          "core_card.customers", batch_id)
    return (src_n, *overwrite_table(spark, df, CUSTOMER_TABLE, h.CUSTOMER_COLS))


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
    df = lh.with_metadata(df, "core_card.cards", batch_id)
    return (src_n, *overwrite_table(spark, df, CARD_TABLE, h.CARD_COLS))


def run_reference(spark, rid, batch_id, force):
    secret = h.require_secret(os.environ.get("HMAC_SECRET"))
    lh.ensure_bronze_table(spark, CUSTOMER_TABLE, h.CUSTOMER_COLS)
    lh.ensure_bronze_table(spark, CARD_TABLE, h.CARD_COLS)
    lh.run_batch(spark, rid, "core_card.customers", batch_id,
                 lambda: extract_customers(spark, batch_id),
                 lh.loaded_batches(spark, "core_card.customers"), force)
    lh.run_batch(spark, rid, "core_card.cards", batch_id,
                 lambda: extract_cards(spark, batch_id, secret),
                 lh.loaded_batches(spark, "core_card.cards"), force)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    t = sub.add_parser("transactions", help="giao dịch theo tháng → bronze.transactions_raw")
    t.add_argument("--month")
    t.add_argument("--from", dest="first")
    t.add_argument("--to", dest="last")
    t.add_argument("--force", action="store_true")
    r = sub.add_parser("reference", help="customers, cards → bronze.*_raw")
    r.add_argument("--batch-id", default=h.ref_batch_id())
    r.add_argument("--force", action="store_true")
    args = ap.parse_args()

    try:
        if args.cmd == "transactions":
            if args.month and not (args.first or args.last):
                months = [f"{h.parse_month(args.month):%Y-%m}"]
            elif args.first and args.last and not args.month:
                months = h.iter_months(args.first, args.last)
            else:
                ap.error("dùng --month M hoặc --from A --to B")
        elif not args.batch_id.startswith("ref_"):
            ap.error("--batch-id phải bắt đầu bằng ref_")
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
