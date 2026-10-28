"""Dimension của lớp Gold (mục 5.4 Design Doc, docs/data_model.md mục 3).

    python gold_dimensions.py reference
    python gold_dimensions.py transactions [--month 2019-03 | --from 2010-01 --to 2019-10]

reference: dim_customer, dim_card, dim_mcc từ Silver tham chiếu, dim_date sinh theo lịch.
transactions: dim_merchant từ silver.card.merchants; dim_location và dim_txn_profile thêm các vị
trí, tổ hợp (kênh, lỗi, hoàn tiền) mới có trong silver.card.transactions của các tháng đã cho
(không truyền tháng thì xét toàn bộ). gold_fact.py gọi run_transactions() trước khi dựng fact.

Mỗi dimension có khóa thay thế ổn định và hai dòng −1 Unknown, −2 Deleted; ghi bằng MERGE INTO
theo khóa, chỉ các dòng thay đổi, nên chạy lại khi Silver không đổi thì không ghi gì.
"""

import argparse
import sys
import time

from pyspark.sql import functions as F

from common import helpers as h
from common import lakehouse as lh
from transform import gold_transforms as gt

DIMS = {
    # tên: (khóa, khóa tự nhiên, DDL các cột nghiệp vụ)
    "customer": ("customer_key", ["customer_id"], """
        customer_key INT, customer_id INT, gender STRING, birth_year INT, yearly_income DECIMAL(12,2),
        income_band STRING, total_debt DECIMAL(12,2), debt_to_income DECIMAL(6,3), credit_score INT,
        credit_score_band STRING, num_credit_cards INT, latitude DOUBLE, longitude DOUBLE"""),
    "card": ("card_key", ["card_id"], """
        card_key INT, card_id INT, customer_id INT, card_brand STRING, card_type STRING, card_token STRING,
        card_last4 STRING, has_chip BOOLEAN, credit_limit DECIMAL(12,2), expires_date DATE,
        acct_open_date DATE, num_cards_issued INT, year_pin_last_changed INT"""),
    "merchant": ("merchant_key", ["merchant_id"], """
        merchant_key INT, merchant_id INT, merchant_city STRING, merchant_state STRING, zip STRING,
        is_online BOOLEAN, is_foreign BOOLEAN"""),
    "location": ("location_key", gt.LOCATION_NK, """
        location_key INT, merchant_city STRING, merchant_state STRING, zip STRING, is_online BOOLEAN,
        is_foreign BOOLEAN"""),
    "mcc": ("mcc_key", ["mcc_code"], """
        mcc_key INT, mcc_code INT, description STRING, mcc_group STRING"""),
    "date": ("date_key", ["date_key"], """
        date_key INT, full_date DATE, day_of_week INT, day_name STRING, week_of_year INT, month INT,
        quarter INT, year INT, is_weekend BOOLEAN"""),
    "txn_profile": ("txn_profile_key", gt.PROFILE_NK, """
        txn_profile_key INT, channel STRING, error_combo STRING, has_error BOOLEAN, is_refund BOOLEAN"""),
}
REFERENCE_DIMS = ("customer", "card", "mcc", "date")


def write_dim(spark, name, src, batch_id, keep_existing=False):
    """Đồng bộ một dimension với src (các cột nghiệp vụ, chưa có khóa).

    keep_existing: src chỉ gồm các khóa tự nhiên của một phần dữ liệu (vài tháng giao dịch), các
    dòng đã có mà không nằm trong src được giữ nguyên. Không có thì dim trùng src (SCD loại 1).
    """
    t0 = time.time()
    key, nk, ddl = DIMS[name]
    table = h.GOLD_DIMS[name]
    lh.ensure_silver_table(spark, table, ddl)
    current = spark.table(table)
    keyed = src if nk == [key] else gt.assign_keys(src, current, key, nk)
    if keep_existing:
        kept = (current.where(F.col(key) > 0).join(keyed.select(key), key, "left_anti")
                       .select(*keyed.columns))
        keyed = keyed.unionByName(kept)
    rows = keyed.unionByName(gt.special_rows(keyed, key)).withColumn("_batch_id", F.lit(batch_id))
    counts = lh.merge_changes(spark, rows, table, key, [c for c in keyed.columns if c != key])
    total = spark.table(table).count()
    print(f"[gold] {table}: {total} dòng (gồm −1, −2); thêm {counts['I']}, sửa {counts['U']}, "
          f"xóa {counts['D']}; {time.time() - t0:.1f}s")


def run_reference(spark):
    write_dim(spark, "customer", gt.dim_customer(spark.table(h.SILVER_CUSTOMERS)),
              lh.require_bronze(spark, h.SRC_CUSTOMERS))
    write_dim(spark, "card", gt.dim_card(spark.table(h.SILVER_CARDS)), lh.require_bronze(spark, h.SRC_CARDS))
    write_dim(spark, "mcc", gt.dim_mcc(spark.table(h.SILVER_MCC)), lh.require_bronze(spark, h.SRC_MCC))
    write_dim(spark, "date", gt.dim_date(spark), "calendar")


def run_transactions(spark, months=None):
    """dim_merchant, và vị trí, tổ hợp giao dịch mới của các tháng (None: toàn bộ Silver)."""
    txn = spark.table(h.SILVER_TXN)
    if months:
        ts = F.col("transaction_ts")
        start, end = h.month_bounds(months[0])[0], h.month_bounds(months[-1])[1]
        txn = txn.where((ts >= F.lit(start).cast("timestamp_ntz")) & (ts < F.lit(end).cast("timestamp_ntz")))
        batch_id = months[-1]
    else:
        batch_id = txn.agg(F.max("_batch_id")).first()[0]
    write_dim(spark, "merchant", gt.dim_merchant(spark.table(h.SILVER_MERCHANTS)), batch_id)
    write_dim(spark, "location", gt.locations(txn), batch_id, keep_existing=True)
    write_dim(spark, "txn_profile", gt.txn_profiles(txn), batch_id, keep_existing=True)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("reference", help="dim_customer, dim_card, dim_mcc, dim_date")
    t = sub.add_parser("transactions", help="dim_merchant, dim_location, dim_txn_profile")
    h.add_month_args(t)
    args = ap.parse_args()
    months = None
    if args.cmd == "transactions" and (args.month or args.first or args.last):
        try:
            months = h.resolve_months(args)
        except ValueError as e:
            ap.error(str(e))

    spark = lh.get_spark(f"gold_dimensions_{args.cmd}")
    try:
        if args.cmd == "reference":
            run_reference(spark)
        else:
            run_transactions(spark, months)
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
