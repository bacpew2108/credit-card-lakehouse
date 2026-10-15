"""Bronze → Silver cho bảng tham chiếu: customers, cards, mcc_codes (mục 5.3 Design Doc).

    spark-submit silver_reference.py

Chỉ đọc Bronze có lần nạp gần nhất SUCCESS. Ghi bằng MERGE INTO theo khóa, chỉ các dòng thay
đổi; chạy lại khi Bronze không đổi thì không ghi gì.
"""

import csv
import sys
from pathlib import Path

from pyspark.sql import functions as F

from common import lakehouse as lh
from transform import silver_transforms as st

MCC_GROUPS = Path(__file__).resolve().parent / "ref" / "mcc_groups.csv"
MCC_SOURCE = "s3://landing/reference/mcc_codes/mcc_codes.jsonl"

TABLES = {
    # bảng Silver: (bảng Bronze, nguồn trong ingestion_log, khóa, DDL các cột nghiệp vụ)
    "lakehouse.silver.customers": ("lakehouse.bronze.customers_raw", "core_card.customers", "customer_id", """
        customer_id INT, current_age SMALLINT, retirement_age SMALLINT, birth_year SMALLINT,
        birth_month SMALLINT, gender STRING, address STRING, latitude DOUBLE, longitude DOUBLE,
        per_capita_income DECIMAL(12,2), yearly_income DECIMAL(12,2), total_debt DECIMAL(12,2),
        debt_to_income DECIMAL(6,3), credit_score SMALLINT, num_credit_cards SMALLINT"""),
    "lakehouse.silver.cards": ("lakehouse.bronze.cards_raw", "core_card.cards", "card_id", """
        card_id INT, customer_id INT, card_brand STRING, card_type STRING, card_token STRING,
        card_bin STRING, card_last4 STRING, expires_date DATE, acct_open_date DATE, has_chip BOOLEAN,
        num_cards_issued SMALLINT, credit_limit DECIMAL(12,2), year_pin_last_changed SMALLINT"""),
    "lakehouse.silver.mcc_codes": ("lakehouse.bronze.mcc_codes_raw", MCC_SOURCE, "mcc_code", """
        mcc_code INT, description STRING, mcc_group STRING"""),
}


def mcc_groups_df(spark):
    with MCC_GROUPS.open(encoding="utf-8") as f:
        rows = [(r["mcc"], r["mcc_group"]) for r in csv.DictReader(f)]
    return spark.createDataFrame(rows, "mcc string, mcc_group string")


def build(spark, table, bronze):
    b = spark.table(bronze)
    if table.endswith("customers"):
        return st.customers(b), st.CUSTOMER_COLS
    if table.endswith("cards"):
        return st.cards(b), st.CARD_COLS
    return st.mcc_codes(b, mcc_groups_df(spark)), st.MCC_COLS


def main():
    spark = lh.get_spark("silver_reference")
    try:
        for table, (bronze, source, key, ddl) in TABLES.items():
            batch_id = lh.require_bronze(spark, source)
            lh.ensure_silver_table(spark, table, ddl)
            df, cols = build(spark, table, bronze)
            src = df.withColumn("_batch_id", F.lit(batch_id))
            counts = lh.merge_changes(spark, src, table, key, [c for c in cols if c != key])
            total = spark.table(table).count()
            print(f"[silver] {table} ← {bronze} ({batch_id}): {total} dòng; "
                  f"thêm {counts['I']}, sửa {counts['U']}, xóa {counts['D']}")
        unmapped = spark.table("lakehouse.silver.mcc_codes").where("mcc_group = 'Chưa phân nhóm'").count()
        if unmapped:
            print(f"[cảnh báo] {unmapped} mã MCC chưa có trong {MCC_GROUPS.name}")
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
