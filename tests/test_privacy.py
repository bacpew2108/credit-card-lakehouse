"""Quyền xóa dữ liệu (spark/maintenance/erase_customer.py) và danh sách xóa ở các bước sau:
giao dịch của khách hàng đã xóa mang id −2, extract ẩn danh chúng, R04 không coi là lỗi, fact gắn khóa
−2 Deleted. Chạy Spark local (cần pyspark, Java 17)."""

import pytest

pytest.importorskip("pyspark")
from pyspark.sql import functions as F  # noqa: E402

from common import lakehouse as lh  # noqa: E402
from maintenance import erase_customer as ec  # noqa: E402
from quality import rules as dq  # noqa: E402


def test_ids_sql_va_not_in_sql():
    assert ec.ids_sql(set()) == "NULL"
    assert ec.ids_sql({3, 1}) == "1, 3"
    assert ec.ids_sql({7}, quote=True) == "'7'"
    assert lh.not_in_sql("id", set()) == ""
    assert lh.not_in_sql("client_id", {12, 5}) == "client_id NOT IN (5, 12)"


def test_plan_chon_dung_dong_cua_khach_o_moi_tang():
    steps = {t: (op, cond, set_) for t, op, cond, set_ in ec.plan(1510, {1257, 4346}, {77}, {300, 301})}
    assert list(steps) == ["bronze.card.customers_raw", "bronze.card.cards_raw", "bronze.card.transactions_raw",
                           "ops.pipeline.quarantine_transactions", "silver.card.customers", "silver.card.cards",
                           "silver.card.transactions", "gold.card.dim_customer", "gold.card.dim_card",
                           "gold.card.fact_transaction"]
    # Bronze là chuỗi: so với id có nháy
    assert steps["bronze.card.transactions_raw"] == (
        "UPDATE", "client_id = '1510' OR card_id IN ('1257', '4346')", "client_id = '-2', card_id = '-2'")
    assert steps["silver.card.transactions"][1] == "customer_id = 1510 OR card_id IN (1257, 4346)"
    assert steps["gold.card.fact_transaction"] == (
        "UPDATE", "customer_key IN (77) OR card_key IN (300, 301)",
        "customer_key = -2, card_key = -2, customer_age_at_txn = NULL")
    assert {op for op, *_ in steps.values()} == {"DELETE", "UPDATE"}
    # khách hàng không có thẻ: IN (NULL) không khớp dòng nào, câu SQL vẫn hợp lệ
    assert "card_id IN (NULL)" in ec.plan(9, set(), set(), set())[6][2]


def test_anonymize_giu_so_dong_va_doi_id_thanh_tru_2(spark):
    df = spark.createDataFrame([("1", "1510", "1257"), ("2", "7", "1257"), ("3", "7", "9")],
                               "id string, client_id string, card_id string")
    out = lh.anonymize(df, "client_id", "card_id", {1510}, {1257, 4346}, "-2")
    assert sorted(tuple(r) for r in out.collect()) == [("1", "-2", "-2"), ("2", "-2", "-2"), ("3", "7", "9")]
    assert lh.anonymize(df, "client_id", "card_id", set(), set(), "-2") is df


def test_r04_khong_coi_giao_dich_da_an_danh_la_loi(fx):
    clean = [r.transaction_id for r in dq.txn_flags(fx.silver, fx.customers, fx.cards, fx.mccs).collect()
             if not any(r[x] for x in dq.txn_checks())]
    tid = clean[0]
    erased = (fx.silver
              .withColumn("customer_id", F.when(F.col("transaction_id") == tid, lh.ERASED_ID)
                          .otherwise(F.col("customer_id")))
              .withColumn("card_id", F.when(F.col("transaction_id") == tid, lh.ERASED_ID).otherwise(F.col("card_id"))))
    row = next(r for r in dq.txn_flags(erased, fx.customers, fx.cards, fx.mccs).collect()
               if r.transaction_id == tid)
    assert not any(row[x] for x in dq.txn_checks())
    # id lạ khác −2 vẫn là R04
    bad = erased.withColumn("customer_id", F.when(F.col("transaction_id") == tid, 999_999)
                            .otherwise(F.col("customer_id")))
    assert next(r for r in dq.txn_flags(bad, fx.customers, fx.cards, fx.mccs).collect()
                if r.transaction_id == tid).R04
