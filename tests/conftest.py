"""Fixture dùng chung cho các test chạy Spark local trên máy host (cần Java 17 và pyspark).

- spark: SparkSession local.
- fx: dữ liệu mẫu có lỗi cố ý (tests/fixtures/dq_*.csv) dựng thành Silver như job thật. Mỗi dòng
  ghi rule mà nó cố ý vi phạm ở cột _expect (rỗng là dòng sạch).
"""

import csv
from pathlib import Path
from types import SimpleNamespace

import pytest

FIXTURES = Path(__file__).resolve().parent / "fixtures"
MONTH = "2019-03"
OTHER_MONTH_IDS = [900]     # id đã có trong Silver ở tháng khác: bản trong batch bị R01


@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    s = (SparkSession.builder.master("local[1]").appName("tests")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "1")
         .getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def read_fixture(name):
    """Các dòng của file fixture; ô rỗng là NULL như ở nguồn."""
    with (FIXTURES / name).open(encoding="utf-8", newline="") as f:
        return [{k: v or None for k, v in r.items()} for r in csv.DictReader(f)]


def to_bronze(spark, rows, cols, extra="", extra_values=lambda i: ()):
    schema = ", ".join(f"{c} string" for c in cols) + extra
    return spark.createDataFrame([tuple(r[c] for c in cols) + extra_values(i) for i, r in enumerate(rows)], schema)


@pytest.fixture(scope="session")
def fx(spark):
    """Fixture dựng thành Silver: bảng tham chiếu qua hàm Silver, giao dịch qua cách ly rồi Silver."""
    from common import helpers as h
    from transform import silver_transforms as st
    from transform.silver_reference import mcc_groups_df

    cust_rows, txn_rows = read_fixture("dq_customers.csv"), read_fixture("dq_transactions.csv")
    customers = st.customers(to_bronze(spark, cust_rows, h.CUSTOMER_COLS))
    cards = st.cards(to_bronze(spark, read_fixture("dq_cards.csv"), h.CARD_COLS))
    mccs = st.mcc_codes(to_bronze(spark, read_fixture("dq_mcc_codes.csv"), h.MCC_COLS), mcc_groups_df(spark))
    bronze = to_bronze(spark, txn_rows, h.TXN_COLS, ", _batch_id string, _row int", lambda i: (MONTH, i))
    other = spark.createDataFrame([(i,) for i in OTHER_MONTH_IDS], "transaction_id bigint")
    valid, quarantine = st.split_batch(bronze, MONTH, other)
    return SimpleNamespace(customers=customers, cards=cards, mccs=mccs, bronze=bronze, valid=valid,
                           quarantine=quarantine, silver=st.transactions(valid),
                           txn_expect=[r["_expect"] or "" for r in txn_rows],
                           cust_expect={int(r["id"]): r["_expect"] or "" for r in cust_rows})
