"""Kiểm thử hàm biến đổi Bronze → Silver (spark/transform/silver_transforms.py).

Chạy Spark local trên máy host (cần Java 17 và pyspark; make test đã thêm pyspark).
"""

import csv
import json
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "spark"))
from common import helpers as h  # noqa: E402
from transform import silver_transforms as st  # noqa: E402

MCC_GROUPS = ROOT / "spark" / "transform" / "ref" / "mcc_groups.csv"


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.master("local[1]").appName("test_silver")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "1")
         .getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def apply(spark, fn, values):
    """Áp một hàm cột lên danh sách chuỗi, trả về danh sách kết quả theo đúng thứ tự."""
    df = spark.createDataFrame([(i, v) for i, v in enumerate(values)], "i int, v string")
    return [r.out for r in df.select("i", fn(F.col("v")).alias("out")).orderBy("i").collect()]


def test_parse_money(spark):
    got = apply(spark, st.parse_money,
                ["$-77.00", "$29278", " $14.5 ", "N/A", "$12,30.5", "", None, "77.00", "$1e5",
                 "$9999999999999.00"])
    assert got == [Decimal("-77.00"), Decimal("29278.00"), Decimal("14.50"),
                   None, None, None, None, None, None, None]


def test_parse_ts_is_ntz_and_strict(spark):
    got = apply(spark, st.parse_ts,
                ["2019-03-01 00:01:00", "2019-02-30 10:00:00", "2019-03-01", "abc", None])
    assert got == [datetime(2019, 3, 1, 0, 1), None, None, None, None]
    df = spark.createDataFrame([("2019-03-01 00:01:00",)], "v string")
    assert df.select(st.parse_ts(F.col("v")).alias("t")).schema["t"].dataType.typeName() == "timestamp_ntz"


def test_normalize_zip(spark):
    assert apply(spark, st.normalize_zip, ["2840.0", "58523.0", "501", None, "ABCDE", "123456.0"]) == \
        ["02840", "58523", "00501", None, None, None]


def test_normalize_channel_keeps_unknown_for_r07(spark):
    assert apply(spark, st.normalize_channel,
                 ["Swipe Transaction", "Chip Transaction", "Online Transaction", "Contactless Transaction"]) == \
        ["Swipe", "Chip", "Online", "Contactless"]


def test_split_errors(spark):
    assert apply(spark, st.split_errors,
                 [None, "", "Bad PIN", "Bad PIN,Insufficient Balance", " Bad CVV , Technical Glitch ,"]) == \
        [[], [], ["Bad PIN"], ["Bad PIN", "Insufficient Balance"], ["Bad CVV", "Technical Glitch"]]


def test_is_foreign(spark):
    assert apply(spark, st.is_foreign, ["OH", "Italy", None, "Dominican Republic", "oh"]) == \
        [False, True, False, True, True]


def test_month_year_to_date(spark):
    assert apply(spark, st.month_year_to_date, ["07/1997", "02/2020", "13/2020", "7/1997", None]) == \
        [date(1997, 7, 1), date(2020, 2, 1), None, None, None]
    assert apply(spark, lambda c: st.month_year_to_date(c, month_end=True), ["02/2020", "12/2024"]) == \
        [date(2020, 2, 29), date(2024, 12, 31)]


def test_small_mappings(spark):
    assert apply(spark, st.yes_no, ["YES", "no", "x"]) == [True, False, None]
    assert apply(spark, st.normalize_gender, ["Male", " female", "X"]) == ["Male", "Female", None]
    assert apply(spark, st.normalize_card_type, ["Debit (Prepaid)", "Credit", "Debit"]) == \
        ["Prepaid", "Credit", "Debit"]


def bronze_txn(spark, rows):
    return spark.createDataFrame(rows, ", ".join(f"{c} string" for c in h.TXN_COLS + ["_batch_id"]))


def txn(id_, date_="2019-03-05 10:00:00", amount="$10.00", use_chip="Chip Transaction",
        city="Akron", state="OH", zip_="44301.0", errors=None):
    return (id_, date_, "1", "2", amount, use_chip, "99", city, state, zip_, "5411", errors, "2019-03")


def test_split_batch_quarantines_block_rules(spark):
    rows = [
        txn("1"),                                         # hợp lệ
        txn(None),                                        # R01: id rỗng
        txn("abc"),                                       # R01: id không phải số
        txn("3", amount="N/A"),                           # R02
        txn("4", amount="$12,30.5"),                      # R02
        txn("5", date_="2019-02-30 10:00:00"),            # R02: ngày không tồn tại
        txn("6", date_="2019-02-28 23:59:59"),            # R03: tháng trước
        txn("7", date_="2019-04-01 00:00:00"),            # R03: biên trên (khoảng nửa mở)
        txn("8", date_="2019-03-01 00:00:00"),            # hợp lệ: biên dưới
        txn("9", date_="2019-03-09 09:00:00"),            # trùng id 9: giữ bản sớm hơn
        txn("9", date_="2019-03-10 09:00:00"),            # R01: bản trùng
        txn("10", amount="N/A"),                          # id 10: bản lỗi R02 ...
        txn("10"),                                        # ... bản hợp lệ được giữ
    ]
    valid, q = st.split_batch(bronze_txn(spark, rows), "2019-03")
    assert sorted(r._id for r in valid.collect()) == [1, 8, 9, 10]
    assert valid.where("_id = 9").first()["date"] == "2019-03-09 09:00:00"
    got = sorted((r.id or "", r.rule_id) for r in q.collect())
    assert got == [("", "R01"), ("10", "R02"), ("3", "R02"), ("4", "R02"), ("5", "R02"),
                   ("6", "R03"), ("7", "R03"), ("9", "R01"), ("abc", "R01")]
    assert set(q.columns) == set(h.TXN_COLS) | {"_batch_id", "rule_id", "reason"}
    assert valid.count() + q.count() == len(rows)          # R10 vế 2: bronze = silver + quarantine


def test_transactions_columns_and_flags(spark):
    rows = [txn("1", amount="$-77.00", use_chip="Online Transaction", city="ONLINE", state=None, zip_=None,
                errors="Bad PIN,Insufficient Balance"),
            txn("2", state="Italy", city="Rome", zip_=None)]
    valid, _ = st.split_batch(bronze_txn(spark, rows), "2019-03")
    out = {r.transaction_id: r for r in st.transactions(valid).collect()}
    assert list(st.transactions(valid).columns) == st.TXN_COLS
    a, b = out[1], out[2]
    assert (a.amount, a.is_refund, a.channel, a.is_online, a.is_foreign) == (Decimal("-77.00"), True, "Online", True, False)
    assert (a.error_list, a.has_error, a.zip) == (["Bad PIN", "Insufficient Balance"], True, None)
    assert (b.is_foreign, b.is_online, b.has_error, b.error_list, b.mcc_code) == (True, False, False, [], 5411)


def test_customers_and_cards(spark):
    cols = ["id", "current_age", "retirement_age", "birth_year", "birth_month", "gender", "address",
            "latitude", "longitude", "per_capita_income", "yearly_income", "total_debt", "credit_score",
            "num_credit_cards"]
    c = spark.createDataFrame([("825", "53", "66", "1966", "11", "Female", "462 Rose Lane", "34.1500",
                                "-117.7600", "$29278", "$59696", "$127613", "787", "5"),
                               ("1", "40", "65", "1979", "1", "Male", "x", "1", "2", "$0", "$0", "$10", "700", "1")],
                              ", ".join(f"{x} string" for x in cols))
    r = {x.customer_id: x for x in st.customers(c).collect()}
    assert r[825].debt_to_income == Decimal("2.138") and r[825].latitude == 34.15
    assert r[825].yearly_income == Decimal("59696.00") and r[1].debt_to_income is None
    k = spark.createDataFrame([("1", "825", "Visa", "Debit (Prepaid)", "t" * 64, "434467", "0444",
                                "12/2022", "YES", "2", "$24295", "09/2002", "2008")],
                              ", ".join(f"{x} string" for x in h.CARD_COLS))
    card = st.cards(k).first()
    assert list(st.cards(k).columns) == st.CARD_COLS
    assert (card.card_type, card.expires_date, card.acct_open_date, card.has_chip, card.credit_limit) == \
        ("Prepaid", date(2022, 12, 31), date(2002, 9, 1), True, Decimal("24295.00"))


def test_merchants_picks_most_frequent_location(spark):
    rows = [(1, "Akron", "OH", "44301", datetime(2019, 3, 1), "2019-03"),
            (1, "Akron", "OH", "44301", datetime(2019, 3, 2), "2019-03"),
            (1, "Ohio City", "OH", "45874", datetime(2019, 4, 1), "2019-04"),
            (2, "ONLINE", None, None, datetime(2019, 3, 1), "2019-03"),
            (3, "Rome", "Italy", None, datetime(2019, 3, 1), "2019-03"),
            (3, "Paris", "France", None, datetime(2019, 3, 5), "2019-03")]   # hòa: lấy vị trí gần nhất
    t = spark.createDataFrame(rows, "merchant_id int, merchant_city string, merchant_state string, "
                                    "zip string, transaction_ts timestamp_ntz, _batch_id string")
    m = {r.merchant_id: r for r in st.merchants(t).collect()}
    assert (m[1].merchant_city, m[1].location_count, m[1].txn_count, m[1]._batch_id) == ("Akron", 2, 3, "2019-04")
    assert (m[2].is_online, m[2].is_foreign) == (True, False)
    assert (m[3].merchant_city, m[3].is_foreign) == ("Paris", True)


def test_mcc_groups_cover_every_code(spark):
    groups = list(csv.DictReader(MCC_GROUPS.open()))
    codes = [int(g["mcc"]) for g in groups]
    assert len(codes) == len(set(codes)) == 109
    assert len({g["mcc_group"] for g in groups}) == 10
    src = ROOT / "data" / "raw" / "mcc_codes.json"
    if src.exists():
        assert set(codes) == {int(k) for k in json.loads(src.read_text())}
    b = spark.createDataFrame([("5812", "Eating Places"), ("1234", "Lạ")], "mcc string, description string")
    g = spark.createDataFrame([(g["mcc"], g["mcc_group"]) for g in groups], "mcc string, mcc_group string")
    out = {r.mcc_code: r.mcc_group for r in st.mcc_codes(b, g).collect()}
    assert out == {5812: "Ăn uống", 1234: "Chưa phân nhóm"}
