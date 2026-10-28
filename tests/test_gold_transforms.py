"""Kiểm thử biến đổi Silver → Gold (spark/transform/gold_transforms.py) và so sánh R11.

Dimension và fact được dựng từ fixture DQ (conftest.fx): các dòng cố ý vi phạm R04, R06 phải nhận
khóa −1, số dòng và tổng tiền của fact phải bằng Silver, mart phải khớp fact.
"""

from datetime import date
from decimal import Decimal
from types import SimpleNamespace

import pytest

pytest.importorskip("pyspark")
from pyspark.sql import functions as F  # noqa: E402

from quality import rules as dq  # noqa: E402
from quality.reconcile import r11_compare  # noqa: E402
from transform import gold_transforms as gt  # noqa: E402
from transform import silver_transforms as st  # noqa: E402


def apply(spark, fn, values, dtype="string"):
    """Áp một hàm cột lên danh sách giá trị, trả về kết quả theo đúng thứ tự."""
    df = spark.createDataFrame([(i, v) for i, v in enumerate(values)], f"i int, v {dtype}")
    return [r.out for r in df.select("i", fn(F.col("v")).alias("out")).orderBy("i").collect()]


def test_bands(spark):
    assert apply(spark, gt.age_band, [17, 18, 24, 25, 64, 65, 101, None], "int") == \
        ["< 18", "18–24", "18–24", "25–34", "55–64", "65+", "65+", "Unknown"]
    assert apply(spark, gt.income_band, [Decimal("29999.99"), Decimal("30000"), Decimal("40000"),
                                         Decimal("50000"), Decimal("74999.99"), Decimal("75000"), None],
                 "decimal(12,2)") == ["< 30k", "30–40k", "40–50k", "50–75k", "50–75k", "≥ 75k", "Unknown"]
    assert apply(spark, gt.credit_score_band, [579, 580, 670, 740, 799, 800, None], "int") == \
        ["Poor (< 580)", "Fair (580–669)", "Good (670–739)", "Very Good (740–799)", "Very Good (740–799)",
         "Exceptional (≥ 800)", "Unknown"]


def test_error_combo_sorted_or_none(spark):
    assert apply(spark, gt.error_combo, [[], ["Insufficient Balance", "Bad PIN"], ["Bad PIN"]], "array<string>") == \
        ["None", "Bad PIN, Insufficient Balance", "Bad PIN"]


def test_assign_keys_keeps_existing_and_appends_in_nk_order(spark):
    cur = spark.createDataFrame([(5, "b"), (9, "d"), (-1, "Unknown")], "k int, n string")
    src = spark.createDataFrame([("a",), ("b",), ("c",), ("d",)], "n string")
    assert {r.n: r.k for r in gt.assign_keys(src, cur, "k", ["n"]).collect()} == {"b": 5, "d": 9, "a": 10, "c": 11}
    # Dựng lại từ đầu: key theo thứ tự khóa tự nhiên
    assert {r.n: r.k for r in gt.assign_keys(src, cur.limit(0), "k", ["n"]).collect()} == \
        {"a": 1, "b": 2, "c": 3, "d": 4}


def test_assign_keys_matches_null_nk(spark):
    cur = spark.createDataFrame([(1, "ONLINE", None, None)],
                                "location_key int, merchant_city string, merchant_state string, zip string")
    src = spark.createDataFrame([("ONLINE", None, None), ("Akron", "OH", "44301")],
                                "merchant_city string, merchant_state string, zip string")
    got = {r.merchant_city: r.location_key for r in gt.assign_keys(src, cur, "location_key", gt.LOCATION_NK).collect()}
    assert got == {"ONLINE": 1, "Akron": 2}


def test_special_rows(spark):
    dim = spark.createDataFrame([(1, "x", 3, True)], "k int, s string, n int, b boolean")
    assert sorted(tuple(r) for r in gt.special_rows(dim, "k").collect()) == \
        [(-2, "Deleted", None, None), (-1, "Unknown", None, None)]


def test_dim_date(spark):
    d = gt.dim_date(spark)
    assert d.count() == 3652
    r = {x.date_key: x for x in d.where("date_key IN (20100101, 20100102, 20100104, 20191231)").collect()}
    assert (r[20100101].day_of_week, r[20100101].day_name, r[20100101].is_weekend) == (5, "Thứ Sáu", False)
    assert (r[20100102].day_of_week, r[20100102].is_weekend) == (6, True)
    assert (r[20100104].day_of_week, r[20100104].day_name) == (1, "Thứ Hai")
    assert (r[20191231].full_date, r[20191231].quarter, r[20191231].month, r[20191231].year) == \
        (date(2019, 12, 31), 4, 12, 2019)


@pytest.fixture(scope="module")
def gold(spark, fx):
    """Dimension (khóa sinh từ đầu, cộng dòng −1, −2) và fact dựng từ fixture DQ."""
    silver = fx.silver.withColumn("_batch_id", F.lit("2019-03"))
    srcs = {
        "customer": (gt.dim_customer(fx.customers), "customer_key", ["customer_id"]),
        "card": (gt.dim_card(fx.cards), "card_key", ["card_id"]),
        "merchant": (gt.dim_merchant(st.merchants(silver)), "merchant_key", ["merchant_id"]),
        "location": (gt.locations(silver), "location_key", gt.LOCATION_NK),
        "mcc": (gt.dim_mcc(fx.mccs), "mcc_key", ["mcc_code"]),
        "txn_profile": (gt.txn_profiles(silver), "txn_profile_key", gt.PROFILE_NK),
    }
    dims = {}
    for name, (src, key, nk) in srcs.items():
        keyed = gt.assign_keys(src, src.limit(0).withColumn(key, F.lit(None).cast("int")), key, nk)
        dims[name] = keyed.unionByName(gt.special_rows(keyed, key))
    calendar = gt.dim_date(spark)
    dims["date"] = calendar.unionByName(gt.special_rows(calendar, "date_key"))
    f = gt.fact(silver, dims)
    return SimpleNamespace(silver=silver, dims=dims, fact=f, rows={r.transaction_id: r for r in f.collect()})


def test_fact_unknown_keys_only_for_r04_r06(gold):
    r = gold.rows
    assert (r[109].customer_key, r[109].card_key, r[109].customer_age_at_txn) == (-1, -1, None)  # R04
    assert r[110].card_key == -1 and r[110].customer_key > 0                                    # R04
    assert r[112].mcc_key == -1                                                                  # R06
    unknown = {(tid, k) for tid, row in r.items() for k in gt.KEYS if row[k] < 0}
    assert unknown == {(109, "customer_key"), (109, "card_key"), (110, "card_key"), (112, "mcc_key")}
    assert r[101].location_key > 0                       # ONLINE: state, zip NULL vẫn tra được
    assert (r[100].date_key, r[100].txn_hour, r[100].customer_age_at_txn) == (20190301, 0, 2019 - 1979)


def test_fact_matches_silver(gold):
    agg = [F.count(F.lit(1)), F.sum("amount")]
    assert tuple(gold.fact.agg(*agg).first()) == tuple(gold.silver.agg(*agg).first()) == (14, Decimal("160.49"))
    assert gold.fact.columns == gt.FACT_COLS


def test_marts(gold):
    f, dims = gold.fact, gold.dims
    for name, (build, cols) in gt.MARTS.items():
        assert build(f, dims).select(*[c for c, _ in cols]).columns == [c for c, _ in cols], name
    kpi = gt.mart_monthly_kpi(f, dims).first()
    assert (kpi.month, kpi.txn_count, kpi.total_amount, kpi.active_customers) == \
        (date(2019, 3, 1), 14, Decimal("160.49"), 4)
    assert (kpi.refund_count, kpi.refund_amount, kpi.error_txn_count) == (1, Decimal("-12.50"), 1)
    assert (kpi.swipe_count, kpi.chip_count, kpi.online_count) == (3, 8, 2)
    cards = {r.card_key: r for r in gt.mart_card_monthly(f, dims).collect()}
    card10 = next(r for r in cards.values() if r.card_type == "Credit" and r.credit_limit == Decimal("10000.00"))
    assert (card10.purchase_amount, card10.utilization) == (Decimal("75.00"), Decimal("0.0075"))
    assert all(r.utilization is None for r in cards.values() if r.card_type != "Credit")
    errors = {(r.error_type, r.channel): (r.error_count, r.channel_txn_count)
              for r in gt.mart_error_monthly(f, dims).collect()}
    assert errors == {("Bad PIN", "Swipe"): (1, 3), ("Insufficient Balance", "Swipe"): (1, 3)}
    geo = {(r.merchant_state, r.is_online): r.txn_count for r in gt.mart_geo_monthly(f, dims).collect()}
    assert geo[(None, True)] == 2                        # 101 và 114 có city = ONLINE
    assert sum(r.txn_count for r in gt.mart_customer_segment_monthly(f, dims).collect()) == 14


def test_r11_compare():
    ok = (118_548, Decimal("4539678.00"))
    assert r11_compare(ok, ok, ok) == (0, "silver 118548 dòng, 4539678.00; fact 118548 dòng, 4539678.00; "
                                          "mart 118548 dòng, 4539678.00")
    assert r11_compare(ok, (118_547, ok[1]), (118_547, ok[1]))[0] == 1      # Silver ≠ fact về số dòng
    assert r11_compare(ok, ok, None)[0] == 2                                # chưa dựng mart
    assert dq.decide("R11", 4, 1)[1] == "BLOCK" and dq.decide("R11", 4, 0)[1] == "PASS"
    assert "R11" not in dq.SILVER_RULES and "R10.2" in dq.SILVER_RULES
