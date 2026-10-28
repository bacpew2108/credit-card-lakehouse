"""Kiểm thử rule chất lượng dữ liệu (spark/quality/rules.py) trên dữ liệu mẫu có lỗi cố ý.

Mỗi dòng trong tests/fixtures/dq_*.csv ghi rule mà nó cố ý vi phạm ở cột _expect (rỗng là dòng
sạch). Fixture được dựng thành Silver như job thật, rồi test khẳng định mọi dòng lỗi bị đúng rule
đó bắt (R01–R03 vào quarantine, rule WARN được ghi nhận) và dòng sạch không bị rule nào bắt.
"""

import csv
from collections import Counter
from pathlib import Path
from types import SimpleNamespace

import pytest

pytest.importorskip("pyspark")
from common import helpers as h  # noqa: E402
from quality import rules as dq  # noqa: E402
from transform import silver_transforms as st  # noqa: E402
from transform.silver_reference import mcc_groups_df  # noqa: E402

FIXTURES = Path(__file__).resolve().parent / "fixtures"
MONTH = "2019-03"
OTHER_MONTH_IDS = [900]     # id đã có trong Silver ở tháng khác: bản trong batch bị R01
RECORD_RULES = {r for r, rule in dq.RULES.items() if rule.per_record}


def read_fixture(name):
    """Các dòng của file fixture; ô rỗng là NULL như ở nguồn."""
    with (FIXTURES / name).open(encoding="utf-8", newline="") as f:
        return [{k: v or None for k, v in r.items()} for r in csv.DictReader(f)]


def to_bronze(spark, rows, cols, extra="", extra_values=lambda i: ()):
    schema = ", ".join(f"{c} string" for c in cols) + extra
    return spark.createDataFrame([tuple(r[c] for c in cols) + extra_values(i) for i, r in enumerate(rows)], schema)


@pytest.fixture(scope="module")
def fx(spark):
    """Fixture dựng thành Silver: bảng tham chiếu qua hàm Silver, giao dịch qua cách ly rồi Silver."""
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


def test_fixture_covers_every_record_rule(fx):
    covered = set(fx.txn_expect) | set(fx.cust_expect.values())
    assert RECORD_RULES <= covered, f"fixture thiếu lỗi cho {RECORD_RULES - covered}"


def test_every_intentional_error_is_caught_by_its_rule(fx):
    """Mỗi dòng giao dịch: rule bắt được (cách ly hoặc WARN) đúng bằng _expect."""
    got = {r._row: r.rule_id for r in fx.quarantine.collect()}
    row_of = {r._id: r._row for r in fx.valid.select("_id", "_row").collect()}
    rules = list(dq.txn_checks())
    for r in dq.txn_flags(fx.silver, fx.customers, fx.cards, fx.mccs).collect():
        got[row_of[r.transaction_id]] = ",".join(x for x in rules if r[x])
    assert [got[i] for i in range(len(fx.txn_expect))] == fx.txn_expect


def test_r09_on_customers(fx):
    flags = dq.customer_flags(fx.customers, fx.cards).collect()
    assert {r.customer_id: "R09" if r.R09 else "" for r in flags} == fx.cust_expect


def test_counts_details_and_r10(fx):
    """Số vi phạm từng rule khớp fixture, details tách theo điều kiện; bronze = silver + quarantine."""
    results = dq.summarize(dq.txn_flags(fx.silver, fx.customers, fx.cards, fx.mccs), dq.txn_checks())
    expect = Counter(fx.txn_expect)
    n_valid = fx.silver.count()
    assert {r: (checked, failed) for r, checked, failed, _ in results} == \
        {r: (n_valid, expect[r]) for r in dq.txn_checks()}
    details = {r: d for r, _, _, d in results}
    assert details["R14"] == "trước tháng mở thẻ: 1; sau tháng hết hạn: 1"
    assert details["R07"] == ("kênh lạ: 1; Online nhưng merchant_city khác ONLINE: 1; "
                              "không phải Online nhưng merchant_city = ONLINE: 1")
    assert details["R05"] is None                       # rule một điều kiện: không cần tách
    q = Counter(r.rule_id for r in fx.quarantine.collect())
    assert q == Counter({r: expect[r] for r in ("R01", "R02", "R03")})
    assert fx.bronze.count() == n_valid + sum(q.values())


def test_dirty_fixture_batch_is_blocked(fx):
    """Fixture có tỷ lệ lỗi rất cao: ở ngưỡng 1% mọi rule có vi phạm đều BLOCK; nới ngưỡng thì WARN."""
    results = dq.summarize(dq.txn_flags(fx.silver, fx.customers, fx.cards, fx.mccs), dq.txn_checks())
    assert {r: dq.decide(r, n, f, 0.01)[1] for r, n, f, _ in results} == {r: "BLOCK" for r in dq.txn_checks()}
    assert {r: dq.decide(r, n, f, 0.5)[1] for r, n, f, _ in results} == {r: "WARN" for r in dq.txn_checks()}


@pytest.mark.parametrize("rule_id, checked, failed, ratio, status", [
    ("R07", 118_906, 0, 0.01, "PASS"),
    ("R07", 118_906, 145, 0.01, "WARN"),          # dữ liệu thật 2019-08: 0,12%
    ("R07", 1000, 10, 0.01, "WARN"),              # đúng bằng ngưỡng: chưa vượt
    ("R07", 1000, 11, 0.01, "BLOCK"),             # vượt ngưỡng: nâng WARN → BLOCK
    ("R01", 1000, 3, 0.01, "WARN"),               # vài bản ghi bị cách ly, batch đi tiếp
    ("R02", 1000, 50, 0.01, "BLOCK"),             # cách ly quá nhiều: dừng DAG
    ("R07", 1000, 11, 0.05, "WARN"),              # ngưỡng cấu hình được
    ("R10.2", 113_000, 1, 0.01, "BLOCK"),         # đối soát lệch một dòng cũng chặn
    ("R12", 1, 1, 0.01, "WARN"),                  # R12 chỉ cảnh báo, không nâng
    ("I01", 74_831, 16_182, 0.01, "INFO"),        # merchant nhiều vị trí: chỉ ghi nhận
    ("R12", 0, 0, 0.01, "PASS"),                  # chưa đủ lịch sử, không xét
])
def test_decide(rule_id, checked, failed, ratio, status):
    got_ratio, got_status = dq.decide(rule_id, checked, failed, ratio)
    assert got_status == status
    assert got_ratio == (failed / checked if checked else None)


def test_r12_three_sigma_against_previous_12_months():
    months = h.iter_months("2018-03", "2019-03")
    base = {m: (100_000 + 1000 * (i % 3), 4_500_000 + 50_000 * (i % 3)) for i, m in enumerate(months[:-1])}
    assert dq.r12({**base, "2019-03": (101_000, 4_550_000)}, "2019-03")[:2] == (1, 0)
    checked, failed, details = dq.r12({**base, "2019-03": (101_000, 4_000_000)}, "2019-03")
    assert (checked, failed) == (1, 1) and "tổng tiền z = -" in details
    del base["2018-07"]
    assert dq.r12({**base, "2019-03": (101_000, 4_000_000)}, "2019-03") == \
        (0, 0, "chưa đủ 12 tháng trước (11/12), không xét")
