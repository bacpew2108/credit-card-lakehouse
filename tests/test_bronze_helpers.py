"""Kiểm thử các hàm thuần của lớp Bronze (spark/common/helpers.py), không cần Spark."""

import argparse
from datetime import date

import pytest

from common import helpers as h


def test_month_bounds_and_year_end():
    assert h.month_bounds("2019-03") == ("2019-03-01", "2019-04-01")
    assert h.month_bounds("2018-12") == ("2018-12-01", "2019-01-01")


@pytest.mark.parametrize("bad", ["2019-3", "2019-13", "19-03", "2019-03-01", "", None, "2019-03'; DROP"])
def test_parse_month_rejects_bad_input(bad):
    with pytest.raises(ValueError):
        h.parse_month(bad)


def test_iter_months_covers_whole_dataset():
    months = h.iter_months("2010-01", "2019-10")
    assert len(months) == 118 and months[0] == "2010-01" and months[-1] == "2019-10"
    assert h.iter_months("2019-11", "2020-02") == ["2019-11", "2019-12", "2020-01", "2020-02"]
    with pytest.raises(ValueError):
        h.iter_months("2019-03", "2019-02")


def test_add_months_and_month_scope():
    assert h.add_months("2019-03", -12) == "2018-03"
    assert h.add_months("2019-01", -1) == "2018-12"
    assert h.add_months("2018-12", 1) == "2019-01"
    assert h.add_months("2010-01", 0) == "2010-01"
    assert h.month_scope("2019-12").format(t="t.") == (
        "t.transaction_ts >= TIMESTAMP_NTZ '2019-12-01 00:00:00' "
        "AND t.transaction_ts < TIMESTAMP_NTZ '2020-01-01 00:00:00'")


def test_resolve_months_accepts_one_form_only():
    ap = argparse.ArgumentParser()
    h.add_month_args(ap)
    assert h.resolve_months(ap.parse_args(["--month", "2019-03"])) == ["2019-03"]
    assert h.resolve_months(ap.parse_args(["--from", "2019-11", "--to", "2020-01"])) == [
        "2019-11", "2019-12", "2020-01"]
    for argv in ([], ["--from", "2019-01"], ["--month", "2019-01", "--to", "2019-02"]):
        with pytest.raises(ValueError):
            h.resolve_months(ap.parse_args(argv))


def test_month_filter_uses_half_open_range():
    assert h.month_filter("2019-12") == (
        "date >= TIMESTAMP '2019-12-01' AND date < TIMESTAMP '2020-01-01'")


def test_select_text_sql_casts_every_column_and_never_selects_cvv():
    sql = h.select_text_sql("cards")
    assert sql.startswith("SELECT id::text AS id, client_id::text AS client_id")
    assert sql.count("::text") == len(h.CARD_SRC_COLS)
    assert "cvv" not in sql
    assert h.select_text_sql("transactions", "x = 1").endswith("FROM transactions WHERE x = 1")


def test_bronze_cards_have_no_pan_or_cvv():
    assert "card_number" not in h.CARD_COLS and "cvv" not in h.CARD_COLS
    assert {"card_token", "card_bin", "card_last4"} <= set(h.CARD_COLS)


def test_protect_pan_rfc4231_vector():
    # RFC 4231, test case 2: HMAC-SHA256(key="Jefe", data="what do ya want for nothing?")
    token, _, _ = h.protect_pan("what do ya want for nothing?", "Jefe")
    assert token == "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"


def test_protect_pan_bin_last4_and_stability():
    token, bin6, last4 = h.protect_pan("4344676511950444", "secret")
    assert (bin6, last4) == ("434467", "0444")
    assert len(token) == 64 and "4344676511950444" not in token
    assert h.protect_pan(" 4344676511950444 ", "secret")[0] == token
    assert h.protect_pan("4344676511950444", "other")[0] != token
    assert h.protect_pan(None, "secret") == (None, None, None)


@pytest.mark.parametrize("bad", [None, "", "change_me"])
def test_require_secret(bad):
    with pytest.raises(ValueError):
        h.require_secret(bad)


def test_ref_batch_id():
    assert h.ref_batch_id(date(2026, 10, 15)) == "ref_20261015"
    assert h.ref_batch_id().startswith("ref_")
    assert h.check_ref_batch_id("ref_20261015") == "ref_20261015"
    with pytest.raises(ValueError):
        h.check_ref_batch_id("2019-03")
