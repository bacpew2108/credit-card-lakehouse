"""Kiểm thử các hàm thuần của generators/seed_source.py (không cần PostgreSQL, MinIO)."""

import json

import pytest

import seed_source as ss

DATA = ss.DEFAULT_DATA_DIR


def need_data(name):
    path = DATA / name
    if not path.exists():
        pytest.skip("không có bộ dữ liệu gốc trong data/raw")
    return path


def test_mcc_to_jsonl():
    out = ss.mcc_to_jsonl({"5812": "Eating Places", "4784": "Tolls and Bridge Fees"})
    lines = out.splitlines()
    assert out.endswith("\n") and len(lines) == 2
    assert json.loads(lines[0]) == {"mcc": "4784", "description": "Tolls and Bridge Fees"}


def test_read_csv_rows_selects_columns_and_nulls(tmp_path):
    f = tmp_path / "cards.csv"
    f.write_text('id,client_id,extra,note\n1,7,x,"a, b"\n2,8,y,\n')
    assert ss.read_csv_rows(f, ["id", "note"]) == [("1", "a, b"), ("2", None)]
    with pytest.raises(ValueError):
        ss.read_csv_rows(f, ["id", "missing"])


def test_load_env_file_keeps_existing(tmp_path, monkeypatch):
    env = tmp_path / ".env"
    env.write_text("# comment\nSEED_TEST_A=1\nSEED_TEST_B='two'\n")
    monkeypatch.setenv("SEED_TEST_A", "env")
    monkeypatch.delenv("SEED_TEST_B", raising=False)
    ss.load_env_file(env)
    assert ss.os.environ["SEED_TEST_A"] == "env"
    assert ss.os.environ["SEED_TEST_B"] == "two"


def test_transactions_header_matches_table():
    # copy_transactions đẩy nguyên file vào COPY nên thứ tự cột phải trùng
    assert ss.csv_header(need_data("transactions_data.csv")) == ss.TXN_COLS


def test_reference_files_have_needed_columns():
    assert set(ss.CUSTOMER_COLS) <= set(ss.csv_header(need_data("users_data.csv")))
    assert set(ss.CARD_COLS) <= set(ss.csv_header(need_data("cards_data.csv")))
