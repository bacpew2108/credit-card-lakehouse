"""Truy vấn Q1–Q10 (sql/run_queries.py) và cấu hình dashboard Superset (infra/superset/dashboards.py):
phép so sánh kết quả hai engine, sự đầy đủ của bộ truy vấn, tham chiếu giữa chart và dataset.
Không cần stack chạy."""

import re
from decimal import Decimal

import pytest

import run_queries as rq

QUERY_FILES = sorted(rq.QUERIES.glob("q*.sql"))
# Truy vấn không có tính toán độc lập: dữ liệu vận hành chỉ có trong ops
NO_INDEPENDENT = {"q10c_pipeline_runs"}


# ---------------------------------------------------------------- compare()

COLS = ["k", "n", "amount", "pct"]


def test_compare_khop():
    t = [("a", 10, Decimal("1.50"), 12.35)]
    d = [("a", 10, Decimal("1.50"), 12.35)]
    assert rq.compare(["k"], COLS, t, COLS, d) == []


def test_compare_so_nguyen_va_decimal_phai_bang_tuyet_doi():
    t = [("a", 10, Decimal("1.50"), 12.35)]
    assert rq.compare(["k"], COLS, t, COLS, [("a", 11, Decimal("1.50"), 12.35)])
    assert rq.compare(["k"], COLS, t, COLS, [("a", 10, Decimal("1.51"), 12.35)])


def test_compare_so_thuc_lech_mot_don_vi_cuoi_duoc_chap_nhan():
    t = [("a", 1, Decimal(0), 12.35), ("b", 1, Decimal(0), 0.5)]
    assert rq.compare(["k"], COLS, t, COLS, [("a", 1, Decimal(0), 12.36), ("b", 1, Decimal(0), 0.5)]) == []
    assert rq.compare(["k"], COLS, t, COLS, [("a", 1, Decimal(0), 12.37), ("b", 1, Decimal(0), 0.5)])


def test_compare_bao_dong_thieu_va_khac_cot():
    t = [("a", 1, Decimal(0), 1.0), ("b", 2, Decimal(0), 1.0)]
    issues = rq.compare(["k"], COLS, t, COLS, [("a", 1, Decimal(0), 1.0)])
    assert any("chỉ có ở trino" in i for i in issues)
    assert rq.compare(["k"], COLS, t, ["k", "n", "amount", "ratio"], t)


def test_compare_null():
    t = [("a", None, Decimal(0), None)]
    assert rq.compare(["k"], COLS, t, COLS, [("a", None, Decimal(0), None)]) == []
    assert rq.compare(["k"], COLS, t, COLS, [("a", 0, Decimal(0), None)])


# ---------------------------------------------------------------- bộ truy vấn

def test_du_10_cau_hoi():
    asked = {int(re.match(r"q(\d+)", f.stem).group(1)) for f in QUERY_FILES}
    assert asked == set(range(1, 11))


@pytest.mark.parametrize("f", QUERY_FILES, ids=lambda f: f.stem)
def test_truy_van_co_khoa_va_tinh_doc_lap(f):
    text = f.read_text()
    keys = rq.keys_of(text)
    assert keys and all(re.search(rf"\b{k}\b", text) for k in keys)
    assert "Dashboard D" in text, "ghi rõ truy vấn phục vụ dashboard nào"
    assert (rq.INDEPENDENT / f.name).exists() or f.stem in NO_INDEPENDENT


def test_truy_van_chi_doc_gold_ops():
    """Vai trò analyst chỉ đọc gold, ops: truy vấn không được nhắc tới bronze, silver."""
    for f in QUERY_FILES:
        assert not re.search(r"\b(bronze|silver)\.", f.read_text()), f.stem


# ---------------------------------------------------------------- dashboard

def test_dashboard_tham_chieu_hop_le():
    pytest.importorskip("yaml")
    import dashboards as sd

    charts = [c for *_, rows, _ in sd.DASHBOARDS for row in rows for c in row]
    uuids = [c["uuid"] for c in charts]
    assert len(uuids) == len(set(uuids)), "uuid chart trùng"
    by_uuid = {sd.uid(f"dataset:{n}"): spec for n, spec in sd.DATASETS.items()}
    for c in charts:
        spec = by_uuid[c["dataset_uuid"]]
        names = {m["metric_name"] for m in spec["metrics"]}
        p = c["params"]
        used = p.get("metrics") or ([p["metric"]] if p.get("metric") else [])
        # metric SQL có nhãn (heatmap) không tham chiếu tên metric của dataset
        used = [m for m in used if isinstance(m, str)]
        assert set(used) <= names, f"{c['slice_name']}: metric {set(used) - names} không có trong dataset"
        assert all(len(row) <= 4 and 12 % len(row) == 0 for *_, rows, _ in sd.DASHBOARDS for row in rows)
    for name in sd.DATASETS:
        assert (rq.QUERIES / f"{name}.sql").exists()


def test_bundle_du_thanh_phan():
    pytest.importorskip("yaml")
    import dashboards as sd

    contents = sd.bundle()
    assert "metadata.yaml" in contents
    assert sum(k.startswith("datasets/") for k in contents) == len(sd.DATASETS)
    assert sum(k.startswith("dashboards/") for k in contents) == len(sd.DASHBOARDS)
