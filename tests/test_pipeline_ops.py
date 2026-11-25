"""Kiểm thử phần vận hành của tuần 8: báo cáo lần chạy, cảnh báo, kiểm chứng dựng lại, lệnh bảo trì."""

import http.server
import json
import threading
from datetime import date, datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

pytest.importorskip("pyspark")
import alerts  # noqa: E402  (airflow/dags)
from common import helpers as h  # noqa: E402
from maintenance import iceberg_maintenance as im  # noqa: E402
from maintenance import rebuild  # noqa: E402
from quality import run_report  # noqa: E402


# ---------------------------------------------------------------- báo cáo lần chạy, cảnh báo

def test_batch_label():
    assert h.batch_label(["2019-03"]) == "2019-03"
    assert h.batch_label(h.iter_months("2010-01", "2018-12")) == "2010-01..2018-12"
    assert h.batch_label([]) is None


def test_dq_summary_counts_latest_status_and_lists_non_pass_rules():
    rows = [("R07", "WARN", 80), ("R14", "WARN", 1), ("R10.1", "PASS", 0), ("I01", "INFO", 16182),   # 2019-03
            ("R07", "BLOCK", 145), ("R14", "WARN", 2), ("R10.1", "PASS", 0), ("R12", "PASS", None)]  # 2019-08
    assert run_report.dq_summary(rows) == (3, 3, 1, "R07 BLOCK 145; R07 WARN 80; R14 WARN 3")
    assert run_report.dq_summary([("R10.1", "PASS", 0)]) == (1, 0, 0, None)
    assert run_report.dq_summary([]) == (0, 0, 0, None)


def test_run_months_from_dag_params():
    start = datetime(2019, 3, 1, tzinfo=timezone.utc)
    assert alerts.run_months({"month": None, "force": False}, start) == ["2019-03"]             # dag_monthly theo lịch
    assert alerts.run_months({"month": "2019-08"}, start) == ["2019-08"]                        # chạy lại một tháng
    assert alerts.run_months({"start_month": "2018-11", "end_month": "2019-01", "clean": True}, start) == \
        ["2018-11", "2018-12", "2019-01"]                                                       # dag_rebuild
    assert alerts.run_months({"force": True}, start) == []                                      # dag_reference
    assert alerts.run_months({"retain_days": 7}) == []                                          # dag_maintenance


def test_alert_text_and_telegram_is_off_without_config(monkeypatch):
    text = alerts.alert_text("dag_monthly", "dq_silver", "scheduled__2019-08-01", "2019-08", "BLOCKED",
                             "2019-08 R07 BLOCK 145/118905", try_number=1)
    assert text.splitlines() == ["[Lakehouse] dag_monthly 2019-08: BLOCKED", "task: dq_silver (lần thử 1)",
                                 "run: scheduled__2019-08-01", "2019-08 R07 BLOCK 145/118905"]
    assert alerts.alert_text("dag_maintenance", "pan_scan", "r1", None, "FAILED", "lỗi").startswith(
        "[Lakehouse] dag_maintenance: FAILED\n")
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "1")
    assert alerts.send_telegram(text) is False          # thiếu token: không gọi ra ngoài


def test_send_telegram_posts_the_alert_to_the_bot_api(monkeypatch):
    """Gửi thật tới một máy chủ HTTP cục bộ đóng vai api.telegram.org."""
    received = {}

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            received["path"] = self.path
            received["body"] = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'{"ok": true}')

        def log_message(self, *args):
            pass

    server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.handle_request, daemon=True).start()
    monkeypatch.setattr(alerts, "TELEGRAM_URL", f"http://127.0.0.1:{server.server_port}/bot{{token}}/sendMessage")
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "123:abc")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "-100")
    try:
        assert alerts.send_telegram("[Lakehouse] dag_monthly 2019-08: BLOCKED") is True
    finally:
        server.server_close()
    assert received == {"path": "/bot123:abc/sendMessage",
                        "body": {"chat_id": "-100", "text": "[Lakehouse] dag_monthly 2019-08: BLOCKED"}}


def test_notify_failure_alerts_even_when_the_run_report_cannot_be_written(monkeypatch, caplog):
    """Spark hỏng (nguyên nhân có thể của chính lỗi đang báo) không được làm mất cảnh báo."""
    def broken(*args, **kwargs):
        raise ConnectionError("spark-connect không phản hồi")
    monkeypatch.setattr(run_report, "record_failure", broken)
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    now = datetime(2026, 11, 12, tzinfo=timezone.utc)
    context = {"ti": SimpleNamespace(dag_id="dag_monthly", task_id="extract_to_bronze", run_id="r1", try_number=3),
               "params": {"month": "2019-03"}, "exception": RuntimeError("nguồn không có giao dịch"),
               "dag_run": SimpleNamespace(start_date=now, run_after=now)}
    with caplog.at_level("ERROR", logger="alerts"):
        alerts.notify_failure(context)
    assert "[ALERT] [Lakehouse] dag_monthly 2019-03: FAILED" in caplog.text
    assert "task: extract_to_bronze (lần thử 3)" in caplog.text and "nguồn không có giao dịch" in caplog.text


# ---------------------------------------------------------------- kiểm chứng dựng lại

def test_compare_before_and_after_rebuild():
    before = {"2015-01": (100, Decimal("5.00"), 11), "2015-02": (90, Decimal("4.00"), 22),
              "2015-03": (80, Decimal("3.00"), 33), "2015-04": (70, Decimal("2.00"), 44)}
    after = {"2015-01": (100, Decimal("5.00"), 11),       # giống hệt
             "2015-02": (90, Decimal("4.00"), 99),        # cùng số dòng, tổng tiền nhưng nội dung khác
             "2015-03": (80, Decimal("3.01"), 33),        # lệch tổng tiền
             "2015-05": (60, Decimal("1.00"), 55)}        # 2015-04 mất sau khi dựng lại; 2015-05 mới
    months = h.iter_months("2015-01", "2015-05")
    assert rebuild.compare(before, after, months) == {
        "2015-01": "same", "2015-02": "content", "2015-03": "diff", "2015-04": "diff", "2015-05": "new"}
    assert set(rebuild.compare({}, after, months).values()) == {"new"}      # nạp lần đầu: không có gì để so


def test_totals_group_by_month_and_ignore_technical_columns(spark):
    rows = [(1, datetime(2015, 1, 3, 10), Decimal("10.00"), "2015-01", datetime(2026, 1, 1)),
            (2, datetime(2015, 1, 31, 23, 59), Decimal("-2.50"), "2015-01", datetime(2026, 1, 1)),
            (3, datetime(2015, 2, 1), Decimal("7.00"), "2015-02", datetime(2026, 1, 1)),
            (4, datetime(2015, 3, 1), Decimal("1.00"), "2015-03", datetime(2026, 1, 1))]   # ngoài khoảng
    schema = "transaction_id bigint, transaction_ts timestamp_ntz, amount decimal(12,2), _batch_id string, _updated_at timestamp"
    df = spark.createDataFrame(rows, schema)
    months = ["2015-01", "2015-02"]
    got = rebuild.totals(df, h.SILVER_TXN, months)
    assert {m: v[:2] for m, v in got.items()} == {"2015-01": (2, Decimal("7.50")), "2015-02": (1, Decimal("7.00"))}
    # dựng lại đổi _updated_at, _batch_id nhưng không đổi checksum; đổi một giá trị nghiệp vụ thì đổi
    rebuilt = spark.createDataFrame([(*r[:3], "rebuild", datetime(2026, 11, 12)) for r in rows], schema)
    assert rebuild.totals(rebuilt, h.SILVER_TXN, months) == got
    changed = spark.createDataFrame([(*r[:2], r[2] + (r[0] == 1), *r[3:]) for r in rows], schema)
    assert rebuild.compare(got, rebuild.totals(changed, h.SILVER_TXN, months), months) == \
        {"2015-01": "diff", "2015-02": "same"}

    kpi = spark.createDataFrame([(date(2015, 1, 1), 2, Decimal("7.50")), (date(2015, 3, 1), 1, Decimal("1.00"))],
                                "month date, txn_count bigint, total_amount decimal(18,2)")
    assert {m: v[:2] for m, v in rebuild.totals(kpi, h.MARTS["monthly_kpi"], months).items()} == \
        {"2015-01": (2, Decimal("7.50"))}


# ---------------------------------------------------------------- bảo trì

class FakeSpark:
    def __init__(self, rows):
        self.rows, self.queries = rows, []

    def sql(self, query):
        self.queries.append(query)
        return SimpleNamespace(collect=lambda: self.rows)


def test_maintenance_calls_procedure_of_the_table_catalog():
    now = datetime(2026, 11, 12, 13, 30, tzinfo=timezone.utc)
    assert im.cutoff(7, now) == "TIMESTAMP '2026-11-05 13:30:00'"
    assert im.cutoff(0, now) == "TIMESTAMP '2026-11-12 13:30:00'"

    spark = FakeSpark([SimpleNamespace(rewritten_data_files_count=2462, added_data_files_count=1,
                                       rewritten_bytes_count=8 * 2**20)])
    assert im.rewrite_data_files(spark, "ops.pipeline.dq_results", None) == "gộp 2462 file thành 1 (8.0 MiB)"
    assert spark.queries == ["CALL ops.system.rewrite_data_files(table => 'pipeline.dq_results')"]

    spark = FakeSpark([SimpleNamespace(orphan_file_location="s3://gold/x")] * 3)
    args = SimpleNamespace(orphan_days=3)
    assert im.remove_orphan_files(spark, "gold.card.fact_transaction", args) == "xóa 3 file mồ côi"
    assert spark.queries[0].startswith("CALL gold.system.remove_orphan_files(table => 'card.fact_transaction', "
                                       "older_than => TIMESTAMP '")
    assert spark.queries[0].endswith("prefix_listing => true)")
    assert im.remove_orphan_files(FakeSpark([]), "gold.card.dim_date", args) is None    # không đổi: không in


def test_maintenance_continues_after_a_failing_table(capsys):
    class Flaky(FakeSpark):
        def sql(self, query):
            if "dim_date" in query:
                raise RuntimeError("catalog lỗi\nJVM stacktrace: …")
            return super().sql(query)

    spark = Flaky([SimpleNamespace(rewritten_manifests_count=99, added_manifests_count=1)])
    tables = ["gold.card.dim_date", "ops.pipeline.dq_results"]
    assert im.run(spark, "rewrite_manifests", tables, None) == ["gold.card.dim_date"]
    out = capsys.readouterr().out
    assert "rewrite_manifests gold.card.dim_date: LỖI RuntimeError: catalog lỗi" in out and "JVM" not in out
    assert "rewrite_manifests ops.pipeline.dq_results: gộp 99 manifest thành 1" in out
    assert "2 bảng, 1 bảng có thay đổi, 1 bảng lỗi" in out
