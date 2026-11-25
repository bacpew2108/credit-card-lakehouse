"""Cảnh báo khi task thất bại: on_failure_callback của mọi DAG (Hình 5 Design Doc).

Khi một task thất bại sau khi hết số lần thử lại, notify_failure:
1. Ghi một dòng vào ops.pipeline.pipeline_runs (spark/quality/run_report.py): BLOCKED nếu lần chạy
   dừng vì rule BLOCK, kèm rule và số liệu; FAILED với thông báo lỗi của task trong các trường hợp
   còn lại.
2. Ghi dòng [ALERT] vào log của task.
3. Gửi tin nhắn Telegram nếu có TELEGRAM_BOT_TOKEN và TELEGRAM_CHAT_ID trong .env.
Lỗi ở bước nào cũng chỉ được ghi log, để callback không che mất lỗi gốc của task.

File này không import airflow hay pyspark ở mức module, nên pytest chạy được trên máy host.
"""

import json
import logging
import os
import urllib.request

from common import helpers as h      # spark/common, qua PYTHONPATH của container

log = logging.getLogger(__name__)
TELEGRAM_URL = "https://api.telegram.org/bot{token}/sendMessage"


def run_months(params, data_interval_start=None):
    """Các tháng mà lần chạy xử lý, suy từ tham số của DAG: dag_rebuild có start_month, end_month;
    dag_monthly có month (để trống thì lấy tháng của data_interval_start); DAG khác không có tháng."""
    if params.get("start_month") and params.get("end_month"):
        return h.iter_months(params["start_month"], params["end_month"])
    if "month" in params:
        month = params["month"] or (data_interval_start and f"{data_interval_start:%Y-%m}")
        return [month] if month else []
    return []


def alert_text(dag_id, task_id, run_id, batch, status, message, try_number=None):
    head = f"[Lakehouse] {dag_id}" + (f" {batch}" if batch else "") + f": {status}"
    tries = f" (lần thử {try_number})" if try_number else ""
    return f"{head}\ntask: {task_id}{tries}\nrun: {run_id}\n{message}"


def send_telegram(text):
    """Gửi text tới chat đã cấu hình; chưa cấu hình thì bỏ qua. → True nếu đã gửi."""
    token, chat_id = os.environ.get("TELEGRAM_BOT_TOKEN"), os.environ.get("TELEGRAM_CHAT_ID")
    if not (token and chat_id):
        return False
    body = json.dumps({"chat_id": chat_id, "text": text[:4000]}).encode()
    req = urllib.request.Request(TELEGRAM_URL.format(token=token), body, {"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10):
        return True


def notify_failure(context):
    ti = context["ti"]
    months = run_months(context["params"], context.get("data_interval_start"))
    error = str(context.get("exception") or "task thất bại")
    status, message = "FAILED", error
    try:
        from quality import run_report
        dag_run = context["dag_run"]
        started = max(dag_run.start_date, dag_run.run_after)    # xem RUN_STARTED trong pipeline_tasks.py
        status, message = run_report.record_failure(
            ti.run_id, ti.dag_id, ti.task_id, months, error, started_at=started)
    except Exception:
        log.exception("Không ghi được ops.pipeline.pipeline_runs")
    text = alert_text(ti.dag_id, ti.task_id, ti.run_id, h.batch_label(months), status, message, ti.try_number)
    log.error("[ALERT] %s", text)
    try:
        if send_telegram(text):
            log.info("Đã gửi cảnh báo Telegram")
    except Exception:
        log.exception("Không gửi được cảnh báo Telegram")
