"""Phần dùng chung của các DAG: tham số mặc định của task và chuỗi task từ Silver đến báo cáo lần chạy.

dag_monthly gọi các job với một tháng (--month M), dag_rebuild với một khoảng tháng (--from A
--to B); các bước từ Silver trở đi giống nhau nên được dựng ở đây.
"""

from datetime import timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param

from alerts import notify_failure

JOB = "python /opt/airflow/spark"
# Mọi task Spark dùng pool spark (1 slot): job của các DAG không chạy chồng lên nhau trên cụm Spark
DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "pool": "spark",
    "on_failure_callback": notify_failure,
}
BLOCK_RATIO = "--block-ratio {{ params.block_ratio }}"
MONTH_PATTERN = r"^\d{4}-(0[1-9]|1[0-2])$"


def block_ratio_param():
    return Param(0.01, type="number", minimum=0, maximum=1,
                 description="tỷ lệ vi phạm của rule xét từng bản ghi mà vượt quá thì BLOCK")


def transform_tasks(scope):
    """silver_transform → dq_silver → gold_build → refresh_marts → reconcile_silver_gold cho phạm vi
    scope ('--month M' hoặc '--from A --to B'). Gọi trong khối `with DAG`; trả về danh sách task đã nối."""
    tasks = [
        BashOperator(task_id="silver_transform",
                     bash_command=f"{JOB}/transform/silver_transactions.py {scope} {BLOCK_RATIO}"),
        BashOperator(task_id="dq_silver",
                     bash_command=f"{JOB}/quality/rules.py transactions {scope} {BLOCK_RATIO}",
                     retries=0),            # BLOCK là kết quả của dữ liệu, chạy lại không đổi
        BashOperator(task_id="gold_build", bash_command=f"{JOB}/transform/gold_fact.py {scope}"),
        BashOperator(task_id="refresh_marts", bash_command=f"{JOB}/transform/marts.py {scope}"),
        BashOperator(task_id="reconcile_silver_gold",
                     bash_command=f"{JOB}/quality/reconcile.py silver-gold {scope}", retries=0),
    ]
    for up, down in zip(tasks, tasks[1:]):
        up >> down
    return tasks


# Lúc lần chạy thật sự bắt đầu. `airflow dags test` đặt start_date bằng ngày logic (VD 2019-03-15) và
# run_after bằng lúc gọi lệnh; lần chạy của scheduler thì start_date là lúc bắt đầu, run_after là
# cuối khoảng dữ liệu hoặc lúc trigger. Giá trị lớn hơn trong hai giá trị đúng cho cả hai trường hợp.
RUN_STARTED = "{{ ([dag_run.start_date, dag_run.run_after] | max).isoformat() }}"


def publish_run_report(scope):
    """Task cuối: ghi một dòng SUCCESS vào ops.pipeline.pipeline_runs. Lần chạy thất bại do
    on_failure_callback ghi (alerts.py)."""
    return BashOperator(
        task_id="publish_run_report",
        bash_command=f"{JOB}/quality/run_report.py {scope} --started-at '{RUN_STARTED}'",
    )
