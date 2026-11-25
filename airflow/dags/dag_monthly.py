"""dag_monthly: luồng xử lý một tháng giao dịch (mục 8 và Hình 5 Design Doc), từ nguồn đến Gold.

check_source_month → extract_to_bronze → reconcile_src_bronze → silver_transform → dq_silver
→ gold_build → refresh_marts → reconcile_silver_gold → publish_run_report

Mỗi lần chạy xử lý tháng M = data_interval_start (CronDataIntervalTimetable: lần chạy của
tháng M bắt đầu sau khi tháng M kết thúc). Tham số month chạy lại một tháng bất kỳ khi trigger
thủ công. Mọi task idempotent: Bronze bỏ qua tháng đã nạp (force để nạp lại), Silver chỉ MERGE
phần thay đổi, fact và mart ghi đè đúng partition của tháng. dq_silver thất bại khi có rule
BLOCK, nên Gold không được dựng; reconcile_silver_gold thất bại khi R11 lệch. Task thất bại thì
on_failure_callback ghi ops.pipeline.pipeline_runs và gửi cảnh báo (alerts.py).

Lịch sử 2010-01 → 2018-12 được nạp một lượt bằng dag_rebuild (extract = true). DAG này bắt đầu
từ 2019-01: bật lên là catchup chạy lần lượt 10 tháng 2019-01 → 2019-10, mỗi tháng một lần chạy,
như dữ liệu mới về hằng tháng (mục 3.3 Design Doc). Muốn catchup cả 118 tháng bằng DAG này thì
đổi start_date về 2010-01-01.
"""

from datetime import datetime, timedelta

from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, CronDataIntervalTimetable, Param

from pipeline_tasks import (DEFAULT_ARGS, JOB, MONTH_PATTERN, block_ratio_param, publish_run_report,
                            transform_tasks)

MONTH = "{{ params.month or data_interval_start.strftime('%Y-%m') }}"

with DAG(
    dag_id="dag_monthly",
    schedule=CronDataIntervalTimetable("@monthly", timezone="UTC"),
    start_date=datetime(2019, 1, 1),     # các tháng trước đó do dag_rebuild nạp
    end_date=datetime(2019, 10, 1),      # tháng cuối của bộ dữ liệu
    catchup=True,
    max_active_runs=1,                   # merchants tính lại trên toàn bảng; R12 cần các tháng trước
    default_args=DEFAULT_ARGS,
    params={
        "month": Param(None, type=["null", "string"], pattern=MONTH_PATTERN,
                       description="YYYY-MM; để trống thì lấy tháng của data_interval_start"),
        "force": Param(False, type="boolean", description="nạp lại Bronze dù tháng đã nạp thành công"),
        "block_ratio": block_ratio_param(),
    },
    tags=["pipeline"],
) as dag:
    check_source_month = SqlSensor(
        task_id="check_source_month",
        conn_id="core_card",
        sql=f"""
            SELECT EXISTS (SELECT 1 FROM transactions
                           WHERE date >= '{MONTH}-01'::timestamp
                             AND date < '{MONTH}-01'::timestamp + interval '1 month')""",
        mode="reschedule",
        poke_interval=timedelta(minutes=30),
        timeout=timedelta(days=1),
        pool="default_pool",
    )
    extract_to_bronze = BashOperator(
        task_id="extract_to_bronze",
        bash_command=f"{JOB}/extract/extract_jdbc.py transactions --month {MONTH}"
                     " {{ '--force' if params.force else '' }}",
    )
    reconcile_src_bronze = BashOperator(
        task_id="reconcile_src_bronze",
        bash_command=f"{JOB}/quality/reconcile.py src-bronze --month {MONTH}",
    )
    transform = transform_tasks(f"--month {MONTH}")

    (check_source_month >> extract_to_bronze >> reconcile_src_bronze >> transform[0])
    transform[-1] >> publish_run_report(f"--month {MONTH}")
