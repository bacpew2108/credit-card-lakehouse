"""dag_monthly: luồng xử lý một tháng giao dịch (mục 8 và Hình 5 Design Doc), từ nguồn đến Gold.

check_source_month → extract_to_bronze → reconcile_src_bronze → silver_transform → dq_silver
→ gold_build → refresh_marts → reconcile_silver_gold

Mỗi lần chạy xử lý tháng M = data_interval_start (CronDataIntervalTimetable: lần chạy của
tháng M bắt đầu sau khi tháng M kết thúc). Tham số month chạy lại một tháng bất kỳ khi trigger
thủ công. Mọi task idempotent: Bronze bỏ qua tháng đã nạp (force để nạp lại), Silver chỉ MERGE
phần thay đổi, fact và mart ghi đè đúng partition của tháng. dq_silver thất bại khi có rule
BLOCK, nên Gold không được dựng; reconcile_silver_gold thất bại khi R11 lệch.
"""

from datetime import datetime, timedelta

from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, CronDataIntervalTimetable, Param

JOB = "python /opt/airflow/spark"
MONTH = "{{ params.month or data_interval_start.strftime('%Y-%m') }}"
BLOCK_RATIO = "--block-ratio {{ params.block_ratio }}"

with DAG(
    dag_id="dag_monthly",
    schedule=CronDataIntervalTimetable("@monthly", timezone="UTC"),
    start_date=datetime(2010, 1, 1),
    end_date=datetime(2019, 10, 1),      # tháng cuối của bộ dữ liệu
    catchup=True,
    max_active_runs=1,                   # merchants tính lại trên toàn bảng; R12 cần các tháng trước
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2), "pool": "spark"},
    params={
        "month": Param(None, type=["null", "string"], pattern=r"^\d{4}-(0[1-9]|1[0-2])$",
                       description="YYYY-MM; để trống thì lấy tháng của data_interval_start"),
        "force": Param(False, type="boolean", description="nạp lại Bronze dù tháng đã nạp thành công"),
        "block_ratio": Param(0.01, type="number", minimum=0, maximum=1,
                             description="tỷ lệ vi phạm của rule xét từng bản ghi mà vượt quá thì BLOCK"),
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
    silver_transform = BashOperator(
        task_id="silver_transform",
        bash_command=f"{JOB}/transform/silver_transactions.py --month {MONTH} {BLOCK_RATIO}",
    )
    dq_silver = BashOperator(
        task_id="dq_silver",
        bash_command=f"{JOB}/quality/rules.py transactions --month {MONTH} {BLOCK_RATIO}",
        retries=0,                       # BLOCK là kết quả, chạy lại không đổi
    )

    gold_build = BashOperator(
        task_id="gold_build",
        bash_command=f"{JOB}/transform/gold_fact.py --month {MONTH}",
    )
    refresh_marts = BashOperator(
        task_id="refresh_marts",
        bash_command=f"{JOB}/transform/marts.py --month {MONTH}",
    )
    reconcile_silver_gold = BashOperator(
        task_id="reconcile_silver_gold",
        bash_command=f"{JOB}/quality/reconcile.py silver-gold --month {MONTH}",
        retries=0,
    )

    (check_source_month >> extract_to_bronze >> reconcile_src_bronze >> silver_transform >> dq_silver
     >> gold_build >> refresh_marts >> reconcile_silver_gold)
