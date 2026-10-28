"""dag_reference: dữ liệu tham chiếu (mục 8 Design Doc), chạy thủ công hoặc khi nguồn thay đổi.

customers, cards (token hóa) và mcc_codes → Bronze → Silver → R09. Batch là ref_YYYYMMDD theo
ngày chạy; batch đã nạp thành công thì bỏ qua, trừ khi bật force. dq_reference thất bại khi có
rule BLOCK.
"""

from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param

JOB = "python /opt/airflow/spark"
FORCE = "{{ '--force' if params.force else '' }}"

with DAG(
    dag_id="dag_reference",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2), "pool": "spark"},
    params={
        "force": Param(False, type="boolean", description="nạp lại Bronze dù batch hôm nay đã nạp"),
        "block_ratio": Param(0.01, type="number", minimum=0, maximum=1,
                             description="tỷ lệ vi phạm mà vượt quá thì BLOCK"),
    },
    tags=["pipeline"],
) as dag:
    bronze_customers_cards = BashOperator(
        task_id="bronze_customers_cards",
        bash_command=f"{JOB}/extract/extract_jdbc.py reference {FORCE}",
    )
    bronze_mcc_codes = BashOperator(
        task_id="bronze_mcc_codes",
        bash_command=f"{JOB}/extract/load_files.py mcc {FORCE}",
    )
    silver_reference = BashOperator(
        task_id="silver_reference",
        bash_command=f"{JOB}/transform/silver_reference.py",
    )
    dq_reference = BashOperator(
        task_id="dq_reference",
        bash_command=f"{JOB}/quality/rules.py reference --block-ratio {{{{ params.block_ratio }}}}",
        retries=0,
    )

    [bronze_customers_cards, bronze_mcc_codes] >> silver_reference >> dq_reference
