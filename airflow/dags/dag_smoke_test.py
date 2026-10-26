"""Kiểm chứng Airflow → python job (client Spark Connect) → Iceberg/Lakekeeper (tuần 2)."""

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

with DAG(
    dag_id="dag_smoke_test",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["smoke"],
) as dag:
    BashOperator(
        task_id="spark_write_iceberg",
        bash_command="python /opt/airflow/spark/smoke_test.py write",
    )
