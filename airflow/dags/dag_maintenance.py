"""dag_maintenance: bảo trì bảng Iceberg và quét số thẻ, chạy hằng tuần (mục 8 Design Doc).

rewrite_data_files → rewrite_manifests → expire_snapshots → remove_orphan_files → pan_scan → table_stats

Bốn task đầu chạy một thủ tục Iceberg trên mọi bảng của bốn tầng (spark/maintenance/
iceberg_maintenance.py): gộp file nhỏ, gộp manifest, xóa snapshot cũ, xóa file mồ côi. pan_scan là
rule R13: quét mọi object còn lại trong bucket bronze, silver, gold, ops, thất bại (BLOCK) nếu tìm
thấy số thẻ đầy đủ. table_stats ghi kích thước từng bảng vào ops.pipeline.table_stats.

Dọn trước, quét sau: R13 quét đúng những gì còn nằm trong bucket sau khi snapshot cũ đã bị xóa.
"""

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param

from pipeline_tasks import DEFAULT_ARGS, JOB

MAINTAIN = f"{JOB}/maintenance/iceberg_maintenance.py"

with DAG(
    dag_id="dag_maintenance",
    schedule="@weekly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={
        "retain_days": Param(7, type="number", minimum=0,
                             description="expire_snapshots: xóa snapshot cũ hơn số ngày này"),
        "retain_last": Param(3, type="integer", minimum=1,
                             description="expire_snapshots: luôn giữ ít nhất số snapshot gần nhất này"),
        "orphan_days": Param(3, type="number", minimum=1,
                             description="remove_orphan_files: chỉ xóa file cũ hơn số ngày này"),
    },
    tags=["maintenance"],
) as dag:
    rewrite_data_files = BashOperator(
        task_id="rewrite_data_files", bash_command=f"{MAINTAIN} rewrite_data_files")
    rewrite_manifests = BashOperator(
        task_id="rewrite_manifests", bash_command=f"{MAINTAIN} rewrite_manifests")
    expire_snapshots = BashOperator(
        task_id="expire_snapshots",
        bash_command=f"{MAINTAIN} expire_snapshots --retain-days {{{{ params.retain_days }}}}"
                     " --retain-last {{ params.retain_last }}",
    )
    remove_orphan_files = BashOperator(
        task_id="remove_orphan_files",
        bash_command=f"{MAINTAIN} remove_orphan_files --orphan-days {{{{ params.orphan_days }}}}",
    )
    pan_scan = BashOperator(
        task_id="pan_scan",
        bash_command=f"{JOB}/quality/pan_scan.py",
        retries=0,                       # BLOCK là kết quả, chạy lại không đổi
    )
    table_stats = BashOperator(
        task_id="table_stats", bash_command=f"{JOB}/maintenance/storage_report.py --record")

    (rewrite_data_files >> rewrite_manifests >> expire_snapshots >> remove_orphan_files
     >> pan_scan >> table_stats)
