"""dag_rebuild: dựng lại Silver và Gold từ Bronze cho một khoảng tháng (mục 8 Design Doc), chạy thủ công.

extract_to_bronze → reconcile_src_bronze → snapshot_before → clean_range → silver_transform
→ dq_silver → gold_build → refresh_marts → reconcile_silver_gold → verify_rebuild → publish_run_report

Các job nhận cả khoảng tháng (--from start_month --to end_month), mỗi bước một Spark session, nên
nhanh hơn nhiều so với chạy dag_monthly từng tháng. Ba cách dùng:

- Kiểm chứng tính tái lập (clean = true): xóa các tháng khỏi Silver giao dịch, fact và mart, dựng
  lại từ Bronze, rồi verify_rebuild so từng tháng với snapshot trước khi xóa (time travel): số dòng,
  tổng amount, checksum. Lệch thì task thất bại.
- Đồng bộ lại (mặc định): không xóa gì. Silver MERGE phần khác biệt so với Bronze, fact và mart ghi
  đè theo tháng. Dùng sau khi dữ liệu tham chiếu hoặc logic biến đổi thay đổi (VD thu nhập của
  khách đổi thì mart phân khúc của các tháng cũ phải tính lại).
- Nạp lịch sử (extract = true): trích xuất các tháng chưa có trong Bronze và đối soát R10.1 trước,
  rồi dựng Silver và Gold. Lịch sử 2010-01 → 2018-12 được nạp bằng cách này, dag_monthly chạy tiếp
  từ 2019-01. Cần chạy dag_reference trước để có dữ liệu tham chiếu.
"""

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param

from pipeline_tasks import (DEFAULT_ARGS, JOB, MONTH_PATTERN, block_ratio_param, publish_run_report,
                            transform_tasks)

RANGE = "--from {{ params.start_month }} --to {{ params.end_month }}"


def when(param, command, otherwise):
    """Lệnh chỉ chạy khi tham số boolean của DAG bật; tắt thì task in lý do bỏ qua và thành công."""
    return f"{{% if params.{param} %}}{command}{{% else %}}echo '{otherwise}'{{% endif %}}"


with DAG(
    dag_id="dag_rebuild",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={
        "start_month": Param(type="string", pattern=MONTH_PATTERN, description="tháng đầu, YYYY-MM"),
        "end_month": Param(type="string", pattern=MONTH_PATTERN, description="tháng cuối, YYYY-MM"),
        "clean": Param(False, type="boolean",
                       description="xóa các tháng khỏi Silver giao dịch, fact, mart trước khi dựng lại"),
        "extract": Param(False, type="boolean",
                         description="trích xuất các tháng chưa có trong Bronze trước khi dựng (nạp lịch sử)"),
        "block_ratio": block_ratio_param(),
    },
    tags=["pipeline"],
) as dag:
    skip_extract = "extract = false: dựng lại từ Bronze đang có, không đọc nguồn"
    extract_to_bronze = BashOperator(
        task_id="extract_to_bronze",
        bash_command=when("extract", f"{JOB}/extract/extract_jdbc.py transactions {RANGE}", skip_extract),
    )
    reconcile_src_bronze = BashOperator(
        task_id="reconcile_src_bronze",
        bash_command=when("extract", f"{JOB}/quality/reconcile.py src-bronze {RANGE}", skip_extract),
    )
    # Dòng cuối của stdout là JSON {bảng: id snapshot}, BashOperator đẩy vào XCom cho verify_rebuild
    snapshot_before = BashOperator(
        task_id="snapshot_before",
        bash_command=f"{JOB}/maintenance/rebuild.py snapshot",
    )
    clean_range = BashOperator(
        task_id="clean_range",
        bash_command=when("clean", f"{JOB}/maintenance/rebuild.py clean {RANGE}",
                          "clean = false: giữ dữ liệu hiện có, chỉ ghi phần khác biệt"),
    )
    transform = transform_tasks(RANGE)
    verify_rebuild = BashOperator(
        task_id="verify_rebuild",
        bash_command=f"{JOB}/maintenance/rebuild.py verify {RANGE}"
                     " --before '{{ ti.xcom_pull(task_ids=\"snapshot_before\") }}'",
        retries=0,                       # lệch là kết quả, chạy lại không đổi
    )

    extract_to_bronze >> reconcile_src_bronze >> snapshot_before >> clean_range >> transform[0]
    transform[-1] >> verify_rebuild >> publish_run_report(RANGE)
