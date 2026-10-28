# Tuần 6 – Data quality, Airflow (1)

Demo cuối mốc: DQ chạy tự động.
- `dag_monthly` xử lý một tháng qua 5 task, từ kiểm tra nguồn đến DQ. Khi có rule BLOCK, task `dq_silver` thất bại và DAG dừng.
- `ops.pipeline.dq_results` có kết quả của 118 tháng. Trên dữ liệu thật, rule báo đúng các vi phạm mà profiling đã đếm: R07 = 5.788, R14 = 392, R12 cảnh báo một tháng (2019-02). Các rule còn lại bằng 0.
- Mọi lỗi cố ý trong fixture đều bị đúng rule của nó bắt.

## Sản phẩm

| Sản phẩm | File |
|---|---|
| Danh mục rule, trạng thái và ngưỡng BLOCK, R04–R09, R12, R14, ghi `dq_results`, xét chặn | [spark/quality/rules.py](../spark/quality/rules.py) |
| Đối soát R10.1 (nguồn = bronze), R10.2 (bronze = silver + quarantine) kèm số cách ly R01–R03 | [spark/quality/reconcile.py](../spark/quality/reconcile.py) |
| R01 trùng id với tháng khác; ghi R01–R03, R10.2 sau mỗi tháng | [spark/transform/silver_transforms.py](../spark/transform/silver_transforms.py) (`mark_batch`), [silver_transactions.py](../spark/transform/silver_transactions.py) |
| I01: merchant nhiều vị trí | [spark/transform/silver_merchants.py](../spark/transform/silver_merchants.py) |
| Bảng `ops.pipeline.dq_results` | [spark/common/lakehouse.py](../spark/common/lakehouse.py) (`DQ_COLS`), quy ước ở [data_model.md](data_model.md) mục 4 |
| DAG | [airflow/dags/dag_monthly.py](../airflow/dags/dag_monthly.py), [airflow/dags/dag_reference.py](../airflow/dags/dag_reference.py) |
| Pool `spark` (1 slot) | [docker-compose.yml](../docker-compose.yml) (`airflow-init`) |
| Fixture lỗi cố ý | [tests/fixtures/](../tests/fixtures/) `dq_transactions.csv`, `dq_customers.csv`, `dq_cards.csv`, `dq_mcc_codes.csv` |
| Kiểm thử (18 test mới, tổng 55) | [tests/test_dq_rules.py](../tests/test_dq_rules.py), [tests/conftest.py](../tests/conftest.py) (Spark local dùng chung) |
| Lệnh `dq`, `dq-range`, `dq-ref`, `dq-status`; `BLOCK_RATIO=` cho `silver`, `silver-range` | [Makefile](../Makefile) |

## Cách dùng

```bash
make dq-ref                              # R09 trên bảng tham chiếu
make dq MONTH=2019-03                    # R10.1, R10.2 + R01–R03, rồi R04–R08, R12, R14; dừng nếu BLOCK
make dq-range FROM=2010-01 TO=2019-10    # như trên cho nhiều tháng, mỗi bước một Spark session
make dq-status                           # tổng hợp theo rule, các batch WARN/BLOCK gần nhất
make dq MONTH=2019-08 BLOCK_RATIO=0.001  # hạ ngưỡng: R07 (0,12%) bị nâng thành BLOCK
```

Airflow (http://localhost:8080). Cả hai DAG ở trạng thái tạm dừng khi tạo. Lệnh `airflow dags test` chạy toàn bộ DAG ngay trong tiến trình CLI, không cần scheduler và không phụ thuộc trạng thái tạm dừng:

```bash
# Một tháng, đủ 5 task
docker compose exec airflow-scheduler airflow dags test dag_monthly 2019-03-15 -c '{"month": "2019-03"}'
# Kịch bản chặn: hạ ngưỡng, R07 tháng 2019-08 (0,12%) thành BLOCK, dq_silver thất bại
docker compose exec airflow-scheduler airflow dags test dag_monthly 2019-08-15 -c '{"month": "2019-08", "block_ratio": 0.001}'
# Dữ liệu tham chiếu
docker compose exec airflow-scheduler airflow dags test dag_reference
```

Với `dag_monthly`, ngày logic (đối số thứ hai) phải thỏa hai điều kiện:
- Nằm trong khoảng `start_date`–`end_date` của DAG. Ngoài khoảng đó, lần chạy được đánh dấu thành công mà không task nào chạy.
- Không phải ngày đầu tháng. Airflow giữ ràng buộc unique trên (dag_id, logical_date), và các ngày đầu tháng dành cho lần chạy catchup.

Tháng xử lý do tham số `month` quyết định. Ngoài `month` còn có tham số `force` (nạp lại Bronze) và `block_ratio`.

`dag_reference` dùng batch `ref_YYYYMMDD` theo ngày chạy. Batch đã nạp trong ngày thì được bỏ qua, trừ khi đặt `force` là true.

## Cách hoạt động

**dag_monthly** (Hình 5 Design Doc, phần đến Silver):

| Task | Việc | Thất bại khi |
|---|---|---|
| `check_source_month` | SqlSensor trên `core_card`: nguồn có giao dịch của tháng M chưa (dùng index `date`) | Quá 1 ngày chưa có dữ liệu |
| `extract_to_bronze` | `extract_jdbc.py transactions --month M`; tháng đã SUCCESS thì bỏ qua | Lỗi trích xuất, R10 lệch khi nạp |
| `reconcile_src_bronze` | R10.1: đếm lại nguồn và Bronze ngay lúc chạy | Lệch |
| `silver_transform` | Cách ly R01–R03, MERGE, ghi R01–R03, R10.2, tính lại merchants và I01 | R10.2 lệch |
| `dq_silver` | R04–R08, R14, R12; xét kết quả mới nhất của mọi rule trong tháng | Có rule BLOCK (không retry) |

- Tháng M lấy từ `data_interval_start`. DAG dùng `CronDataIntervalTimetable`, nên lần chạy của tháng M có khoảng dữ liệu `[đầu M, đầu M+1)`. Tham số `month` ghi đè giá trị này khi trigger thủ công.
- `catchup=True`, `start_date` 2010-01, `end_date` 2019-10, `max_active_runs=1`. Các tháng chạy lần lượt vì merchants được tính lại trên toàn bảng và R12 cần các tháng trước.
- Mọi task Spark dùng pool `spark` (1 slot), nên các job của `dag_monthly` và `dag_reference` không chạy chồng lên nhau trên cụm Spark. Sensor dùng `default_pool`.
- `retries = 2` cho mọi task, trừ task DQ: BLOCK là kết quả của dữ liệu, chạy lại cũng không đổi.

**Trạng thái của rule** (chi tiết ở [data_model.md](data_model.md) mục 4):

| Rule | Mức | Vi phạm | Tỷ lệ vượt ngưỡng (mặc định 1%) |
|---|---|---|---|
| R01–R03 | BLOCK | Cách ly, batch đi tiếp (WARN) | BLOCK |
| R04–R09, R14 | WARN | Ghi nhận (WARN) | BLOCK |
| R10.1, R10.2 | BLOCK | BLOCK dù chỉ lệch một dòng | – |
| R12 | WARN | WARN | Không nâng |
| I01 | INFO | INFO | Không chặn |

**Rule xét từng bản ghi** ([rules.py](../spark/quality/rules.py)):
- Mỗi rule là OR của một vài điều kiện con, ví dụ R07 gồm kênh lạ, Online nhưng city khác ONLINE, và không phải Online nhưng city = ONLINE.
- Giao dịch Silver của tháng M được join broadcast với customers, cards, mcc_codes. Mọi rule và mọi điều kiện con được đếm trong một lượt `agg`. `details` ghi số vi phạm theo từng điều kiện.
- R14 so ngày giao dịch với `acct_open_date` (ngày đầu tháng mở thẻ) và `expires_date` (ngày cuối tháng hết hạn), tương đương với việc so theo tháng.

**R12:**
- Tổng hợp số giao dịch và tổng tiền theo tháng từ Silver một lần cho cả khoảng cần xét, rồi tính trong Python.
- Tháng M vi phạm khi một trong hai chỉ số lệch khỏi trung bình của 12 tháng liền trước quá 3 độ lệch chuẩn mẫu.
- Năm 2010 chưa đủ 12 tháng trước nên không xét (`checked_count` = 0).

**R01 trùng id toàn bảng:**
- Trước khi cách ly tháng M, job chỉ đọc cột `transaction_id` của Silver ngoài tháng M. Job giữ lại các id có trong batch: id của batch nhỏ được broadcast, phép join là left semi.
- Id trùng bị gắn R01 với lý do "transaction_id đã có ở tháng khác". Bản nạp trước được giữ, bản sau vào quarantine. Vì vậy MERGE theo từng tháng không thể tạo ra hai dòng cùng id.

## Quyết định thiết kế

1. **Ngưỡng 1% áp cho cả R01–R03.** Design Doc chỉ nói nâng mức cho rule WARN. Tuy vậy, nếu một batch phải cách ly hơn 1% số dòng thì nhiều khả năng nguồn hoặc extract đã hỏng, và đi tiếp lên Gold với phần còn lại sẽ cho số liệu sai lệch. Cách ly vài dòng vẫn là WARN và batch đi tiếp. Trên dữ liệu thật, R07 cao nhất là 0,12% một tháng (2019-08) và R14 là 0,017%, nên ngưỡng 1% không chặn nhầm.
2. **R12 giữ công thức của Design Doc, không chuẩn hóa theo số ngày trong tháng.** Tổng tiền tháng 02/2019 thấp hơn trung bình 12 tháng trước 3,08σ, vì tháng 2 chỉ có 28 ngày. Nếu chia cho số ngày thì có tới 4 tháng bị báo nhầm (2011-06, 2013-06, 2015-03, 2016-02), vì số giao dịch tăng dần theo năm. R12 chỉ cảnh báo, nên một cảnh báo có giải thích được là chấp nhận được.
3. **Tách nơi tính rule và nơi quyết định dừng.**
   - R10.1 nằm ở task riêng. R01–R03 và R10.2 do job Silver ghi vì nó có sẵn số liệu. Các rule WARN nằm ở `dq_silver`.
   - `dq_silver` chỉ chặn dựa trên kết quả mới nhất của từng rule trong `dq_results`. Vì vậy chạy lại một bước bất kỳ cũng cập nhật được quyết định, và `make dq-range` chạy lại được toàn bộ rule cho lịch sử mà không phải dựng lại Silver.
4. **`dq_results` chỉ ghi thêm.** Mỗi (lần chạy, batch, rule) là một dòng, nên dashboard D4 xem được lịch sử từng lần chạy. Kết quả có hiệu lực là dòng mới nhất.
5. **Merchant nhiều vị trí là I01, mức INFO.** Design Doc yêu cầu ghi chỉ số này vào DQ. Tỷ lệ là 21,6%: nếu tính như một rule WARN thì nó luôn bị nâng thành BLOCK, nên chỉ ghi nhận.
6. **`CronDataIntervalTimetable` thay cho chuỗi `@monthly`.** Ở Airflow 3 (`create_cron_data_intervals = False`), chuỗi `@monthly` dùng `CronTriggerTimetable`. Khi đó lần chạy lúc 00:00 ngày 01/04 có ngày logic là 01/04 và khoảng dữ liệu bằng 0, nên muốn xử lý tháng 03 vừa kết thúc thì phải tự lùi một tháng. Timetable theo khoảng cho lần chạy đó `data_interval_start` = 01/03, đúng tháng cần xử lý như Hình 5.

## Lưu ý

- Chưa có R11 (Silver = fact, tuần 7) và R13 (quét số thẻ, `dag_maintenance` tuần 8). Các task `gold_build`, `refresh_marts`, `reconcile_silver_gold` thuộc tuần 7; `publish_run_report`, `on_failure_callback` thuộc tuần 8.
- **Scheduler chỉ chạy task của DAG đã bật (unpause), kể cả lần chạy do `airflow backfill create` hay `airflow dags trigger` tạo ra.** Với `catchup=True`, bật `dag_monthly` lên là scheduler tạo đủ 118 lần chạy từ 2010-01, mỗi lần khoảng một phút (Bronze bỏ qua tháng đã nạp, MERGE không ghi gì). Vì vậy ở tuần này `dag_monthly` vẫn tạm dừng và chỉ được chạy thử bằng `airflow dags test`. Cách nạp lịch sử (catchup đủ 118 tháng, hay backfill bằng `*-range` rồi chỉ chạy DAG cho các tháng 2019) sẽ chốt ở tuần 8.
