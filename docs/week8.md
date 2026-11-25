# Tuần 8 – Airflow end-to-end

Demo cuối mốc: pipeline tự chạy.
- Bật `dag_monthly` là scheduler chạy lần lượt 10 tháng 2019-01 → 2019-10, mỗi tháng 9 task từ kiểm tra nguồn đến báo cáo lần chạy. Cả 10 lần chạy thành công.
- `dag_rebuild` xóa Silver giao dịch, fact và mart của 108 tháng 2010-01 → 2018-12 (12.145.949 giao dịch), dựng lại từ Bronze trong một lần chạy, và kết quả trùng khớp với trước khi xóa ở cả 108 tháng: số dòng, tổng tiền và checksum của Silver, fact, `mart_monthly_kpi`.
- `dag_maintenance` chạy hằng tuần: gộp file, gộp manifest, xóa snapshot cũ, xóa file mồ côi trên mọi bảng của bốn tầng, rồi R13 quét mọi object của bốn bucket. Trên dữ liệu thật R13 PASS; khi cố ý ghi một số thẻ vào một bảng thì R13 BLOCK và job Gold dừng.
- Từ một stack trống (volume mới), chuỗi lệnh ở mục Cách dùng chạy một mạch không lỗi: `dag_reference`, nạp lịch sử 108 tháng (21 phút), catchup 10 tháng, bảo trì. Kết quả đủ 13.305.915 dòng fact, R11 PASS cả 118 tháng, R13 PASS.
- Task thất bại thì `on_failure_callback` ghi `ops.pipeline.pipeline_runs` và gửi cảnh báo. Bốn tình huống lỗi đã thử đều được xử lý đúng (mục Thử lỗi).

## Sản phẩm

| Sản phẩm | File |
|---|---|
| `dag_monthly` đủ 9 task (thêm `publish_run_report`), bắt đầu từ 2019-01 | [airflow/dags/dag_monthly.py](../airflow/dags/dag_monthly.py) |
| `dag_rebuild`: dựng lại Silver, Gold từ Bronze cho một khoảng tháng; nạp lịch sử | [airflow/dags/dag_rebuild.py](../airflow/dags/dag_rebuild.py), [spark/maintenance/rebuild.py](../spark/maintenance/rebuild.py) |
| `dag_maintenance` hằng tuần | [airflow/dags/dag_maintenance.py](../airflow/dags/dag_maintenance.py) |
| Task dùng chung của các DAG, tham số mặc định | [airflow/dags/pipeline_tasks.py](../airflow/dags/pipeline_tasks.py) |
| Cảnh báo `on_failure_callback` (log, `pipeline_runs`, Telegram) | [airflow/dags/alerts.py](../airflow/dags/alerts.py) |
| Bảng `ops.pipeline.pipeline_runs`: báo cáo lần chạy | [spark/quality/run_report.py](../spark/quality/run_report.py), quy ước ở [data_model.md](data_model.md) mục 4 |
| R13: quét số thẻ trong bucket | [spark/quality/pan_scan.py](../spark/quality/pan_scan.py) |
| Bảo trì Iceberg: 4 thủ tục trên mọi bảng | [spark/maintenance/iceberg_maintenance.py](../spark/maintenance/iceberg_maintenance.py) |
| Thống kê kích thước bảng, bảng `ops.pipeline.table_stats` | [spark/maintenance/storage_report.py](../spark/maintenance/storage_report.py) (`--record`) |
| Gold dừng khi R13 BLOCK | [spark/transform/gold_fact.py](../spark/transform/gold_fact.py) (`check_ready`), [spark/quality/rules.py](../spark/quality/rules.py) (`R13`, `latest_result`) |
| Tài khoản MinIO `pan-scanner` chỉ đọc bốn bucket | [infra/minio/init.sh](../infra/minio/init.sh), [docker-compose.yml](../docker-compose.yml), [.env.example](../.env.example) |
| Kiểm thử (33 test mới, tổng 99) | [tests/test_pan_scan.py](../tests/test_pan_scan.py), [tests/test_pipeline_ops.py](../tests/test_pipeline_ops.py), [tests/test_dq_rules.py](../tests/test_dq_rules.py) |
| Lệnh `dag-on`, `dag-off`, `dag-trigger`, `dag-runs`, `rebuild`, `runs-status`, `maintenance`, `pan-scan` | [Makefile](../Makefile) |

## Cách dùng

`.env` cần thêm `MINIO_SECRET_PAN_SCANNER` (`make secrets` in ra giá trị mới), sau đó `make up` để tạo tài khoản `pan-scanner`. Muốn nhận cảnh báo qua Telegram thì điền `TELEGRAM_BOT_TOKEN` và `TELEGRAM_CHAT_ID`.

Từ máy sạch, sau `make up` và `make seed`:

```bash
make dag-trigger DAG=dag_reference                 # customers, cards, mcc_codes đến dimension tham chiếu
make dag-runs DAG=dag_reference                    # chờ lần chạy này thành công rồi mới nạp giao dịch
make rebuild FROM=2010-01 TO=2018-12 EXTRACT=1     # nạp lịch sử: 108 tháng trong một lần chạy dag_rebuild
make dag-on DAG=dag_monthly                        # catchup 2019-01 → 2019-10, mỗi tháng một lần chạy
make dag-on DAG=dag_maintenance                    # bảo trì hằng tuần (chạy ngay một lần cho tuần gần nhất)
make dag-runs DAG=dag_monthly                      # trạng thái các lần chạy trong Airflow
make runs-status                                   # ops.pipeline.pipeline_runs: kết quả, số dòng, DQ, thời gian
```

Chạy lại, dựng lại, bảo trì bằng tay:

```bash
make dag-trigger DAG=dag_monthly CONF='{"month": "2019-05"}'     # chạy lại một tháng
make rebuild FROM=2018-01 TO=2018-12 CLEAN=1                     # xóa rồi dựng lại cả năm, kiểm chứng trùng khớp
make rebuild FROM=2018-01 TO=2018-12                             # chỉ đồng bộ lại phần khác biệt
make maintenance                                                 # 4 thủ tục trên mọi bảng, giữ snapshot 7 ngày
make maintenance TABLE=ops.pipeline.dq_results RETAIN_DAYS=0 RETAIN_LAST=1   # một bảng, xóa hết snapshot cũ
make pan-scan                                                    # R13
make storage-status                                              # dung lượng bucket và từng bảng
```

`airflow dags test` vẫn chạy được như ở [tuần 6](week6.md) và không cần bật DAG. Với `dag_monthly`, ngày logic phải nằm trong khoảng 2019-01-01 → 2019-10-01:

```bash
docker compose exec airflow-scheduler airflow dags test dag_monthly 2019-08-15 -c '{"month": "2019-08", "block_ratio": 0.001}'
docker compose exec airflow-scheduler airflow dags test dag_rebuild -c '{"start_month": "2015-01", "end_month": "2015-03", "clean": true}'
```

## Cách hoạt động

**dag_monthly** (Hình 5 Design Doc) thêm task cuối và cảnh báo:

| Task | Việc | Thất bại khi |
|---|---|---|
| 8 task của tuần 6–7 | Từ `check_source_month` đến `reconcile_silver_gold` | Như cũ |
| `publish_run_report` | Ghi một dòng SUCCESS vào `ops.pipeline.pipeline_runs`: số dòng của tháng ở nguồn, Bronze, Silver, quarantine, fact; số rule PASS/WARN/BLOCK; thời gian chạy | Lỗi ghi |
| `on_failure_callback` (mọi task, mọi DAG) | Khi một task thất bại sau khi hết lượt thử lại: ghi một dòng BLOCKED hoặc FAILED vào `pipeline_runs`, ghi dòng `[ALERT]` vào log, gửi Telegram nếu đã cấu hình | Không làm task thất bại thêm: lỗi của callback chỉ được ghi log |

- `start_date` là 2019-01-01, `end_date` 2019-10-01, `catchup=True`: bật DAG là có đúng 10 lần chạy.
- Lần chạy dừng vì rule BLOCK có `status` = BLOCKED, và `message` là rule kèm số liệu lấy từ `dq_results` của chính lần chạy đó. Lỗi khác là FAILED với thông báo lỗi của task.

**dag_rebuild** (tham số `start_month`, `end_month`, `clean`, `extract`, `block_ratio`):

| Task | Việc |
|---|---|
| `extract_to_bronze`, `reconcile_src_bronze` | Chỉ chạy khi `extract = true`: trích xuất các tháng chưa có trong Bronze, đối soát R10.1 |
| `snapshot_before` | Ghi id snapshot hiện tại của Silver giao dịch, fact, `mart_monthly_kpi` vào XCom |
| `clean_range` | Chỉ chạy khi `clean = true`: kiểm tra mọi tháng đã có Bronze SUCCESS, rồi xóa các tháng khỏi Silver giao dịch, fact và 6 mart |
| `silver_transform` → `reconcile_silver_gold` | Cùng các job như `dag_monthly`, nhưng nhận cả khoảng tháng, mỗi bước một Spark session |
| `verify_rebuild` | So từng tháng của 3 bảng với chính bảng đó ở snapshot đã ghi (time travel): số dòng, tổng amount, checksum các cột nghiệp vụ. Lệch số dòng hoặc tổng tiền thì thất bại |
| `publish_run_report` | Một dòng cho cả khoảng, `batch_id` dạng `2010-01..2018-12` |

**dag_maintenance** (`@weekly`, tham số `retain_days` = 7, `retain_last` = 3, `orphan_days` = 3):

| Task | Việc |
|---|---|
| `rewrite_data_files` | Gộp file nhỏ trong từng partition (binpack). Có tác dụng với bảng chỉ ghi thêm (`dq_results`, `ingestion_log`, `pipeline_runs`); bảng partition theo tháng chỉ có một file mỗi partition |
| `rewrite_manifests` | Gộp manifest |
| `expire_snapshots` | Xóa snapshot cũ hơn `retain_days` ngày, giữ ít nhất `retain_last` snapshot; xóa file không còn snapshot nào dùng |
| `remove_orphan_files` | Xóa file trong thư mục bảng mà metadata không tham chiếu, cũ hơn `orphan_days` ngày |
| `pan_scan` | R13 |
| `table_stats` | Ghi số dòng, dung lượng, số file, số snapshot của từng bảng vào `ops.pipeline.table_stats` |

Một bảng lỗi không dừng các bảng còn lại; cuối task job thoát với mã lỗi nếu có bảng lỗi.

**R13** ([pan_scan.py](../spark/quality/pan_scan.py)):
1. Lấy tập `card_token` từ `bronze.card.cards_raw`.
2. Liệt kê và đọc mọi object của bucket `bronze`, `silver`, `gold`, `ops` bằng tài khoản `pan-scanner`. File Parquet được giải mã từng cột; manifest Avro và metadata JSON nén gzip được giải nén rồi tìm trên byte thô.
3. Với mỗi dãy từ 13 chữ số liền nhau, mọi chuỗi con dài 13–19 là một ứng viên. Ứng viên là số thẻ thật khi `HMAC-SHA256(HMAC_SECRET, ứng viên)` trùng một `card_token`.
4. Ghi một dòng R13 vào `dq_results` với `batch_id` = `scan_YYYYMMDD`. Object có số thẻ, có cột tên `card_number` hoặc `cvv`, hoặc không đọc được đều tính là vi phạm. Có vi phạm thì BLOCK và task thất bại.
5. Khi kết quả R13 gần nhất là BLOCK, `gold_fact.py` không dựng fact (chặn công bố), cho tới khi một lần quét sau PASS.

## Thử lỗi

| Tình huống | Cách thử | Kết quả |
|---|---|---|
| Nguồn chưa có dữ liệu | Chạy `dag_monthly` với `month` = 2019-11 (rút thời gian chờ của sensor xuống 10 giây) | `check_source_month` hết thời gian chờ, thất bại ngay không thử lại; các task sau không chạy; `pipeline_runs` có dòng FAILED |
| Rule BLOCK | `dag_monthly` tháng 2019-08 với `block_ratio` = 0.001 | R07 (0,12%) thành BLOCK, `dq_silver` thất bại, Gold không được dựng; `pipeline_runs` có dòng BLOCKED kèm `R07 BLOCK 145/118905`. Chạy lại với ngưỡng mặc định thì tháng đi tiếp |
| Task chết giữa chừng | Kill tiến trình `gold_fact.py` khi đang ghi fact tháng 2019-05 | `gold_build` thất bại lần 1, Airflow chạy lại sau 2 phút và thành công; R11 PASS; không có cảnh báo vì lần chạy vẫn thành công |
| Số thẻ lọt vào Lakehouse | Ghi một số thẻ của nguồn vào bảng tạm `ops.pipeline.r13_probe` rồi chạy `dag_maintenance` | `pan_scan` báo đúng object, số thẻ chỉ hiện dạng `393314…8401`; R13 BLOCK, `pipeline_runs` có dòng BLOCKED; `gold_fact.py` từ chối chạy. Xóa bảng, quét lại thì PASS |

## Quyết định thiết kế

1. **Lịch sử nạp bằng `dag_rebuild`, `dag_monthly` chỉ catchup năm 2019.** Một lần chạy `dag_monthly` mất 70–115 giây cho một tháng, phần lớn là khởi động 9 task và tính lại merchants trên toàn bảng, nên catchup đủ 118 tháng mất khoảng 3 giờ. `dag_rebuild` gọi các job với cả khoảng tháng, mỗi bước một Spark session: 12 tháng mất 4 phút, 108 tháng mất 28 phút. Mười tháng 2019 vẫn đi qua `dag_monthly`, đủ để minh họa nạp tăng dần như mục 3.3 Design Doc. Muốn catchup cả 118 tháng bằng `dag_monthly` thì chỉ cần đổi `start_date` về 2010-01-01.
2. **R13 đọc object trong bucket, không đọc qua catalog.** Quét qua Spark chỉ thấy snapshot hiện tại của các bảng. Số thẻ lọt vào một lần ghi cũ vẫn nằm trong file của snapshot cũ, và giá trị nhỏ nhất, lớn nhất của cột còn nằm trong manifest. Đọc thẳng object thì thấy hết những gì còn lưu. Đổi lại cần một tài khoản MinIO riêng, `pan-scanner`, chỉ có quyền đọc.
3. **Ứng viên là mọi chuỗi con 13–19 chữ số, xác nhận bằng HMAC.** Số thẻ có thể dính liền với chữ số khác, nên không chỉ xét dãy có độ dài đúng 13–19. So HMAC với `card_token` thì không cần số thẻ gốc và không báo nhầm: mỗi lần quét có hàng chục nghìn dãy chữ số (id snapshot, checksum, đoạn chữ số trong token) mà không dãy nào bị báo.
4. **Lần chạy thất bại do callback ghi, không dùng task `publish_run_report` với `trigger_rule="all_done"`.** Trạng thái của một lần chạy DAG lấy theo task lá. Nếu task báo cáo luôn chạy và thành công thì lần chạy có task lỗi vẫn hiện là thành công.
5. **BLOCKED xét theo `run_id`.** Chỉ rule BLOCK do chính lần chạy này ghi mới biến thất bại thành BLOCKED. Một BLOCK cũ của tháng đó không làm lỗi kỹ thuật của lần chạy sau bị gắn nhầm nhãn.
6. **`verify_rebuild` so bằng time travel, không lưu số liệu trung gian.** Snapshot trước khi dựng lại vẫn còn trong bảng, nên chỉ cần truyền id snapshot qua XCom. Lệch checksum mà khớp số dòng và tổng tiền chỉ là cảnh báo: sau khi dữ liệu tham chiếu đổi (VD năm sinh của khách), fact dựng lại khác nội dung một cách hợp lệ, còn tiêu chí nghiệm thu là số dòng và tổng tiền theo tháng.
7. **`clean` giữ nguyên dimension.** Khóa thay thế phụ thuộc thứ tự nạp ([tuần 7](week7.md), quyết định 3). Dựng lại dimension từ đầu cho một khoảng tháng sẽ làm fact của các tháng ngoài khoảng trỏ sai.
8. **Giữ snapshot 7 ngày và ít nhất 3 snapshot; file mồ côi phải cũ hơn 3 ngày.** Bảy ngày đủ để quay lại trạng thái trước một lần chạy lỗi trong tuần. Iceberg không cho xóa file mồ côi mới hơn 24 giờ, vì file của một job đang ghi chưa được metadata tham chiếu.
9. **Dọn trước, quét sau.** `pan_scan` đứng sau `expire_snapshots` và `remove_orphan_files`, nên R13 kết luận về đúng những gì còn nằm trong bucket.
10. **Cảnh báo gồm bảng `pipeline_runs`, log và Telegram tùy chọn.** Hình 5 ghi email hoặc Telegram. Telegram chỉ cần một lệnh HTTP, không cần máy chủ SMTP. Không cấu hình thì cảnh báo vẫn nằm trong `pipeline_runs`, là nguồn của dashboard D4.

## Lưu ý

- **Bật `dag_maintenance` là scheduler chạy ngay một lần** cho tuần gần nhất, dù `catchup=False`.
- **Sau khi gộp file, dung lượng bucket chưa giảm ngay.** File cũ còn được snapshot trước đó tham chiếu, đến khi snapshot hết hạn sau 7 ngày mới bị xóa. Lần bảo trì đầu tiên: `dq_results` từ 2.462 file còn 1 file, `fact_transaction` từ 140 còn 19 snapshot, bucket `gold` từ 2.582 còn 1.588 object.
- **`expire_snapshots` làm mất khả năng time travel về snapshot đã xóa.** Các kịch bản time travel, rollback ở tuần 10 phải chạy trong vòng 7 ngày kể từ lúc tạo snapshot, hoặc tạm dừng `dag_maintenance`. `verify_rebuild` cũng cần snapshot trước khi dựng lại còn tồn tại.
- **Trong lúc `dag_rebuild` chạy với `clean = true`, Silver và Gold thiếu các tháng đang dựng lại.** Truy vấn và dashboard trên các tháng đó cho kết quả thiếu cho tới khi DAG xong.
- **`DROP TABLE … PURGE` qua Spark báo lỗi nhưng bảng và file đã bị xóa.** Lakekeeper xóa bảng và dọn file, sau đó Spark đọc lại metadata để tự dọn thì không còn thấy bảng. Cần tính đến khi viết `erase_customer.py` ở tuần 10 ([tuần 2](week2.md), lưu ý 2).
- **`airflow dags test` không áp dụng pool `spark`**, nên đừng chạy nó cùng lúc với lần chạy của scheduler.
- **Thời gian chạy trong `pipeline_runs` tính từ lúc lần chạy thật sự bắt đầu.** `airflow dags test` đặt `start_date` của lần chạy bằng ngày logic, nên job lấy giá trị lớn hơn giữa `start_date` và `run_after`.
- `pipeline_runs` hiện có các dòng BLOCKED, FAILED của những lần thử lỗi ở trên.
- Thông báo của dòng FAILED là thông báo chung của BashOperator (`Bash command failed…`); lỗi cụ thể nằm trong log của task.
- Chưa có truy vấn Q1–Q10 và dashboard (tuần 9), `make erase` và `make demo` (tuần 10).
