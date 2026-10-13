# Tuần 4 – Bronze

Demo cuối mốc: 4 bảng Bronze có đủ dữ liệu; chạy lại không bị trùng dòng. `make bronze-ref` nạp 3 bảng tham chiếu, `make bronze-range FROM=2010-01 TO=2019-10` nạp 118 tháng giao dịch trong khoảng 4,5 phút.

## Sản phẩm

| Sản phẩm | File |
|---|---|
| Trích xuất JDBC: giao dịch theo tháng, customers, cards (token hóa, bỏ CVV) | [spark/extract/extract_jdbc.py](../spark/extract/extract_jdbc.py) |
| Nạp file landing (mcc_codes) | [spark/extract/load_files.py](../spark/extract/load_files.py) |
| Hàm thuần: biên tháng, câu SELECT, HMAC | [spark/common/helpers.py](../spark/common/helpers.py) |
| Session, DDL, đọc JDBC, checksum, ops.ingestion_log | [spark/common/lakehouse.py](../spark/common/lakehouse.py) |
| Kiểm thử (18 test mới, tổng 23) | [tests/test_bronze_helpers.py](../tests/test_bronze_helpers.py) |
| Lệnh `bronze-ref`, `bronze`, `bronze-range`, `bronze-status` | [Makefile](../Makefile) |

## Cách dùng

```bash
make up-core && make seed                       # cần nguồn core_card có dữ liệu (tuần 3)
make bronze-ref                                 # customers, cards, mcc_codes
make bronze MONTH=2019-03                       # một tháng
make bronze-range FROM=2010-01 TO=2019-10       # nhiều tháng trong một Spark session
make bronze-status                              # số dòng từng bảng, 10 lần nạp gần nhất
```

Batch đã nạp thành công thì bị bỏ qua; thêm `FORCE=1` để nạp lại. Job chạy bằng `spark-submit` trong container `airflow-scheduler`, giống cách các DAG ở tuần 6 sẽ gọi.

## Cách hoạt động

| Bảng | Nguồn | Cách ghi | `_batch_id` |
|---|---|---|---|
| `bronze.transactions_raw` | `core_card.transactions`, `date` thuộc tháng M | `overwritePartitions()`, partition `identity(_batch_id)` | `2019-03` |
| `bronze.customers_raw` | `core_card.customers` | `overwrite(true)`: ghi đè toàn bảng | `ref_YYYYMMDD` |
| `bronze.cards_raw` | `core_card.cards` | Ghi đè toàn bảng | `ref_YYYYMMDD` |
| `bronze.mcc_codes_raw` | `s3://landing/reference/mcc_codes/mcc_codes.jsonl` | Ghi đè toàn bảng | `ref_YYYYMMDD` |

Mỗi batch chạy qua `run_batch`:
1. Đọc `ops.ingestion_log`. Nếu lần nạp gần nhất của `(source, batch)` là SUCCESS và không có `--force` thì bỏ qua.
2. Đếm số dòng ở nguồn với cùng điều kiện lọc. Tháng không có dữ liệu thì báo lỗi.
3. Đọc qua JDBC, ép mọi cột sang `::text` ngay trong câu SQL, thêm `_ingested_at`, `_source`, `_batch_id`, rồi ghi.
4. Đọc lại phần vừa ghi để lấy số dòng và checksum, sau đó so với số dòng ở nguồn (**R10**). Nếu lệch, ghi FAILED và job thoát với mã lỗi.
5. Ghi một dòng vào `ops.ingestion_log`: run_id, source, batch_id, source_count, row_count, checksum, status, message, started_at, finished_at. Bước này chạy cả khi job lỗi.

**Bảo vệ số thẻ.** Câu SELECT bảng `cards` không có cột `cvv`, nên CVV không ra khỏi Postgres. `card_number` đi qua một Python UDF, trả về `card_token = hex(HMAC-SHA256(HMAC_SECRET, PAN))`, `card_bin` và `card_last4`. Cột PAN bị loại trước khi ghi. Job cũng `assert` rằng DataFrame ghi xuống không còn `card_number` hay `cvv`. Khóa HMAC chỉ nằm trong biến môi trường của container, không bao giờ được gửi tới nguồn.

## Kết quả kiểm chứng (2026-10-12)

| Kiểm tra | Kết quả |
|---|---|
| `make test` | ✅ 23 passed (18 test mới, có vector RFC 4231 cho HMAC-SHA256) |
| `make bronze-ref` | ✅ customers 2.000, cards 6.146, mcc 109; nguồn = Bronze; 38 giây cho 2 lần spark-submit |
| `make bronze MONTH=2019-03` | ✅ 118.548 dòng, khớp tuần 3; 5,4 giây không tính thời gian khởi động Spark |
| Chạy lại không có FORCE | ✅ Bỏ qua (cả giao dịch lẫn tham chiếu) |
| Chạy lại với `FORCE=1` | ✅ Checksum giống hệt; `$snapshots` có thêm một snapshot `overwrite` (xóa 118.548, thêm 118.548); tổng số dòng không đổi |
| `make bronze-range FROM=2010-01 TO=2019-10` | ✅ 117 tháng mới (2019-03 bỏ qua), 273 giây, khoảng 2 giây/tháng; chậm nhất 19,3 giây (2015-04, chỉ một lần) |
| Chạy lại toàn bộ 118 tháng | ✅ `Đã nạp 0/118 tháng`, 22 giây |
| Đối soát độc lập: đếm theo tháng ở Postgres so với đếm theo `_batch_id` qua Trino | ✅ 118/118 tháng khớp; 13.305.915 dòng; 0 id trùng; 0 dòng có `date` ngoài tháng của batch |
| Tổng amount theo năm ở Bronze | ✅ Khớp báo cáo profiling cả 10 năm (VD 2019: $49.475.055,31) |
| `bronze.cards_raw` | ✅ Không có cột `card_number` hay `cvv` |
| Tính lại HMAC từ PAN ở nguồn cho cả 6.146 thẻ | ✅ 6.146/6.146 token, BIN, last4 khớp; 0 dòng chứa PAN đầy đủ |
| Tháng sai định dạng (`MONTH=2019-3`) | ✅ Bị chặn ở bước kiểm tra tham số, không khởi động Spark |
| Tháng không có dữ liệu (`MONTH=2009-12`) | ✅ Ghi FAILED vào ingestion_log kèm thông báo lỗi, `make` thoát với mã lỗi |
| Dung lượng | transactions_raw 220,5 MB, 118 file Parquet (1 file mỗi tháng); cards_raw 0,4 MB |

## Quyết định thiết kế

1. **Token hóa trong Spark, không dùng pgcrypto ở nguồn.** Nếu tính HMAC trong Postgres thì khóa phải nằm trong câu SQL gửi tới nguồn, và có thể lộ qua log hay `pg_stat_activity`. Làm trong Spark, PAN chỉ tồn tại trong bộ nhớ của job.
2. **Ép `::text` ngay trong câu truy vấn nguồn.** Bronze nhận đúng chuỗi Postgres trả về, kể cả định dạng mà Postgres đã chuẩn hóa: `latitude` NUMERIC(9,4) thành `38.2200`, `date` thành `2019-03-01 00:01:00`. Silver sẽ ép kiểu lại.
3. **Đọc mcc_codes bằng boto3, không dùng S3A.** Image không có `hadoop-aws`, và bản tương thích Spark 4 cần AWS SDK bundle khoảng 600 MB. File chỉ 109 dòng nên driver đọc trực tiếp rồi gọi `createDataFrame`.
4. **Checksum = `sum(xxhash64(cột nghiệp vụ))`.** Kết quả không phụ thuộc thứ tự dòng, đủ để chứng minh lần nạp lại cho ra dữ liệu giống hệt. Đây không phải hàm băm mật mã.
5. **ingestion_log chỉ ghi khi job kết thúc** (một dòng mỗi lần chạy, SUCCESS hoặc FAILED), không ghi trạng thái RUNNING. Như vậy mỗi batch chỉ tạo một commit cho log, và không còn dòng RUNNING mồ côi khi job bị kill.
6. **Một Spark session cho cả khoảng tháng.** Cách này bỏ được 117 lần khởi động JVM khi backfill. dag_monthly ở tuần 6 sẽ gọi theo từng tháng.

## Lưu ý

- **Nếu R10 lệch, dữ liệu vẫn đã được commit vào Bronze**, chỉ ingestion_log mang trạng thái FAILED. Tuần 5–6: Silver chỉ đọc những batch có lần nạp gần nhất là SUCCESS, và DAG dừng khi task Bronze thất bại.
- Trino giới hạn bộ nhớ khoảng 500 MB mỗi node. `count(DISTINCT id)` trên 13 triệu chuỗi, nhóm theo tháng, bị vượt giới hạn; ép `id` sang số nguyên và đếm toàn bảng thì chạy được. Cần chú ý điều này với truy vấn nặng ở tuần 11.
- Có 244 ô trong `card_token` chứa chuỗi 13–19 chữ số liền nhau. Đây là trùng hợp ngẫu nhiên trong chuỗi hex, không phải PAN. Vì vậy R13 (tuần 8) phải so HMAC của chuỗi tìm được với token, không chỉ dùng regex (đúng như R13 trong Design Doc).
- Mỗi batch thêm một snapshot cho bảng giao dịch và một cho `ops.ingestion_log`. dag_maintenance (tuần 8) sẽ chạy `expire_snapshots` và `rewrite_manifests`.
- `_batch_id` của tham chiếu theo ngày UTC. Nạp lại tham chiếu trong cùng ngày thì phải thêm `FORCE=1`.
