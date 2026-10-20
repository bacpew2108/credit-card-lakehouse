# Tuần 5 – Silver

Demo cuối mốc: có đủ 5 bảng Silver. `make silver-ref` dựng customers, cards, mcc_codes. `make silver-range FROM=2010-01 TO=2019-10` dựng 118 tháng giao dịch, kèm cách ly bản ghi vi phạm R01–R03, rồi tính lại merchants. Mọi số liệu khớp báo cáo profiling. Chạy lại khi Bronze không đổi thì không ghi gì.

## Sản phẩm

| Sản phẩm | File |
|---|---|
| Hàm biến đổi (biểu thức Spark gốc, không UDF) | [spark/transform/silver_transforms.py](../spark/transform/silver_transforms.py) |
| Silver bảng tham chiếu | [spark/transform/silver_reference.py](../spark/transform/silver_reference.py) |
| Silver giao dịch theo tháng: cách ly, MERGE, đối soát R10 | [spark/transform/silver_transactions.py](../spark/transform/silver_transactions.py) |
| Suy ra merchants | [spark/transform/silver_merchants.py](../spark/transform/silver_merchants.py) |
| Ánh xạ MCC → 10 nhóm ngành | [spark/transform/ref/mcc_groups.csv](../spark/transform/ref/mcc_groups.csv) |
| MERGE chỉ ghi dòng thay đổi; kiểm tra Bronze đã SUCCESS | [spark/common/lakehouse.py](../spark/common/lakehouse.py) (`merge_changes`, `require_bronze`) |
| Kiểm thử (13 test mới chạy Spark local, tổng 36) | [tests/test_silver_transforms.py](../tests/test_silver_transforms.py) |
| Lệnh `silver-ref`, `silver`, `silver-range`, `silver-merchants`, `silver-status` | [Makefile](../Makefile) |

## Cách dùng

```bash
make silver-ref                              # customers, cards, mcc_codes
make silver MONTH=2019-03                    # một tháng giao dịch, rồi merchants
make silver-range FROM=2010-01 TO=2019-10    # nhiều tháng trong một Spark session, rồi merchants
make silver-status                           # số dòng 5 bảng, số bản ghi bị cách ly theo rule
```

Silver không có `FORCE`. Mỗi lần chạy, job so dữ liệu mới với bảng hiện có rồi chỉ ghi phần khác biệt, nên chạy lại bao nhiêu lần cũng an toàn.

## Cách hoạt động

**Giao dịch, với mỗi tháng M** ([silver_transactions.py](../spark/transform/silver_transactions.py)):

1. Kiểm tra lần nạp Bronze gần nhất của M trong `ops.ingestion_log`. Nếu không phải SUCCESS thì dừng, không dựng Silver.
2. `mark_batch` gắn rule BLOCK cho từng dòng trong một lượt, theo thứ tự: R01 (id rỗng hoặc không phải số), R02 (amount, date không parse được), R03 (ngoài tháng M), rồi R01 (trùng id). Với id trùng, giữ bản có `transaction_ts` sớm nhất. Kết quả được cache để dùng lại ở các bước sau.
3. Dòng vi phạm được ghi vào `ops.quarantine_transactions` bằng `overwrite(_batch_id = M)`. Cách ghi đè theo điều kiện này xóa được bản cách ly cũ kể cả khi lần này không còn dòng lỗi nào. Khi cả cũ lẫn mới đều không có dòng lỗi thì bỏ qua bước ghi, để không tạo snapshot rỗng.
4. `merge_changes`:
   - Full outer join dữ liệu mới với Silver của tháng M, tạo ra các thao tác I/U/D.
   - Nếu có thay đổi thì `MERGE INTO` với điều kiện `ON` gồm cả khoảng thời gian của tháng M, để Iceberg chỉ quét một partition.
   - Không có thay đổi thì không ghi gì.
5. Đối soát **R10 vế 2: bronze = silver + quarantine** cho tháng M. Lệch thì job báo lỗi.
6. Sau các tháng, tính lại merchants **trong cùng Spark session**.

**Bảng tham chiếu và merchants:** cũng đi qua `merge_changes`, nhưng phạm vi là cả bảng. Merchants: mỗi merchant lấy vị trí (city, state, zip) có nhiều giao dịch nhất; hòa thì lấy vị trí gần nhất, rồi theo thứ tự chữ cái.

## Kết quả kiểm chứng (2026-10-14)

| Kiểm tra | Kết quả |
|---|---|
| `make test` | 36 passed: 13 test Silver chạy Spark local (parse tiền, ngày, zip, kênh, lỗi; tách R01–R03 với 13 dòng mẫu gồm cả trường hợp biên; merchants; 109 mã MCC đều có nhóm) |
| silver.transactions | 13.305.915 dòng, 0 id trùng, 0 dòng bị cách ly (dữ liệu gốc sạch với R01–R03) |
| Số giao dịch và tổng amount theo năm | Khớp profiling cả 10 năm (VD 2019: 1.159.966 dòng, $49.475.055,31) |
| Cờ và cột chuẩn hóa | Hoàn tiền 660.049; amount = 0: 10.639; online 1.557.912; nước ngoài 89.006; có lỗi 211.393; zip NULL 1.652.706, zip sai định dạng 0; channel đúng 3 giá trị |
| `error_list` | 22 tổ hợp lỗi + mảng rỗng |
| Vi phạm R07 có sẵn (city = ONLINE nhưng không phải Online) | 5.788, khớp profiling (rule chạy ở tuần 6) |
| Bố cục bảng | 118 partition `months(transaction_ts)`, 1 file mỗi tháng, 226,8 MB (trong bucket `warehouse`, cùng chỗ với Bronze); `transaction_ts` là `timestamp(6)` không múi giờ |
| silver.customers / cards / mcc_codes | 2.000 / 6.146 (Prepaid 578; hạn mức bằng 0: 31; 0 ngày parse lỗi) / 109 (10 nhóm) |
| silver.merchants | 74.831 merchant; 16.182 (21,6%) có nhiều vị trí, nhiều nhất 4.403; tổng `txn_count` = 13.305.915 |
| Chạy lại khi Bronze không đổi | Thêm 0, sửa 0, xóa 0; không tạo snapshot mới (customers vẫn chỉ có 1 snapshot) |
| **MERGE từ đầu đến cuối** (sửa tạm 3 giao dịch 2019-10 ở nguồn → Bronze `FORCE=1` → Silver) | Đổi amount → **sửa 1**; amount `N/A` → **cách ly R02** và xóa khỏi Silver; dòng bị xóa ở nguồn → **xóa**. Tổng: sửa 1, xóa 2, quarantine 1; R10: 117.373 = 117.372 + 1. Snapshot chỉ chạm 1 partition |
| Khôi phục nguồn rồi chạy lại | Thêm 2, sửa 1; quarantine của tháng về 0; checksum Bronze và tổng 2019 trở lại đúng giá trị gốc |

### Thời gian

Máy chạy thử thiếu RAM nghiêm trọng trong lúc đo (RAM trống dưới 100 MB, khoảng 8 GB bị nén; container Airflow giới hạn 3 GiB), nên các con số dưới đây cao hơn so với một máy rảnh.

| Bước | Thời gian |
|---|---|
| Một tháng giao dịch (trong session) | 7–8 giây khi máy rảnh; trung bình 11,3 giây khi backfill 41 tháng (475 giây); tháng đầu tiên của session 18–24 giây (JVM còn lạnh) |
| Merchants trong cùng session | 27–44 giây (riêng phép tính khoảng 3 giây khi đã nóng, phần còn lại là đọc lại 13,3 triệu dòng và MERGE) |
| Merchants chạy bằng spark-submit riêng | 48–60 giây, nên đã gộp vào job giao dịch |
| `make silver MONTH=…` (khởi động + 1 tháng + merchants) | Khoảng 63 giây |
| `make silver-ref` | 52 giây lần đầu |
| `make test` | 14 giây (lần đầu phải tải pyspark khoảng 430 MB, mất 24 phút với mạng ~0,5 MB/s) |

## Quyết định thiết kế

1. **Làm cách ly R01–R03 sớm một tuần.** MERGE báo lỗi khi một batch có hai dòng trùng khóa, và dòng không parse được mà vẫn vào Silver thì sẽ thành NULL. Vì vậy cách ly là điều kiện để Silver đúng, không tách sang tuần 6 được. Tuần 6 vẫn phụ trách R04–R12, `ops.dq_results`, nâng WARN lên BLOCK, fixture lỗi và Airflow.
2. **Tính chênh lệch trước rồi mới MERGE** thay vì `MERGE … WHEN NOT MATCHED BY SOURCE`. Mệnh đề đó khiến Iceberg không cắt được partition theo điều kiện `ON`, có thể phải quét và ghi lại cả bảng. Tự tính I/U/D trên đúng tháng M giữ được việc cắt partition. Thêm nữa, chạy lại khi không có thay đổi sẽ không ghi gì, và `_updated_at` chỉ đổi khi dòng thực sự đổi.
3. **Biểu thức Spark gốc, kiểm tra bằng regex trước rồi mới `try_cast`.** Spark 4 bật ANSI mode, nên một giá trị sai sẽ làm dừng cả job. Amount phải đúng dạng `$-77.00` hoặc `$29278`; giá trị như `$12,30.5` bị coi là không hợp lệ.
4. **`transaction_ts` dùng `TIMESTAMP_NTZ`.** Giờ giao dịch là giờ địa phương; nếu dùng kiểu có múi giờ thì Trino và Superset sẽ hiển thị kèm "UTC".
5. **Nhóm MCC tự xây bằng tay.** Mã MCC trong bộ dữ liệu không theo chuẩn ISO (ví dụ 3000 là "Steelworks"). Mã nào chưa có trong `mcc_groups.csv` được gán `Chưa phân nhóm` và job in cảnh báo.
6. **Merchants tính lại toàn bộ, nhưng trong cùng session.** Chỉ cập nhật merchant xuất hiện trong tháng M cũng không đỡ được bao nhiêu, vì vẫn phải quét toàn bộ lịch sử của các merchant đó. Chi phí thật nằm ở việc khởi động JVM và đọc lạnh.
7. **Cache bằng `persist()`, không dùng `localCheckpoint()`.** Bản đầu dùng `localCheckpoint`; khi backfill, thời gian mỗi tháng tăng dần từ 10 lên 25 giây vì `unpersist()` không giải phóng được dữ liệu checkpoint. Chuyển sang `persist()` thì thời gian ổn định.

## Lưu ý

- **2026-10-14: dữ liệu bị mất do volume Docker được tạo lại** (lúc 13:24, trước khi bắt đầu tuần 5; có thể do `make clean` hoặc reset Docker Desktop). Đã nạp lại bằng `make seed`, `make bronze-ref`, `make bronze-range`, mất tổng 583 giây. Checksum Bronze trùng tuần 4 nhờ `HMAC_SECRET` không đổi.
- **Cách ly R01 mới xét trùng id trong cùng tháng.** Vì MERGE chỉ so trong phạm vi tháng M, một id trùng với id của tháng khác sẽ được thêm vào thành hai dòng. Tuần 6 bổ sung kiểm tra trùng id toàn bảng (dữ liệu thật: 0).
- R10 vế 2 hiện chỉ in ra và báo lỗi; tuần 6 sẽ ghi kết quả vào `ops.dq_results`. Tuần 6 cũng ghi merchant nhiều vị trí vào DQ.
- **Catchup ở tuần 6:** mỗi tháng là một lần spark-submit (khoảng 60 giây gồm khởi động và merchants), nên 118 tháng mất khoảng 2 giờ chỉ riêng tầng Silver. Nên backfill lịch sử bằng `bronze-range`/`silver-range` (một session), còn `dag_monthly` dùng cho các tháng gần đây. Cần chốt lại ở tuần 6.
- Container Airflow không có `ps`, `pkill`, `kill` (thiếu gói procps). Muốn dừng job Spark thì dùng lệnh `kill` có sẵn trong bash: `docker compose exec airflow-scheduler bash -c 'kill <pid>'`.
