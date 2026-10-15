# Mô hình dữ liệu (tuần 3)

Đặc tả cột của các tầng Bronze, Silver, Gold và schema `ops`, chi tiết hóa mục 5 và Hình 3–4 của Design Doc. Tuần 4–7 code theo tài liệu này; khi có thay đổi thì sửa ở đây trước.

- Catalog `lakehouse`, namespace `bronze`, `silver`, `gold`, `ops`. Iceberg format v2.
- Kiểu dữ liệu ghi theo Spark SQL: `int`, `bigint`, `smallint`, `decimal(p,s)`, `string`, `date`, `timestamp`, `boolean`, `array<string>`.
- `timestamp` là timestamp không múi giờ (dữ liệu gốc không có múi giờ). Spark dùng `timestamp_ntz`, Trino thấy là `timestamp(6)`.
- Mapping từng cột từ nguồn lên Gold nằm trong [source_to_target_mapping.md](source_to_target_mapping.md). Mức dữ liệu cá nhân của từng cột nằm trong [data_catalog.csv](data_catalog.csv).

## 0. Nguồn `core_card` (PostgreSQL)

Schema ở [01_core_card.sql](../infra/postgres-source/init/01_core_card.sql) và [02_core_card_constraints.sql](../infra/postgres-source/init/02_core_card_constraints.sql). `make seed` (`generators/seed_source.py`) nạp toàn bộ dữ liệu trong một lần.

| Bảng | Khóa | Ghi chú |
|---|---|---|
| customers | id (PK) | 2.000 dòng; tiền là chuỗi `'$59696'` |
| cards | id (PK), client_id → customers | 6.146 dòng; có `card_number` (PAN), `cvv` |
| transactions | id (PK), client_id → customers, card_id → cards; index trên `date` | 13.305.915 dòng, 01/2010–10/2019 |

So với Hình 1 của Design Doc, chỉ bỏ cột `cards.card_on_dark_web`, vì cột này chỉ có một giá trị "No".

## 1. Bronze – bản sao thô kèm metadata

Mọi cột nghiệp vụ đều là `string`, giữ nguyên giá trị nguồn (kể cả giá trị sai định dạng), để Silver là nơi duy nhất diễn giải dữ liệu. Ba cột kỹ thuật có ở mọi bảng:

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `_ingested_at` | timestamp | Thời điểm job Bronze ghi |
| `_source` | string | `core_card.transactions`, `core_card.cards`, `s3://landing/reference/mcc_codes/…` |
| `_batch_id` | string | `2019-03` với giao dịch; `ref_YYYYMMDD` với dữ liệu tham chiếu |

| Bảng | Cột nghiệp vụ | Partition | Cách ghi |
|---|---|---|---|
| `bronze.transactions_raw` | id, date, client_id, card_id, amount, use_chip, merchant_id, merchant_city, merchant_state, zip, mcc, errors | `identity(_batch_id)` | `overwritePartitions()` theo `_batch_id`, nên chạy lại không nhân đôi |
| `bronze.customers_raw` | 14 cột của `core_card.customers` | – | Ghi đè toàn bảng; snapshot cũ vẫn truy vết được |
| `bronze.cards_raw` | Như `core_card.cards` nhưng **`card_number` → `card_token`, `card_bin`, `card_last4`; không có `cvv`** | – | Ghi đè toàn bảng |
| `bronze.mcc_codes_raw` | mcc, description | – | Ghi đè toàn bảng |

- `card_token = hex(HMAC-SHA256(HMAC_SECRET, card_number))`, 64 ký tự. `card_bin` là 6 số đầu, `card_last4` là 4 số cuối. Việc token hóa làm ngay trong job extract, trước khi ghi, nên PAN không bao giờ chạm vào bucket warehouse.
- Mọi cột được ép `::text` ngay trong câu truy vấn nguồn, nên Bronze giữ đúng giá trị Postgres trả về: `date` dạng `2019-03-01 00:01:00`, `latitude` dạng `38.2200` (NUMERIC(9,4) ở nguồn).
- `checksum` trong ingestion_log = `sum(xxhash64(các cột nghiệp vụ))`, không phụ thuộc thứ tự dòng; hai lần nạp cùng dữ liệu cho cùng checksum.
- Extract giao dịch tháng M lọc theo `date >= đầu tháng M AND date < đầu tháng M+1` (dùng index `transactions_date_idx`). `_batch_id` = `M`. Số dòng đếm ở nguồn với cùng điều kiện này là vế trái của R10.

## 2. Silver – đã ép kiểu, làm sạch

Mọi bảng Silver có thêm `_batch_id string` (batch nạp gần nhất) và `_updated_at timestamp`.

Cách ghi (tuần 5): job tính phần chênh lệch giữa dữ liệu mới và bảng hiện có (full outer join theo khóa), rồi `MERGE INTO` chỉ các dòng thêm/sửa/xóa. Vì vậy `_updated_at` chỉ đổi khi dòng thực sự đổi, và chạy lại khi Bronze không đổi thì không tạo snapshot mới. Silver chỉ đọc batch Bronze có lần nạp gần nhất là SUCCESS trong `ops.ingestion_log`. Mọi phép parse dùng `try_cast` sau khi kiểm tra định dạng bằng regex, vì Spark 4 bật ANSI mode (ép kiểu sai làm dừng cả job). Iceberg không có kiểu `smallint`: các cột ghi `smallint` dưới đây được lưu thành `int`.

### silver.customers (khóa `customer_id`)

| Cột | Kiểu | Từ nguồn |
|---|---|---|
| customer_id | int | id |
| current_age, retirement_age | smallint | |
| birth_year, birth_month | smallint | |
| gender | string | Chuẩn hóa thành `Male` / `Female` |
| address | string | **Chỉ ở Silver**, không đưa lên Gold |
| latitude, longitude | double | |
| per_capita_income, yearly_income, total_debt | decimal(12,2) | Bỏ `$` |
| debt_to_income | decimal(6,3) | total_debt / yearly_income, NULL nếu thu nhập bằng 0 |
| credit_score, num_credit_cards | smallint | |

### silver.cards (khóa `card_id`)

| Cột | Kiểu | Từ nguồn |
|---|---|---|
| card_id | int | id |
| customer_id | int | client_id |
| card_brand | string | Visa, Mastercard, Amex, Discover |
| card_type | string | `Credit` / `Debit` / `Prepaid` (`Debit (Prepaid)` → `Prepaid`) |
| card_token, card_bin, card_last4 | string | Từ Bronze |
| expires_date | date | `MM/YYYY` → ngày cuối tháng |
| acct_open_date | date | `MM/YYYY` → ngày đầu tháng |
| has_chip | boolean | YES / NO |
| num_cards_issued, year_pin_last_changed | smallint | |
| credit_limit | decimal(12,2) | Bỏ `$`; 31 thẻ có giá trị 0 |

### silver.mcc_codes (khóa `mcc_code`)

| Cột | Kiểu | Ghi chú |
|---|---|---|
| mcc_code | int | |
| description | string | |
| mcc_group | string | 10 nhóm, bảng ánh xạ tự xây [mcc_groups.csv](../spark/transform/ref/mcc_groups.csv); mã chưa có trong bảng → `Chưa phân nhóm` |

### silver.transactions (khóa `transaction_id`, partition `months(transaction_ts)`, ghi bằng MERGE INTO theo từng tháng)

| Cột | Kiểu | Biến đổi |
|---|---|---|
| transaction_id | bigint | id |
| transaction_ts | timestamp_ntz | date. Iceberg `timestamp` không múi giờ: giờ giao dịch là giờ địa phương |
| customer_id, card_id | int | client_id, card_id |
| amount | decimal(12,2) | `'$-77.00'` → -77.00 |
| channel | string | `Swipe Transaction` → `Swipe`, tương tự `Chip`, `Online` |
| merchant_id | int | |
| merchant_city, merchant_state | string | Giữ nguyên |
| zip | string | `'2840.0'` → `'02840'`; NULL khi online hoặc ở nước ngoài |
| mcc_code | int | mcc |
| error_list | array<string> | Tách `errors` theo dấu phẩy, giữ thứ tự; mảng rỗng nếu không có lỗi (22 tổ hợp lỗi + mảng rỗng) |
| has_error | boolean | size(error_list) > 0 |
| is_refund | boolean | amount < 0 |
| is_online | boolean | channel = Online |
| is_foreign | boolean | merchant_state không NULL và không phải mã bang 2 ký tự |

Bản ghi vi phạm rule BLOCK (R01–R03) không vào bảng này mà vào `ops.quarantine_transactions`. Thứ tự xét: R01 (id rỗng hoặc không phải số) → R02 (amount, date không parse được) → R03 (ngoài tháng của batch) → R01 (trùng id: giữ bản có `transaction_ts` sớm nhất). Sau mỗi tháng job đối soát R10 vế 2: bronze = silver + quarantine.

### silver.merchants (khóa `merchant_id`, suy ra từ silver.transactions)

| Cột | Kiểu | Ghi chú |
|---|---|---|
| merchant_id | int | |
| merchant_city, merchant_state, zip | string | Vị trí xuất hiện nhiều nhất của merchant; hòa thì lấy vị trí có giao dịch gần nhất, rồi theo thứ tự chữ cái |
| is_online, is_foreign | boolean | Theo vị trí đã chọn |
| location_count | int | Số vị trí khác nhau. Lớn hơn 1 ở 21,6% merchant và được ghi vào DQ |
| txn_count | bigint | |

MCC không giữ ở đây (456 merchant có 2 MCC) mà giữ ở fact.

## 3. Gold – star schema (Hình 4)

Quy ước chung cho mọi dimension:
- Khóa thay thế (`*_key`) kiểu `int`, sinh theo thứ tự khóa tự nhiên, nên dựng lại thì ra cùng giá trị.
- Dòng **−1 Unknown** dùng cho khóa không khớp (R04, R06). Dòng **−2 Đã xóa** dùng khi thực hiện quyền xóa dữ liệu. Các cột chuỗi của hai dòng này lần lượt là `'Unknown'` và `'Deleted'`, các cột số là NULL.
- `dim_customer`, `dim_card` là SCD loại 1: ghi đè khi nguồn thay đổi.

### dim_customer (nguồn silver.customers)
customer_key int PK · customer_id int NK · gender string · birth_year smallint · yearly_income decimal(12,2) · income_band string · total_debt decimal(12,2) · debt_to_income decimal(6,3) · credit_score smallint · credit_score_band string · num_credit_cards smallint · latitude double · longitude double

Không có address, birth_month, current_age.

### dim_card (nguồn silver.cards)
card_key int PK · card_id int NK · customer_id int · card_brand string · card_type string · card_token string · card_last4 string · has_chip boolean · credit_limit decimal(12,2) · expires_date date · acct_open_date date · num_cards_issued smallint · year_pin_last_changed smallint

Không có PAN, CVV, card_bin.

### dim_merchant (nguồn silver.merchants)
merchant_key int PK · merchant_id int NK · merchant_city string · merchant_state string · zip string · is_online boolean · is_foreign boolean

### dim_mcc (nguồn silver.mcc_codes)
mcc_key int PK · mcc_code int NK · description string · mcc_group string

### dim_date (sinh theo lịch, 2010-01-01 → 2019-12-31)
date_key int PK (yyyymmdd) · full_date date · day_of_week smallint (1 = thứ Hai) · day_name string · week_of_year smallint · month smallint · quarter smallint · year smallint · is_weekend boolean

### dim_txn_profile (junk dimension)
txn_profile_key int PK · channel string · error_combo string · has_error boolean · is_refund boolean

`error_combo` là các lỗi đã sắp xếp, ghép bằng `', '`, hoặc `'None'` khi không có lỗi. Số dòng tối đa là 3 kênh × 23 tổ hợp (22 tổ hợp + None) × 2 = 138. Thực tế chỉ dựng các tổ hợp có xuất hiện, cộng dòng −1.

### fact_transaction (grain: 1 giao dịch, partition `months(transaction_ts)`)

| Cột | Kiểu | Ghi chú |
|---|---|---|
| transaction_id | bigint | PK |
| date_key | int | FK dim_date |
| customer_key | int | FK dim_customer; −1 nếu client_id không tồn tại |
| card_key | int | FK dim_card |
| merchant_key | int | FK dim_merchant |
| mcc_key | int | FK dim_mcc; −1 nếu mcc không tồn tại |
| txn_profile_key | int | FK dim_txn_profile |
| transaction_ts | timestamp | Phục vụ hidden partitioning và Q3 |
| txn_hour | smallint | |
| customer_age_at_txn | smallint | Năm giao dịch − birth_year |
| amount | decimal(12,2) | |
| batch_id | string | Truy vết batch, phục vụ đối soát R11 |

### Nhóm phân khúc (bản nháp, chốt lại ở tuần 7 theo phân bố thực tế)

| Nhóm | Ngưỡng |
|---|---|
| age_band | `< 18`, `18–24`, `25–34`, `35–44`, `45–54`, `55–64`, `65+` (đã có profiling) |
| income_band | `< 30k`, `30–50k`, `50–75k`, `75–100k`, `≥ 100k` (yearly_income, USD) |
| credit_score_band | `Poor < 580`, `Fair 580–669`, `Good 670–739`, `Very Good 740–799`, `Exceptional ≥ 800` (thang FICO) |

### Mart (tổng hợp sẵn cho dashboard)

`month` là ngày đầu tháng (`date`). Mọi mart được dựng lại theo từng tháng (`overwritePartitions`).

| Mart | Grain | Cột chính | Câu hỏi |
|---|---|---|---|
| mart_monthly_kpi | month | txn_count, total_amount, avg_amount, active_customers, active_cards, refund_count, refund_amount, error_txn_count, swipe_count, chip_count, online_count | Q1, Q2 |
| mart_mcc_monthly | month × mcc_code | mcc_group, description, txn_count, total_amount, refund_count, refund_amount | Q4, Q6 |
| mart_geo_monthly | month × merchant_state × is_online × is_foreign | txn_count, total_amount | Q5 |
| mart_error_monthly | month × error_type × channel | error_count, channel_txn_count (mẫu số để tính tỷ lệ) | Q6 |
| mart_customer_segment_monthly | month × age_band × income_band × credit_score_band | customer_count, txn_count, total_amount, avg_spend_per_customer | Q7 |
| mart_card_monthly | month × card_key | card_brand, card_type, txn_count, total_amount, credit_limit, utilization (chỉ thẻ Credit có credit_limit > 0) | Q8, Q9 |

## 4. Schema ops – vận hành, chất lượng, danh mục

| Bảng | Cột | Ghi bởi |
|---|---|---|
| ops.ingestion_log | run_id, source, batch_id, source_count, row_count, checksum, status (SUCCESS / FAILED), message, started_at, finished_at | Job Bronze (một dòng mỗi lần nạp; batch có lần nạp gần nhất SUCCESS thì bỏ qua) |
| ops.dq_results | run_id, batch_id, rule_id, severity, checked_count, failed_count, failed_ratio, status (PASS / WARN / BLOCK), created_at | Job Silver, R13 |
| ops.quarantine_transactions | Các cột của bronze.transactions_raw + rule_id, reason, run_id, quarantined_at; partition `_batch_id`, ghi đè phần của tháng M mỗi lần chạy | Job Silver (tuần 5) |
| ops.data_catalog | layer, table_name, column_name, pii_level, treatment, note | Nạp từ [data_catalog.csv](data_catalog.csv) |
