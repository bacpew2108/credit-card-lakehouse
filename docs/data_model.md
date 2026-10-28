# Mô hình dữ liệu

Đặc tả cột của các tầng Bronze, Silver, Gold và tầng vận hành `ops`, chi tiết hóa mục 5 và Hình 3–4 của Design Doc. Tuần 4–7 code theo tài liệu này; khi có thay đổi thì sửa ở đây trước.

- Mỗi tầng một catalog `bronze`, `silver`, `gold`, `ops`, tương ứng một warehouse Lakekeeper và một bucket MinIO cùng tên. Bảng nghiệp vụ nằm trong namespace `card`, bảng vận hành trong `ops.pipeline`. Iceberg format v2.
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
| `bronze.card.transactions_raw` | id, date, client_id, card_id, amount, use_chip, merchant_id, merchant_city, merchant_state, zip, mcc, errors | `identity(_batch_id)` | `overwritePartitions()` theo `_batch_id`, nên chạy lại không nhân đôi |
| `bronze.card.customers_raw` | 14 cột của `core_card.customers` | – | Ghi đè toàn bảng; snapshot cũ vẫn truy vết được |
| `bronze.card.cards_raw` | Như `core_card.cards` nhưng **`card_number` → `card_token`, `card_bin`, `card_last4`; không có `cvv`** | – | Ghi đè toàn bảng |
| `bronze.card.mcc_codes_raw` | mcc, description | – | Ghi đè toàn bảng |

- `card_token = hex(HMAC-SHA256(HMAC_SECRET, card_number))`, 64 ký tự. `card_bin` là 6 số đầu, `card_last4` là 4 số cuối. Việc token hóa làm ngay trong job extract, trước khi ghi, nên PAN không bao giờ chạm vào bucket warehouse.
- Mọi cột được ép `::text` ngay trong câu truy vấn nguồn, nên Bronze giữ đúng giá trị Postgres trả về: `date` dạng `2019-03-01 00:01:00`, `latitude` dạng `38.2200` (NUMERIC(9,4) ở nguồn).
- `checksum` trong ingestion_log = `sum(xxhash64(các cột nghiệp vụ))`, không phụ thuộc thứ tự dòng; hai lần nạp cùng dữ liệu cho cùng checksum.
- Extract giao dịch tháng M lọc theo `date >= đầu tháng M AND date < đầu tháng M+1` (dùng index `transactions_date_idx`). `_batch_id` = `M`. Số dòng đếm ở nguồn với cùng điều kiện này là vế trái của R10.

## 2. Silver – đã ép kiểu, làm sạch

Mọi bảng Silver có thêm `_batch_id string` (batch nạp gần nhất) và `_updated_at timestamp`.

Cách ghi: job tính phần chênh lệch giữa dữ liệu mới và bảng hiện có (full outer join theo khóa), rồi `MERGE INTO` chỉ các dòng thêm/sửa/xóa. Vì vậy `_updated_at` chỉ đổi khi dòng thực sự đổi, và chạy lại khi Bronze không đổi thì không tạo snapshot mới. Silver chỉ đọc batch Bronze có lần nạp gần nhất là SUCCESS trong `ops.pipeline.ingestion_log`. Mọi phép parse dùng `try_cast` sau khi kiểm tra định dạng bằng regex, vì Spark 4 bật ANSI mode (ép kiểu sai làm dừng cả job). Iceberg không có kiểu `smallint`: các cột ghi `smallint` dưới đây được lưu thành `int`.

### silver.card.customers (khóa `customer_id`)

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

### silver.card.cards (khóa `card_id`)

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

### silver.card.mcc_codes (khóa `mcc_code`)

| Cột | Kiểu | Ghi chú |
|---|---|---|
| mcc_code | int | |
| description | string | |
| mcc_group | string | 10 nhóm, bảng ánh xạ tự xây [mcc_groups.csv](../spark/transform/ref/mcc_groups.csv); mã chưa có trong bảng → `Chưa phân nhóm` |

### silver.card.transactions (khóa `transaction_id`, partition `months(transaction_ts)`, ghi bằng MERGE INTO theo từng tháng)

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

Bản ghi vi phạm rule BLOCK (R01–R03) không vào bảng này mà vào `ops.pipeline.quarantine_transactions`. Thứ tự xét: R01 (id rỗng hoặc không phải số) → R02 (amount, date không parse được) → R03 (ngoài tháng của batch) → R01 (id đã có trong Silver ở tháng khác: bản nạp trước được giữ) → R01 (trùng id trong tháng: giữ bản có `transaction_ts` sớm nhất). Sau mỗi tháng job ghi số bản ghi bị cách ly theo R01–R03 và đối soát R10.2 (bronze = silver + quarantine) vào `ops.pipeline.dq_results`.

### silver.card.merchants (khóa `merchant_id`, suy ra từ silver.card.transactions)

| Cột | Kiểu | Ghi chú |
|---|---|---|
| merchant_id | int | |
| merchant_city, merchant_state, zip | string | Vị trí xuất hiện nhiều nhất của merchant; hòa thì lấy vị trí có giao dịch gần nhất, rồi theo thứ tự chữ cái |
| is_online, is_foreign | boolean | Theo vị trí đã chọn |
| location_count | int | Số vị trí khác nhau. Lớn hơn 1 ở 21,6% merchant, ghi vào `ops.pipeline.dq_results` với rule I01 (chỉ ghi nhận) |
| txn_count | bigint | |

MCC không giữ ở đây (456 merchant có 2 MCC) mà giữ ở fact.

## 3. Gold – star schema (Hình 4)

Quy ước chung cho mọi dimension:
- Khóa thay thế (`*_key`) kiểu `int`. Khóa tự nhiên (NK) đã có giữ nguyên khóa cũ; NK mới nhận khóa tiếp theo sau khóa lớn nhất, theo thứ tự NK trong lượt nạp đó. Vì vậy thêm dữ liệu không làm đổi khóa của dòng cũ. Khóa phụ thuộc thứ tự nạp: dựng cả khoảng tháng trong một lần thì khóa theo thứ tự NK, còn nạp từng tháng thì NK xuất hiện ở tháng sớm hơn nhận khóa nhỏ hơn. Do đó chỉ dựng lại một dimension từ đầu khi dựng lại cả fact. `dim_date` dùng chính `date_key` (yyyymmdd).
- Dòng **−1 Unknown** dùng cho khóa không khớp (R04, R06). Dòng **−2 Đã xóa** dùng khi thực hiện quyền xóa dữ liệu. Các cột chuỗi của hai dòng này lần lượt là `'Unknown'` và `'Deleted'`, các cột số và boolean là NULL.
- `dim_customer`, `dim_card`, `dim_mcc`, `dim_merchant` là SCD loại 1: dim trùng với Silver, ghi đè khi nguồn thay đổi. `dim_location`, `dim_txn_profile` chỉ thêm các vị trí, tổ hợp mới xuất hiện trong các tháng được dựng.
- Dimension ghi bằng MERGE INTO theo khóa, chỉ các dòng thay đổi (như Silver), nên có thêm `_batch_id` và `_updated_at`. Chạy lại khi Silver không đổi thì không ghi gì.
- Fact và mart tra dimension bằng join broadcast; mọi dimension đều nhỏ (lớn nhất là dim_merchant, 74.833 dòng).

### dim_customer (nguồn silver.card.customers)
customer_key int PK · customer_id int NK · gender string · birth_year smallint · yearly_income decimal(12,2) · income_band string · total_debt decimal(12,2) · debt_to_income decimal(6,3) · credit_score smallint · credit_score_band string · num_credit_cards smallint · latitude double · longitude double

Không có address, birth_month, current_age.

### dim_card (nguồn silver.card.cards)
card_key int PK · card_id int NK · customer_id int · card_brand string · card_type string · card_token string · card_last4 string · has_chip boolean · credit_limit decimal(12,2) · expires_date date · acct_open_date date · num_cards_issued smallint · year_pin_last_changed smallint

Không có PAN, CVV, card_bin.

### dim_merchant (nguồn silver.card.merchants)
merchant_key int PK · merchant_id int NK · merchant_city string · merchant_state string · zip string · is_online boolean · is_foreign boolean

`merchant_id` là một chuỗi cửa hàng, không phải một điểm bán: 21,6% merchant có nhiều vị trí, có merchant ở 4.403 vị trí. Các cột vị trí ở đây là **vị trí chính** (xuất hiện nhiều nhất), chỉ để tham khảo. 61% giao dịch không diễn ra ở vị trí chính và 58% khác cả bang, nên vị trí của từng giao dịch nằm ở `dim_location`.

### dim_location (vị trí giao dịch, suy ra từ silver.card.transactions)
location_key int PK · merchant_city string · merchant_state string · zip string (NK là bộ ba city, state, zip; so khớp null-safe) · is_online boolean (city = `ONLINE`) · is_foreign boolean (state không phải mã bang 2 chữ cái)

25.425 vị trí. Giao dịch online có state, zip NULL; giao dịch nước ngoài có zip NULL.

### dim_mcc (nguồn silver.card.mcc_codes)
mcc_key int PK · mcc_code int NK · description string · mcc_group string

### dim_date (sinh theo lịch, 2010-01-01 → 2019-12-31)
date_key int PK (yyyymmdd) · full_date date · day_of_week smallint (1 = thứ Hai) · day_name string (`Thứ Hai` … `Chủ Nhật`) · week_of_year smallint · month smallint · quarter smallint · year smallint · is_weekend boolean

### dim_txn_profile (junk dimension)
txn_profile_key int PK · channel string · error_combo string · has_error boolean · is_refund boolean

NK là bộ ba (channel, error_combo, is_refund). `error_combo` là các lỗi đã sắp xếp, ghép bằng `', '`, hoặc `'None'` khi không có lỗi. Số dòng tối đa là 3 kênh × 23 tổ hợp (22 tổ hợp + None) × 2 = 138. Thực tế chỉ dựng các tổ hợp có xuất hiện, cộng dòng −1, −2.

### fact_transaction (grain: 1 giao dịch, partition `months(transaction_ts)`, ghi đè partition của tháng M bằng `overwritePartitions`)

| Cột | Kiểu | Ghi chú |
|---|---|---|
| transaction_id | bigint | PK |
| date_key | int | FK dim_date |
| customer_key | int | FK dim_customer; −1 nếu client_id không tồn tại |
| card_key | int | FK dim_card |
| merchant_key | int | FK dim_merchant (chuỗi cửa hàng) |
| location_key | int | FK dim_location (nơi giao dịch diễn ra) |
| mcc_key | int | FK dim_mcc; −1 nếu mcc không tồn tại |
| txn_profile_key | int | FK dim_txn_profile |
| transaction_ts | timestamp | Phục vụ hidden partitioning và Q3 |
| txn_hour | smallint | |
| customer_age_at_txn | smallint | Năm giao dịch − birth_year |
| amount | decimal(12,2) | |
| batch_id | string | Truy vết batch, phục vụ đối soát R11 |

Khóa không khớp nhận −1. Trên dữ liệu thật không có khóa −1 nào (R04, R06 bằng 0). Gold chỉ dựng tháng M khi không có rule nào từ nguồn đến Silver đang BLOCK trong `ops.pipeline.dq_results`.

### Nhóm phân khúc

| Nhóm | Ngưỡng | Phân bố |
|---|---|---|
| age_band | `< 18`, `18–24`, `25–34`, `35–44`, `45–54`, `55–64`, `65+` (tuổi tại thời điểm giao dịch) | Có giao dịch ở tuổi 17 (profiling) |
| income_band | `< 30k`, `30–40k`, `40–50k`, `50–75k`, `≥ 75k` (yearly_income, USD) | 340 / 617 / 448 / 454 / 141 khách; trung vị 40.745 USD |
| credit_score_band | `Poor (< 580)`, `Fair (580–669)`, `Good (670–739)`, `Very Good (740–799)`, `Exceptional (≥ 800)` (thang FICO) | 81 / 348 / 931 / 474 / 166 khách |

Giá trị thiếu (dòng −1) có nhóm `Unknown`.

### Mart (tổng hợp sẵn cho dashboard)

`month` là ngày đầu tháng (`date`). Mart tổng hợp từ fact join dimension (star join), partition theo `month`. Các tháng được dựng ghi đè bằng điều kiện trên `month`, khớp trọn partition, nên tháng không còn dòng nào của một mart cũng được xóa.

| Mart | Grain | Cột chính | Câu hỏi |
|---|---|---|---|
| mart_monthly_kpi | month | txn_count, total_amount, avg_amount, active_customers, active_cards, refund_count, refund_amount, error_txn_count, swipe_count, chip_count, online_count | Q1, Q2 |
| mart_mcc_monthly | month × mcc_code | mcc_group, description, txn_count, total_amount, refund_count, refund_amount | Q4, Q6 |
| mart_geo_monthly | month × merchant_state × is_online × is_foreign, theo **dim_location** | txn_count, total_amount | Q5 |
| mart_error_monthly | month × error_type × channel | error_count, channel_txn_count (mẫu số để tính tỷ lệ) | Q6 |
| mart_customer_segment_monthly | month × age_band × income_band × credit_score_band | customer_count, txn_count, total_amount, avg_spend_per_customer | Q7 |
| mart_card_monthly | month × card_key | card_brand, card_type, credit_limit, txn_count, total_amount, purchase_amount (tổng giao dịch có amount > 0), utilization = purchase_amount / credit_limit (chỉ thẻ Credit có credit_limit > 0, còn lại NULL) | Q8, Q9 |

`active_customers`, `active_cards`, `customer_count` chỉ đếm khóa thật (bỏ −1, −2). R11 đối soát mỗi tháng: số dòng và tổng amount của Silver = fact = `mart_monthly_kpi`.

## 4. Ops (`ops.pipeline`) – vận hành, chất lượng, danh mục

| Bảng | Cột | Ghi bởi |
|---|---|---|
| ops.pipeline.ingestion_log | run_id, source, batch_id, source_count, row_count, checksum, status (SUCCESS / FAILED), message, started_at, finished_at | Job Bronze (một dòng mỗi lần nạp; batch có lần nạp gần nhất SUCCESS thì bỏ qua) |
| ops.pipeline.dq_results | run_id, batch_id, rule_id, severity (BLOCK / WARN / INFO), checked_count, failed_count, failed_ratio (double), status (PASS / WARN / BLOCK / INFO), details, created_at | `reconcile.py` (R10, R11), job Silver (R01–R03, I01), `rules.py` (R04–R09, R12, R14), R13. Chỉ ghi thêm: một dòng cho mỗi (lần chạy, batch, rule) |
| ops.pipeline.quarantine_transactions | Các cột của bronze.card.transactions_raw + rule_id, reason, run_id, quarantined_at; partition `_batch_id`, ghi đè phần của tháng M mỗi lần chạy | Job Silver |
| ops.pipeline.data_catalog | layer, table_name, column_name, pii_level, treatment, note | Nạp từ [data_catalog.csv](data_catalog.csv) |

### Quy ước của ops.pipeline.dq_results

- `batch_id` là tháng (`2019-03`) với rule giao dịch, `ref_YYYYMMDD` với R09. `rule_id` theo mục 6 Design Doc; R10 tách thành **R10.1** (số dòng nguồn = bronze) và **R10.2** (bronze = silver + quarantine). **I01** là chỉ số thông tin: merchant có nhiều vị trí.
- `severity` là mức của rule: BLOCK (R01–R03, R10, R11, R13), WARN (R04–R09, R12, R14), INFO (I01).
- `status` là kết quả của batch:
  - PASS: không có vi phạm.
  - WARN: có vi phạm nhưng batch đi tiếp. Với R01–R03, các bản ghi vi phạm đã nằm trong quarantine.
  - BLOCK: dừng DAG. Đối soát R10, R11 lệch dù chỉ một dòng; hoặc rule xét từng bản ghi (R01–R09, R14) có `failed_ratio` vượt ngưỡng (mặc định 1%, tham số `--block-ratio` hoặc biến `DQ_BLOCK_RATIO`).
  - INFO: chỉ ghi nhận, không chặn.
- R12 xét theo tháng: `checked_count` = 1, `failed_count` = 1 khi số giao dịch hoặc tổng tiền lệch quá ±3σ so với 12 tháng liền trước; chưa đủ 12 tháng thì `checked_count` = 0. R12 chỉ cảnh báo, không nâng thành BLOCK.
- R11 so 4 cặp mỗi tháng (số dòng, tổng amount của Silver với fact; của fact với `mart_monthly_kpi`): `checked_count` = 4, `failed_count` = số cặp lệch.
- `details` ghi số vi phạm theo từng điều kiện khi rule có nhiều điều kiện (VD R14: `trước tháng mở thẻ: 1; sau tháng hết hạn: 0`), lý do cách ly với R01–R03, số dòng hai vế với R10, số dòng và tổng tiền ba bảng với R11, z-score với R12.
- Kết quả có hiệu lực của một rule trong một batch là dòng mới nhất (`max_by(status, created_at)`). Task `dq_silver` của `dag_monthly` và job Gold dừng khi kết quả có hiệu lực của một rule từ nguồn đến Silver trong tháng là BLOCK (`SILVER_RULES`: mọi rule trừ R11 và I01). Vì không xét R11, một lần R11 BLOCK cũ không chặn việc chạy lại `dag_monthly` để dựng lại Gold.
