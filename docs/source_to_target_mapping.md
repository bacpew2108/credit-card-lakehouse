# Mapping nguồn → Gold (tuần 3, cập nhật tuần 5)

Mỗi cột nguồn đi qua Bronze, Silver, Gold như thế nào, biến đổi ra sao, và rule DQ nào kiểm tra nó. Kiểu dữ liệu chi tiết nằm trong [data_model.md](data_model.md). Ký hiệu **✗** nghĩa là cột dừng lại ở tầng đó, không đi tiếp.

## core_card.customers → bronze.card.customers_raw → silver.card.customers → gold.card.dim_customer

| Nguồn | Bronze | Silver | Gold | Biến đổi | Rule |
|---|---|---|---|---|---|
| id | id | customer_id | customer_id (NK), customer_key | Ép int; dim sinh key thay thế | R04 (tham chiếu) |
| current_age | current_age | current_age | ✗ | Tuổi lúc chụp dữ liệu (2019), không dùng để phân tích | |
| retirement_age | retirement_age | retirement_age | ✗ | | |
| birth_year | birth_year | birth_year | birth_year | Dùng tính customer_age_at_txn ở fact | |
| birth_month | birth_month | birth_month | ✗ | Giảm độ chi tiết dữ liệu cá nhân | |
| gender | gender | gender | gender | Trim, chuẩn hóa Male/Female | |
| address | address | address | ✗ | Định danh trực tiếp, chỉ giữ ở Silver | |
| latitude, longitude | latitude, longitude | double | double | | |
| per_capita_income | per_capita_income | decimal | ✗ | Bỏ `$` | |
| yearly_income | yearly_income | decimal | yearly_income, income_band | Bỏ `$`; tính nhóm thu nhập | |
| total_debt | total_debt | decimal | total_debt | Bỏ `$` | |
| – | – | debt_to_income | debt_to_income | total_debt / yearly_income | |
| credit_score | credit_score | credit_score | credit_score, credit_score_band | | R09 |
| num_credit_cards | num_credit_cards | num_credit_cards | num_credit_cards | | R09 (so với số thẻ thực tế) |
| updated_at | ✗ | – | – | Cột kỹ thuật của nguồn, thay bằng `_ingested_at` | |

## core_card.cards → bronze.card.cards_raw → silver.card.cards → gold.card.dim_card

| Nguồn | Bronze | Silver | Gold | Biến đổi | Rule |
|---|---|---|---|---|---|
| id | id | card_id | card_id (NK), card_key | | R04 |
| client_id | client_id | customer_id | customer_id | | R05 |
| card_brand | card_brand | card_brand | card_brand | | |
| card_type | card_type | card_type | card_type | `Debit (Prepaid)` → `Prepaid` | |
| **card_number** | **card_token, card_bin, card_last4** | giữ 3 cột | card_token, card_last4 | HMAC-SHA256 **ngay khi trích xuất**; PAN không vào Lakehouse | R13 |
| **cvv** | **✗** | – | – | Bị loại khi trích xuất, không có ở bất kỳ tầng nào | R13 |
| expires | expires | expires_date | expires_date | `MM/YYYY` → ngày cuối tháng | R14 |
| has_chip | has_chip | boolean | boolean | YES/NO → true/false | R08 |
| num_cards_issued | num_cards_issued | smallint | smallint | | |
| credit_limit | credit_limit | decimal | decimal | Bỏ `$` | Q9 loại các thẻ có giá trị 0 |
| acct_open_date | acct_open_date | acct_open_date | acct_open_date | `MM/YYYY` → ngày đầu tháng | R14 |
| year_pin_last_changed | year_pin_last_changed | smallint | smallint | | |

## core_card.transactions → bronze.card.transactions_raw → silver.card.transactions → gold.card.fact_transaction

| Nguồn | Bronze | Silver | Gold (fact) | Biến đổi | Rule |
|---|---|---|---|---|---|
| id | id | transaction_id | transaction_id | Ép bigint | **R01** (BLOCK) |
| date | date | transaction_ts | transaction_ts, date_key, txn_hour | Parse timestamp; date_key = yyyymmdd | **R02, R03** (BLOCK) |
| client_id | client_id | customer_id | customer_key, customer_age_at_txn | Tra dim_customer, không khớp thì gán −1 | R04, R05 |
| card_id | card_id | card_id | card_key | Tra dim_card, không khớp thì gán −1 | R04, R08, R14 |
| amount | amount | amount, is_refund | amount, txn_profile_key (is_refund) | `'$-77.00'` → decimal; < 0 là hoàn tiền | **R02** (BLOCK), R11, R12 |
| use_chip | use_chip | channel, is_online | txn_profile_key (channel) | `Swipe Transaction` → `Swipe`, … | R07, R08 |
| merchant_id | merchant_id | merchant_id | merchant_key | → silver.card.merchants → dim_merchant | |
| merchant_city | merchant_city | merchant_city | (qua dim_merchant) | `ONLINE` với giao dịch trực tuyến | R07 |
| merchant_state | merchant_state | merchant_state, is_foreign | (qua dim_merchant) | Không phải mã bang thì là nước ngoài | |
| zip | zip | zip | (qua dim_merchant) | `'2840.0'` → `'02840'` | |
| mcc | mcc | mcc_code | mcc_key | Tra dim_mcc, không khớp thì gán −1 | R06 |
| errors | errors | error_list, has_error | txn_profile_key (error_combo, has_error) | Tách theo dấu phẩy → mảng | |
| (tháng M của lần chạy) | `_batch_id` | `_batch_id` | batch_id | Tháng được trích xuất, VD `2019-03` | R03, R10, R11 |
| created_at | ✗ | – | – | Cột kỹ thuật của nguồn | |
| (count(*) ở nguồn của tháng M) | – | – | – | So với số dòng Bronze | **R10** (BLOCK) |

## landing/reference/mcc_codes → bronze.card.mcc_codes_raw → silver.card.mcc_codes → gold.card.dim_mcc

| Nguồn (JSON Lines) | Bronze | Silver | Gold | Biến đổi |
|---|---|---|---|---|
| mcc | mcc | mcc_code | mcc_code (NK), mcc_key | Ép int |
| description | description | description | description | |
| – | – | mcc_group | mcc_group | Bảng ánh xạ tự xây [mcc_groups.csv](../spark/transform/ref/mcc_groups.csv): 109 mã, 10 nhóm; mã thiếu → `Chưa phân nhóm` |

`mcc_codes.json` gốc là một JSON object duy nhất. `seed_source.py` chuyển nó thành JSON Lines (`{"mcc": "5812", "description": "…"}`) và đặt ở `s3://landing/reference/mcc_codes/mcc_codes.jsonl`.

## Bảng không có nguồn trực tiếp

| Bảng | Suy ra từ | Cách dựng |
|---|---|---|
| silver.card.merchants → dim_merchant | silver.card.transactions | Theo từng merchant_id, lấy vị trí (city, state, zip) xuất hiện nhiều nhất; hòa thì lấy vị trí gần nhất, rồi theo thứ tự chữ cái |
| dim_txn_profile | silver.card.transactions | DISTINCT (channel, error_combo, has_error, is_refund) |
| dim_date | Lịch | Sinh từ 2010-01-01 đến 2019-12-31 |
| mart_* | fact + dimension | Xem data_model.md mục 3 |

## Rule DQ theo tầng

| Rule | Chạy ở | Dữ liệu kiểm tra |
|---|---|---|
| R10 | Bronze (sau extract) và Silver | Số dòng tháng M ở nguồn = bronze; bronze = silver + quarantine |
| R01–R03 | Silver | Bản ghi vi phạm đưa vào ops.pipeline.quarantine_transactions |
| R04–R09, R12, R14 | Silver | Ghi vào ops.pipeline.dq_results; WARN thành BLOCK khi tỷ lệ vi phạm > 1% |
| R11 | Gold | silver = fact theo số dòng và tổng amount của tháng |
| R13 | dag_maintenance | Quét toàn bộ bucket bronze, silver, gold, ops tìm số thẻ đầy đủ |

## Kiểm chứng rule

Dữ liệu gốc gần như sạch, nên cơ chế cách ly và BLOCK được kiểm chứng bằng pytest trên bộ dữ liệu mẫu nhỏ có lỗi cố ý (`tests/fixtures/`, tuần 6). Trên dữ liệu thật, rule phải báo đúng các vi phạm mà profiling đã đếm.

| Rule | Lỗi cố ý trong fixture | Kết quả mong đợi (fixture) | Trên dữ liệu thật |
|---|---|---|---|
| R01 | Trùng transaction_id | Bản trùng vào quarantine | 0 |
| R02 | amount `N/A`, `$12,30.5`; date không parse được | Vào quarantine | 0 |
| R03 | transaction_ts ngoài tháng của batch | Vào quarantine | 0 |
| R04 | client_id / card_id không tồn tại | Giữ lại, key = −1 | 0 |
| R05 | Thẻ không thuộc khách hàng | Ghi nhận | 0 |
| R06 | mcc không có trong mcc_codes | Giữ lại, mcc_key = −1 | 0 |
| R07 | Kênh lạ; Chip với city = ONLINE | Ghi nhận | **5.788** |
| R08 | Thẻ has_chip = NO có giao dịch Chip | Ghi nhận | 0 |
| R09 | credit_score ngoài [300, 850] | Ghi nhận | 0 |
| R14 | Giao dịch trước ngày mở thẻ / sau ngày hết hạn | Ghi nhận | **392** (309 + 83) |
| Nâng WARN → BLOCK | Tỷ lệ vi phạm WARN > 1% | DAG dừng | – |
