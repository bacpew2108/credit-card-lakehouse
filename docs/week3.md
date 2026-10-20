# Tuần 3 – Nguồn giả lập, thiết kế

Demo cuối mốc: `make seed` nạp toàn bộ bộ dữ liệu vào PostgreSQL `core_card` trong một lần, khoảng 1 phút. Pipeline phía sau (tuần 4 trở đi) vẫn trích xuất giao dịch **theo từng tháng**, bằng cách lọc theo `date`.

## Sản phẩm

| Sản phẩm | File |
|---|---|
| Schema `core_card` (3 bảng, đúng Hình 1) | [01_core_card.sql](../infra/postgres-source/init/01_core_card.sql), [02_core_card_constraints.sql](../infra/postgres-source/init/02_core_card_constraints.sql) |
| Script nạp dữ liệu nguồn | [generators/seed_source.py](../generators/seed_source.py) |
| Kiểm thử (5 test) | [tests/test_seed_source.py](../tests/test_seed_source.py) |
| Đặc tả Bronze/Silver/Gold/ops | [data_model.md](data_model.md) |
| Mapping nguồn → Gold | [source_to_target_mapping.md](source_to_target_mapping.md) |
| Danh mục dữ liệu cá nhân (bản nháp, 123 cột) | [data_catalog.csv](data_catalog.csv) |

## Vị trí bộ dữ liệu

Bộ dữ liệu Kaggle nằm trong `Code/data/raw/`. Thư mục này không được commit vì `data/` có trong `.gitignore`.

```
Code/data/raw/
├── users_data.csv          → core_card.customers
├── cards_data.csv          → core_card.cards
├── transactions_data.csv   → core_card.transactions
├── mcc_codes.json          → s3://landing/reference/mcc_codes/mcc_codes.jsonl
└── train_fraud_labels.json   (không dùng)
```

Nếu đặt dữ liệu ở chỗ khác thì truyền `DATA_DIR=…` cho `make seed` và `make profile`.

## Cách dùng

```bash
make up-core        # cần postgres-source và MinIO
make seed           # nạp toàn bộ; đã có dữ liệu thì bỏ qua
make seed FORCE=1   # xóa và nạp lại từ đầu
make seed-status    # số dòng từng bảng, khoảng thời gian giao dịch
make source-sql     # psql vào core_card
```

## Cách nạp

Mọi bước nằm trong **một transaction**. Nếu lỗi giữa chừng, dữ liệu cũ còn nguyên.

1. Bỏ PK, FK và index của `transactions`, rồi `TRUNCATE transactions, cards, customers`.
2. `COPY` customers và cards. Hai file này nhỏ, được đọc bằng module `csv`; cột `card_on_dark_web` bị bỏ.
3. Đẩy **nguyên file** `transactions_data.csv` vào `COPY … (FORMAT csv, HEADER MATCH)`. Postgres tự parse CSV: trường errors bọc `"…"` được đọc đúng, ô rỗng thành NULL. `HEADER MATCH` bắt dòng tiêu đề phải trùng danh sách cột.
4. Tạo lại PK, FK (client_id → customers, card_id → cards) và index trên `date`. Cách này nhanh hơn nhiều so với kiểm tra ràng buộc cho từng dòng trong lúc COPY.
5. `ANALYZE`, rồi đưa mcc_codes (JSON Lines) lên `landing`.

## Kết quả kiểm chứng (2026-10-12)

| Kiểm tra | Kết quả |
|---|---|
| `make test` | 5 passed |
| `make seed` | 60 giây (COPY 37 giây, tạo ràng buộc và index 23 giây) |
| Số dòng | customers 2.000, cards 6.146, transactions 13.305.915, khớp profiling |
| Số giao dịch và tổng amount theo từng năm 2010–2019 | Khớp báo cáo profiling cả 10 năm (VD 2010: 1.240.880 dòng, $54.232.556,12) |
| errors NULL / zip NULL / giao dịch có nhiều lỗi | 13.094.522 / 1.652.706 / 941, khớp profiling (CSV được parse đúng) |
| PK và FK được tạo lại thành công | Xác nhận dữ liệu gốc không trùng id, không có client_id/card_id mồ côi |
| Lấy một tháng (03/2019, 118.548 dòng) | Index Only Scan trên `transactions_date_idx`, 39 ms |
| Chạy lại `make seed` | Bỏ qua |
| `make seed FORCE=1` | Nạp lại sau 100 giây (phải xóa 13,3 triệu dòng cũ trước), số dòng không đổi |
| mcc_codes trên MinIO | `landing/reference/mcc_codes/mcc_codes.jsonl`, 109 dòng |
| Dung lượng database | 2,1 GB |

## Quyết định thiết kế

1. **Nạp toàn bộ một lần, không phát hành theo tháng, không chèn lỗi.** Đã bỏ `batch_control`, `fault_log`, `INJECT_FAULTS`. Design Doc đã sửa tương ứng (mục 1, 3.2, 3.3, 4.1, 5.2, 6, 10, 12, 13; Hình 1, 2, 5). Bản trước khi sửa: `Design_Doc.backup-truoc-tuan3.docx`.
2. **dag_monthly vẫn chạy theo tháng.** Task đầu tiên kiểm tra nguồn có giao dịch của tháng M (`date` thuộc tháng M). R10 so số dòng tháng M đếm trực tiếp ở nguồn với Bronze.
3. **Kiểm chứng cách ly/BLOCK (R01–R03) bằng pytest** trên bộ dữ liệu mẫu nhỏ có lỗi cố ý (`tests/fixtures/`, tuần 6). Trên dữ liệu thật, rule phải báo đúng các vi phạm có sẵn mà profiling đã đếm: R07 = 5.788 và R14 = 392 (309 trước ngày mở thẻ + 83 sau ngày hết hạn).
4. **Bỏ `cards.card_on_dark_web`**: cột chỉ có một giá trị "No" và Hình 1 cũng không có.

## Lưu ý

- `make source-sql Q="…"`: câu truy vấn không được chứa ký tự `$`, vì make hiểu đó là biến. Truy vấn có `$` thì mở psql tương tác (`make source-sql`).
- Seed chạy trên máy host qua cổng 5434 (postgres-source) và 9000 (MinIO); thông tin đăng nhập đọc từ `.env`.
- Nhóm thu nhập và nhóm điểm tín dụng trong data_model.md là bản nháp, chốt lại ở tuần 7.
