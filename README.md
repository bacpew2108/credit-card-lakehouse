# Credit Card Lakehouse

Hệ thống Data Lakehouse quy mô nhỏ phục vụ phân tích dữ liệu giao dịch thẻ (Đồ án 2).

Luồng dữ liệu: PostgreSQL `core_card` → Bronze → Silver → Gold (Apache Iceberg trên MinIO) → Trino → Superset, điều phối bằng Airflow. Thiết kế chi tiết nằm trong `Design_Doc.docx`.

## Dữ liệu nằm ở đâu

| Thành phần | Lưu gì |
|---|---|
| PostgreSQL `postgres-source` (`core_card`) | Hệ thống nguồn giả lập: customers, cards, transactions |
| MinIO, bucket `landing` | File thô chưa thành bảng: `reference/mcc_codes/mcc_codes.jsonl` |
| MinIO, bucket `bronze`, `silver`, `gold`, `ops` | Bảng Iceberg của từng tầng, mỗi tầng một bucket: file dữ liệu Parquet (`data/`) và file metadata Iceberg (`metadata/`) |
| Lakekeeper (database `lakekeeper` trong `postgres-meta`) | Mỗi tầng một warehouse cùng tên với bucket: danh sách namespace, bảng và đường dẫn tới file metadata hiện tại của từng bảng |

Mỗi tầng là một catalog trong Spark và Trino (`bronze`, `silver`, `gold`, `ops`), trỏ tới warehouse cùng tên trong Lakekeeper và bucket cùng tên trên MinIO. Bên trong, bảng nghiệp vụ nằm ở namespace `card`, bảng vận hành ở `ops.pipeline`, nên tên đầy đủ có dạng `<tầng>.<namespace>.<bảng>`, ví dụ `silver.card.transactions`. Trong bucket, mỗi bảng là một thư mục theo UUID; muốn biết bảng nằm ở đâu thì xem Lakekeeper UI hoặc truy vấn `SELECT "$path" FROM silver.card.transactions LIMIT 1` qua Trino. Không xóa hay sửa tay file trong các bucket này: catalog sẽ trỏ tới file không còn tồn tại.

Khi Spark hoặc Trino đọc một bảng: hỏi Lakekeeper đường dẫn file metadata hiện tại → đọc metadata trên MinIO để biết file Parquet nào thuộc snapshot mới nhất → đọc các file Parquet đó. Khi ghi: ghi Parquet và metadata mới lên MinIO, rồi Lakekeeper đổi con trỏ sang metadata mới trong một thao tác nguyên tử.

## Yêu cầu

- Docker Desktop (Compose v2). Máy nên có ≥ 16 GB RAM, ≥ 4 nhân CPU và ≥ 50 GB đĩa trống. Máy ít RAM thì dùng `make up-core`.
- [uv](https://docs.astral.sh/uv/) để chạy profiling và test trên máy host; Java 17 để `make test` chạy Spark local (lần đầu uv tải pyspark ~430 MB).
- Bộ dữ liệu [Financial Transactions Dataset](https://www.kaggle.com/datasets/computingvictor/transactions-fraud-datasets), giải nén vào `data/raw/` (không commit). Nếu đặt ở chỗ khác thì truyền `DATA_DIR=…`.

## Chạy nhanh

```bash
make env        # tạo .env từ .env.example
make secrets    # sinh khóa, dán vào .env thay cho change_me
make up         # khởi động toàn bộ dịch vụ (make up-core: không có Superset)
make ps         # kiểm tra trạng thái / healthcheck
make smoke      # kiểm chứng Spark ghi → Trino đọc/ghi → Spark đọc cùng bảng Iceberg
make seed       # nạp toàn bộ dữ liệu nguồn vào PostgreSQL core_card (~1 phút; FORCE=1 để nạp lại)
make seed-status
make bronze-ref                              # customers, cards (token hóa), mcc_codes → Bronze
make bronze-range FROM=2010-01 TO=2019-10    # giao dịch 118 tháng → Bronze (make bronze MONTH=… cho một tháng)
make bronze-status
make silver-ref                               # customers, cards, mcc_codes → Silver
make silver-range FROM=2010-01 TO=2019-10     # giao dịch → Silver (MERGE theo tháng, cách ly R01–R03) + merchants
make silver-status
```

Lần đầu `make up` phải build image Spark (tải PySpark ~430 MB) và image Airflow, mất vài phút. Những lần sau khởi động trong khoảng 35 giây.

| Dịch vụ | URL |
|---|---|
| MinIO console | http://localhost:9001 |
| Lakekeeper (Iceberg REST Catalog) | http://localhost:8181/ui |
| Trino | http://localhost:8082 (`make trino` để mở CLI) |
| Airflow | http://localhost:8080 |
| Spark master / worker | http://localhost:8090 / http://localhost:8091 |
| Spark UI (driver `spark-connect`) | http://localhost:4040 |
| Superset | http://localhost:8088 |

Gõ `make help` để xem toàn bộ lệnh. Các lệnh `demo`, `erase` sẽ được hiện thực dần theo kế hoạch.

## Phiên bản công nghệ (chốt tuần 1)

| Thành phần | Phiên bản | Trạng thái |
|---|---|---|
| Apache Iceberg | 1.10.1, table format v2 (cùng bản Trino 480 đóng gói) | Kiểm chứng tuần 2 |
| Iceberg REST Catalog | Lakekeeper v0.13.6 | Kiểm chứng tuần 2 |
| Object storage | MinIO `RELEASE.2025-09-07T16-13-09Z` | Đang dùng |
| Trino | 480 (Iceberg connector, native S3) | Đang dùng |
| Apache Spark | PySpark 4.0.4 (Scala 2.13), OpenJDK 17, `iceberg-spark-runtime-4.0_2.13:1.10.1`, `iceberg-aws-bundle:1.10.1`, PostgreSQL JDBC 42.7.8 | Kiểm chứng tuần 2 (`make smoke`) |
| Apache Airflow | 3.3.1, LocalExecutor, image `lakehouse-airflow` có `pyspark-client` 4.0.4; task chạy `python job.py` | Đang dùng |
| Cụm Spark | `spark-master`, `spark-worker` (4 nhân, 2 GB), `spark-connect` (driver thường trực, cổng 15002); image `lakehouse-spark` = Python 3.13 + Java 17 + PySpark và các jar ở trên | Đang dùng |
| Apache Superset | 6.0.0 + `trino` 0.340.0 + `psycopg2-binary` 2.9.13 | Đang dùng |
| PostgreSQL | 16 (`postgres-meta`: 3 database; `postgres-source`: core_card) | Đang dùng |
| Python host | 3.13, DuckDB 1.5.6, psycopg 3.3.5, minio 7.2.20 | Profiling, seed nguồn, test |

## Cấu trúc thư mục

```
├── docker-compose.yml
├── Makefile
├── .env.example          # mẫu biến môi trường (khóa HMAC, mật khẩu)
├── infra/                # airflow, spark, trino, lakekeeper, postgres-meta, postgres-source, superset
├── data/raw/             # bộ dữ liệu Kaggle (không commit)
├── generators/           # seed_source.py – nạp dữ liệu nguồn vào core_card
├── spark/
│   ├── common/           # helpers.py (hàm thuần), lakehouse.py (session, ingestion_log, MERGE)
│   ├── extract/          # extract_jdbc.py, load_files.py
│   ├── transform/        # silver_*.py (+ ref/mcc_groups.csv); gold_*.py, marts.py (tuần 7)
│   ├── quality/          # rules.py, reconcile.py, pan_scan.py (tuần 6, 8)
│   └── maintenance/      # iceberg_maintenance.py, erase_customer.py (tuần 8–10)
├── airflow/dags/         # dag_smoke_test; dag_reference, dag_monthly (tuần 6), dag_rebuild, dag_maintenance
├── sql/                  # truy vấn Q1–Q10, demo Iceberg (tuần 11)
├── tests/                # pytest
└── docs/
    ├── profiling/        # báo cáo profiling tuần 1
    ├── week2.md … week5.md  # tổng kết từng tuần
    ├── data_model.md     # đặc tả Bronze/Silver/Gold/ops
    ├── source_to_target_mapping.md
    └── data_catalog.csv  # danh mục dữ liệu cá nhân (bản nháp)
```

## Tiến độ

| Tuần | Nội dung | Trạng thái |
|---|---|---|
| 1 | Chốt đề tài, profiling, phiên bản, khung repo | Xong. Xem [docs/profiling/README.md](docs/profiling/README.md) |
| 2 | Dựng môi trường, Spark ghi → Trino đọc | Xong. Xem [docs/week2.md](docs/week2.md) |
| 3 | Nguồn giả lập, thiết kế | Xong. Xem [docs/week3.md](docs/week3.md) |
| 4 | Bronze | Xong. Xem [docs/week4.md](docs/week4.md) |
| 5 | Silver | Xong. Xem [docs/week5.md](docs/week5.md) |
| 6 | Data quality, Airflow (1) | Tiếp theo |

## Bảo mật

Không bao giờ commit `.env`. `HMAC_SECRET` dùng để token hóa số thẻ, nên không được đổi sau khi đã nạp dữ liệu: đổi khóa đồng nghĩa với việc mọi `card_token` đều thay đổi.
