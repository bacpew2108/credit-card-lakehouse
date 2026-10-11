# Credit Card Lakehouse

Hệ thống Data Lakehouse quy mô nhỏ phục vụ phân tích dữ liệu giao dịch thẻ (Đồ án 2).

Luồng dữ liệu: PostgreSQL `core_card` → Bronze → Silver → Gold (Apache Iceberg trên MinIO) → Trino → Superset, điều phối bằng Airflow. Thiết kế chi tiết nằm trong `Design_Doc.docx`.

## Yêu cầu

- Docker Desktop (Compose v2). Máy nên có ≥ 16 GB RAM, ≥ 4 nhân CPU và ≥ 50 GB đĩa trống. Máy ít RAM thì dùng `make up-core`.
- [uv](https://docs.astral.sh/uv/) để chạy profiling và test trên máy host.
- Bộ dữ liệu [Financial Transactions Dataset](https://www.kaggle.com/datasets/computingvictor/transactions-fraud-datasets), giải nén vào `../Financial Transactions Dataset/`. Nếu đặt ở chỗ khác thì truyền `DATA_DIR=…`.

## Chạy nhanh

```bash
make env        # tạo .env từ .env.example
make secrets    # sinh khóa, dán vào .env thay cho change_me
make up         # khởi động toàn bộ dịch vụ (make up-core: không có Superset)
make ps         # kiểm tra trạng thái / healthcheck
make smoke      # kiểm chứng Spark ghi → Trino đọc/ghi → Spark đọc cùng bảng Iceberg
```

Lần đầu `make up` phải build image Airflow (tải PySpark ~430 MB), mất vài phút. Những lần sau khởi động trong khoảng 35 giây.

| Dịch vụ | URL |
|---|---|
| MinIO console | http://localhost:9001 |
| Lakekeeper (Iceberg REST Catalog) | http://localhost:8181/ui |
| Trino | http://localhost:8082 (`make trino` để mở CLI) |
| Airflow | http://localhost:8080 |
| Superset | http://localhost:8088 |

Gõ `make help` để xem toàn bộ lệnh. Các lệnh `seed`, `demo`, `erase` sẽ được hiện thực dần theo kế hoạch.

## Phiên bản công nghệ (chốt tuần 1)

| Thành phần | Phiên bản | Trạng thái |
|---|---|---|
| Apache Iceberg | 1.10.1, table format v2 (cùng bản Trino 480 đóng gói) | ✅ Kiểm chứng tuần 2 |
| Iceberg REST Catalog | Lakekeeper v0.13.6 | ✅ Kiểm chứng tuần 2 |
| Object storage | MinIO `RELEASE.2025-09-07T16-13-09Z` | Đang dùng |
| Trino | 480 (Iceberg connector, native S3) | ✅ |
| Apache Spark | PySpark 4.0.4 (Scala 2.13), OpenJDK 17, `iceberg-spark-runtime-4.0_2.13:1.10.1`, `iceberg-aws-bundle:1.10.1`, PostgreSQL JDBC 42.7.8 | ✅ Kiểm chứng tuần 2 (`make smoke`) |
| Apache Airflow | 3.3.1, LocalExecutor, image `lakehouse-airflow` có Java + PySpark | ✅ |
| Apache Superset | 6.0.0 + `trino` 0.340.0 + `psycopg2-binary` 2.9.13 | ✅ |
| PostgreSQL | 16 (`postgres-meta`: 3 database; `postgres-source`: core_card) | ✅ |
| Python host | 3.13, DuckDB 1.5.6 | Profiling, test |

## Cấu trúc thư mục

```
├── docker-compose.yml
├── Makefile
├── .env.example          # mẫu biến môi trường (khóa HMAC, mật khẩu)
├── infra/                # airflow, spark, trino, lakekeeper, postgres-meta, postgres-source, superset
├── generators/           # seed_source.py – phát hành dữ liệu theo tháng
├── spark/
│   ├── extract/          # extract_jdbc.py, load_files.py
│   ├── transform/        # silver_*.py, gold_*.py, marts.py
│   ├── quality/          # rules.py, reconcile.py, pan_scan.py
│   └── maintenance/      # iceberg_maintenance.py, erase_customer.py
├── airflow/dags/         # dag_reference, dag_monthly, dag_rebuild, dag_maintenance
├── sql/                  # truy vấn Q1–Q10, demo Iceberg
├── tests/                # pytest
└── docs/
    └── profiling/        # báo cáo profiling tuần 1
```

## Tiến độ

| Tuần | Nội dung | Trạng thái |
|---|---|---|
| 1 | Chốt đề tài, profiling, phiên bản, khung repo | ✅ Xong. Xem [docs/profiling/README.md](docs/profiling/README.md) |
| 2 | Dựng môi trường, Spark ghi → Trino đọc | ✅ Xong. Xem [docs/week2.md](docs/week2.md) |
| 3 | Nguồn giả lập, thiết kế | ⏳ |

## Bảo mật

Không bao giờ commit `.env`. `HMAC_SECRET` dùng để token hóa số thẻ, nên không được đổi sau khi đã nạp dữ liệu: đổi khóa đồng nghĩa với việc mọi `card_token` đều thay đổi.
