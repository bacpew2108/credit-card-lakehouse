# Tuần 2 – Dựng môi trường

Demo cuối mốc: `make up` khởi động đủ dịch vụ, có healthcheck; `make smoke` chứng minh Spark và Trino cùng đọc/ghi một bảng Iceberg qua một catalog chung.

## Thành phần đã dựng

| Dịch vụ | Image / phiên bản | Vai trò | Cổng host |
|---|---|---|---|
| minio, minio-init | MinIO RELEASE.2025-09-07 | Bucket `landing`, `warehouse`; tài khoản dịch vụ `lakehouse-svc` (không dùng root) | 9000, 9001 |
| postgres-meta | postgres:16-alpine | 1 instance, 3 database: `lakekeeper`, `airflow`, `superset`, mỗi database một user riêng | – |
| postgres-source | postgres:16-alpine | Hệ thống nguồn `core_card` (schema làm ở tuần 3) | 5434 (`SOURCE_DB_HOST_PORT`) |
| lakekeeper (+ migrate, bootstrap) | quay.io/lakekeeper/catalog:v0.13.6 | Iceberg REST Catalog; warehouse `lakehouse` → `s3://warehouse/lakehouse/` | 8181 (UI: `/ui`) |
| trino | trinodb/trino:480 (Iceberg 1.10.1) | Catalog `lakehouse` | 8082 |
| airflow-apiserver, scheduler, dag-processor | `lakehouse-airflow` = Airflow 3.3.1 + Java 17 + PySpark 4.0.4 + iceberg-spark-runtime-4.0_2.13:1.10.1 + iceberg-aws-bundle:1.10.1 + PostgreSQL JDBC 42.7.8 | Điều phối; task gọi `spark-submit` local[*] | 8080 |
| superset, superset-init (profile `bi`) | `lakehouse-superset` = Superset 6.0.0 + psycopg2 + trino | BI | 8088 |

- **Profile:** `make up-core` chạy các dịch vụ không gắn profile, không có Superset. `make up` thêm profile `bi`.
- **Giới hạn RAM** (`mem_limit`): scheduler 3 GB (chạy Spark), Trino 2 GB, các dịch vụ khác 256 MB – 1 GB.
- **Namespace Iceberg** `bronze`, `silver`, `gold`, `ops` được tạo bởi `spark/smoke_test.py`. Từ tuần 4, mọi job cũng tự tạo `bronze`, `silver`, `ops` khi khởi động (`get_spark`).
- **Tầng là namespace, không phải bucket.** Mọi bảng của cả bốn namespace nằm chung bucket `warehouse`, mỗi bảng một thư mục UUID dưới `warehouse/lakehouse/`. Lakekeeper giữ ánh xạ tên bảng → file metadata hiện tại; metadata Iceberg (JSON, Avro) và dữ liệu Parquet đều nằm trên MinIO. Bucket `landing` chỉ chứa file thô chưa thành bảng.

## Kết quả kiểm chứng (2026-10-11, Docker Desktop 8 GB RAM, 8 CPU, Apple Silicon)

| Kiểm tra | Kết quả |
|---|---|
| `make up` từ trạng thái đã có image | Đủ 14 container healthy hoặc exit 0 sau **≈ 34 giây** |
| Giao diện Airflow, Superset, Lakekeeper, Trino, MinIO | Đều trả HTTP 200 |
| `make smoke`: Spark ghi 3 dòng → Trino đọc + ghi 1 dòng → Spark đọc lại | ✅ `{'spark': 3, 'trino': 1}`, chạy 2 lần liên tiếp cho cùng kết quả |
| `dag_smoke_test` (Airflow → BashOperator → spark-submit) | ✅ success, **17 giây** |
| Dữ liệu trên MinIO | Parquet nằm ở `warehouse/lakehouse/<table-uuid>/data/created_at_day=…/`, metadata ở `…/<table-uuid>/metadata/`; partition ẩn `days(created_at)` hoạt động |
| Chạy lại `init.sh` của postgres-meta và bootstrap Lakekeeper | Idempotent, không lỗi |
| RAM khi nghỉ (`docker stats`) | ≈ 2,4 GB tổng; Trino ≈ 1 GB, Airflow ≈ 0,8 GB, Superset ≈ 0,2 GB |

## Spark

**Quyết định: giữ Spark (local[*] trong container Airflow).**

Lý do:
- Chi phí khởi động Spark khoảng **3–4 giây** mỗi lần `spark-submit`; một job nhỏ tổng cộng khoảng 10 giây. Với 118 batch tháng, phần khởi động chỉ cộng thêm vài phút cho toàn bộ backfill.
- Spark và Trino dùng **cùng bản Iceberg 1.10.1**. Trino 480 đóng gói sẵn 1.10.1, nên Spark được chọn theo, đúng nguyên tắc "tổ hợp đã kiểm chứng tương thích".

## Lưu ý

1. **`CREATE OR REPLACE TABLE` chỉ có DDL trong Spark không xóa dữ liệu cũ** khi đi qua Lakekeeper: snapshot mới vẫn nối tiếp snapshot cũ. Trong khi đó, `CREATE OR REPLACE TABLE … AS SELECT` (cả Spark lẫn Trino) chạy đúng.
   → Không dùng REPLACE chỉ có DDL. Pipeline dùng `overwritePartitions()` hoặc `MERGE INTO` như thiết kế; khi cần dựng lại bảng thì dùng `DROP TABLE` rồi `CREATE TABLE`.
2. **Spark `DROP TABLE … PURGE` thất bại** trên bảng mà Trino vừa REPLACE (Lakekeeper báo không tìm thấy metadata). Xóa bằng Trino hoặc `DROP TABLE` không có PURGE thì được.
   → Kiểm tra lại khi viết `erase_customer.py` và `iceberg_maintenance.py` (tuần 8–10).
3. Cổng 5433 trên máy đã bị chiếm, nên cổng host của postgres-source là biến `SOURCE_DB_HOST_PORT` (mặc định 5434).

## Thay đổi so với cấu hình cũ

- `apache/iceberg-rest-fixture` + JdbcCatalog → **Lakekeeper**, đúng như Design Doc.
- 3 instance Postgres 13 → `postgres-meta` (Postgres 16, 3 database) + `postgres-source`.
- Bucket `raw` → `landing`. Catalog Trino `iceberg` → `lakehouse`.
- Bỏ mount `/var/run/docker.sock` vào Airflow. Superset có Dockerfile riêng, không còn phụ thuộc image `banking-superset` chỉ có trên một máy.
- `.env`: `ICEBERG_DB_*` → `LAKEKEEPER_DB_*`; thêm `LAKEKEEPER_ENCRYPTION_KEY`, `META_DB_ADMIN_*`, `SOURCE_DB_HOST_PORT`; mỗi dịch vụ có user database riêng.
