#!/bin/bash
# Tạo database + user riêng cho từng dịch vụ nền tảng trong một instance (mục 9 Design Doc).
# Chạy tự động khi volume còn trống; chạy lại an toàn (bỏ qua user/database đã có).
set -euo pipefail
for svc in LAKEKEEPER AIRFLOW SUPERSET; do
  u="${svc}_DB_USER"; p="${svc}_DB_PASSWORD"; d="${svc}_DB_NAME"
  echo "Database ${!d} (user ${!u}) cho ${svc}"
  psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d postgres \
       -v u="${!u}" -v p="${!p}" -v d="${!d}" <<'SQL'
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'u', :'p')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'u') \gexec
SELECT format('CREATE DATABASE %I OWNER %I', :'d', :'u')
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = :'d') \gexec
SELECT format('REVOKE ALL ON DATABASE %I FROM PUBLIC', :'d') \gexec
SQL
done
