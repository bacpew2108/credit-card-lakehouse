#!/bin/sh
# Khởi tạo Lakekeeper (một lần) và tạo warehouse "lakehouse" trên bucket warehouse của MinIO.
# Chạy lại an toàn: đã bootstrap / warehouse đã tồn tại thì bỏ qua.
set -eu
API=http://lakekeeper:8181/management/v1

code=$(curl -s -o /tmp/out -w '%{http_code}' -X POST "$API/bootstrap" \
  -H 'Content-Type: application/json' --data '{"accept-terms-of-use": true}')
case "$code" in
  2*) echo "Bootstrap OK" ;;
  *) grep -qi 'already' /tmp/out && echo "Đã bootstrap trước đó" || { echo "Bootstrap lỗi $code"; cat /tmp/out; exit 1; } ;;
esac

if curl -sf "$API/warehouse" | grep -q "\"name\":\"${WAREHOUSE_NAME}\""; then
  echo "Warehouse ${WAREHOUSE_NAME} đã tồn tại"
  exit 0
fi

cat > /tmp/warehouse.json <<JSON
{
  "warehouse-name": "${WAREHOUSE_NAME}",
  "storage-profile": {
    "type": "s3",
    "bucket": "warehouse",
    "key-prefix": "${WAREHOUSE_NAME}",
    "endpoint": "http://minio:9000",
    "region": "us-east-1",
    "path-style-access": true,
    "flavor": "s3-compat",
    "sts-enabled": false
  },
  "storage-credential": {
    "type": "s3",
    "credential-type": "access-key",
    "access-key-id": "${MINIO_ACCESS_KEY}",
    "secret-access-key": "${MINIO_SECRET_KEY}"
  }
}
JSON
code=$(curl -s -o /tmp/out -w '%{http_code}' -X POST "$API/warehouse" \
  -H 'Content-Type: application/json' --data @/tmp/warehouse.json)
case "$code" in
  2*) echo "Đã tạo warehouse ${WAREHOUSE_NAME}" ;;
  *) echo "Tạo warehouse lỗi $code"; cat /tmp/out; exit 1 ;;
esac
