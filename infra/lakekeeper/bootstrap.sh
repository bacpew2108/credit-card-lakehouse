#!/bin/sh
# Khởi tạo Lakekeeper (một lần) và tạo mỗi tầng một warehouse trên bucket MinIO cùng tên:
# warehouse bronze → s3://bronze, silver → s3://silver, gold → s3://gold, ops → s3://ops.
# Mỗi warehouse dùng tài khoản MinIO lakekeeper-<tầng>, chỉ có quyền trên bucket của tầng đó.
# Spark không giữ khóa S3: Lakekeeper ký từng request (remote signing) bằng credential này.
# Chạy lại an toàn: warehouse đã có thì cập nhật credential.
set -eu
API=http://lakekeeper:8181/management/v1

code=$(curl -s -o /tmp/out -w '%{http_code}' -X POST "$API/bootstrap" \
  -H 'Content-Type: application/json' --data '{"accept-terms-of-use": true}')
case "$code" in
  2*) echo "Bootstrap OK" ;;
  *) grep -qi 'already' /tmp/out && echo "Đã bootstrap trước đó" || { echo "Bootstrap lỗi $code"; cat /tmp/out; exit 1; } ;;
esac

credential() {
  secret=$(printenv "MINIO_SECRET_LAKEKEEPER_$(echo "$1" | tr 'a-z' 'A-Z')")
  cat <<JSON
{"type": "s3", "credential-type": "access-key",
 "access-key-id": "lakekeeper-$1", "secret-access-key": "$secret"}
JSON
}

request() {   # $1 method, $2 path, $3 file JSON, $4 mô tả
  code=$(curl -s -o /tmp/out -w '%{http_code}' -X "$1" "$API$2" \
    -H 'Content-Type: application/json' --data @"$3")
  case "$code" in
    2*) echo "$4" ;;
    *) echo "$4 lỗi $code"; cat /tmp/out; exit 1 ;;
  esac
}

existing=$(curl -sf "$API/warehouse")
for wh in ${WAREHOUSES}; do
  id=$(echo "$existing" | tr '{' '\n' | grep "\"name\":\"${wh}\"" | sed 's/.*"id":"\([^"]*\)".*/\1/' | head -1)
  if [ -n "$id" ]; then
    echo "{\"new-storage-credential\": $(credential "$wh")}" > /tmp/cred.json
    request POST "/warehouse/$id/storage-credential" /tmp/cred.json "Warehouse ${wh}: cập nhật credential lakekeeper-${wh}"
    continue
  fi
  cat > /tmp/warehouse.json <<JSON
{
  "warehouse-name": "${wh}",
  "storage-profile": {
    "type": "s3",
    "bucket": "${wh}",
    "endpoint": "http://minio:9000",
    "region": "us-east-1",
    "path-style-access": true,
    "flavor": "s3-compat",
    "sts-enabled": false
  },
  "storage-credential": $(credential "$wh")
}
JSON
  request POST /warehouse /tmp/warehouse.json "Đã tạo warehouse ${wh} → s3://${wh}"
done
