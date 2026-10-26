#!/bin/sh
# Tạo bucket và tài khoản dịch vụ của MinIO. Mỗi tài khoản chỉ có quyền trên đúng bucket nó cần,
# nên phân quyền được MinIO cưỡng chế, không phụ thuộc engine truy vấn. Chạy lại an toàn.
#
#   lakekeeper-<tầng>  đọc/ghi bucket <tầng>  credential của warehouse <tầng>; Spark đọc/ghi qua
#                                             Lakekeeper (remote signing), không giữ khóa S3
#   trino              đọc bronze, silver, gold; đọc/ghi ops
#   landing-writer     đọc/ghi landing        seed_source.py tải file tham chiếu lên
#   landing-reader     đọc landing            job load_files.py trong Airflow
set -eu

mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null
for b in landing bronze silver gold ops; do
  mc mb --ignore-existing "local/$b"
done

# $1 tên policy, $2 hành động trên object, các tham số còn lại là bucket
policy() {
  name=$1 actions=$2
  shift 2
  buckets="" objects=""
  for b in "$@"; do
    buckets="$buckets\"arn:aws:s3:::$b\","
    objects="$objects\"arn:aws:s3:::$b/*\","
  done
  cat > "/tmp/$name.json" <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {"Effect": "Allow", "Action": ["s3:GetBucketLocation", "s3:ListBucket", "s3:ListBucketMultipartUploads"],
     "Resource": [${buckets%,}]},
    {"Effect": "Allow", "Action": [$actions], "Resource": [${objects%,}]}
  ]
}
JSON
  mc admin policy create local "$name" "/tmp/$name.json" >/dev/null
}
READ='"s3:GetObject"'
WRITE='"s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"'

# $1 tài khoản, $2 secret, $3 policy
account() {
  mc admin user add local "$1" "$2" >/dev/null
  mc admin policy attach local "$3" --user "$1" >/dev/null 2>&1 || true   # đã gắn thì bỏ qua
  echo "Tài khoản $1 → policy $3"
}

for layer in bronze silver gold ops; do
  policy "rw-$layer" "$WRITE" "$layer"
done
policy read-layers "$READ" bronze silver gold
policy rw-landing "$WRITE" landing
policy read-landing "$READ" landing

account lakekeeper-bronze "$MINIO_SECRET_LAKEKEEPER_BRONZE" rw-bronze
account lakekeeper-silver "$MINIO_SECRET_LAKEKEEPER_SILVER" rw-silver
account lakekeeper-gold "$MINIO_SECRET_LAKEKEEPER_GOLD" rw-gold
account lakekeeper-ops "$MINIO_SECRET_LAKEKEEPER_OPS" rw-ops
account trino "$MINIO_SECRET_TRINO" read-layers
mc admin policy attach local rw-ops --user trino >/dev/null 2>&1 || true
account landing-writer "$MINIO_SECRET_LANDING_WRITER" rw-landing
account landing-reader "$MINIO_SECRET_LANDING_READER" read-landing
echo "MinIO sẵn sàng: bucket landing, bronze, silver, gold, ops"
