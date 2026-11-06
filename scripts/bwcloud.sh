#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
: "${S3_ENDPOINT:?Set the plain MinIO HTTP(S) URL}"
: "${S3_BUCKET:?Set S3_BUCKET}"
: "${AWS_ACCESS_KEY_ID:?Set AWS_ACCESS_KEY_ID}"
: "${AWS_SECRET_ACCESS_KEY:?Set AWS_SECRET_ACCESS_KEY}"
export BWGRAPH_STORAGE_BACKEND=s3
export AWS_REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-us-east-1}}"
export S3_PREFIX="${S3_PREFIX:-bw-cloud-graph-java}"
export BWGRAPH_S3_LOCAL_ROOT="${BWGRAPH_S3_LOCAL_ROOT:-$PWD/workspace/bw-cloud-graph}"
if [[ "${1:-start}" = start ]]; then
  export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS:-} -Dmonac.upload.host=${MONAC_UPLOAD_HOST:-127.0.0.1}"
  exec bash scripts/bwgraph.sh start "${2:-gremlin-server-bw-cloud.yaml}"
else
  exec bash scripts/bwgraph.sh "$@"
fi
