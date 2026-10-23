#!/bin/bash
# =============================================================================
# flume/start_flume.sh — Khởi chạy Flume Agent tự động nhận diện đường dẫn spool
# =============================================================================
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
SPOOL_DIR="${SPOOL_DIR:-${REPO_DIR}/data/spool}"

mkdir -p "$SPOOL_DIR"

echo "=========================================================="
echo " [INFO] KHOI DONG APACHE FLUME AGENT (a1)"
echo " Repository root : $REPO_DIR"
echo " Spool Directory : $SPOOL_DIR"
echo " Flume Config    : ${SCRIPT_DIR}/flume-hdfs.conf"
echo " HDFS Path       : hdfs://localhost:9000/data/raw/%Y-%m-%d"
echo "=========================================================="

# Đảm bảo FLUME_CLASSPATH có thư viện Hadoop
if [ -z "$FLUME_CLASSPATH" ]; then
    if command -v hadoop &> /dev/null; then
        export FLUME_CLASSPATH=$(hadoop classpath)
    fi
fi

# Chạy Flume từ thư mục gốc của repository
cd "$REPO_DIR"

flume-ng agent \
    --conf "${SCRIPT_DIR}" \
    --conf-file "${SCRIPT_DIR}/flume-hdfs.conf" \
    --name a1 \
    -Dflume.root.logger=INFO,console
