#!/bin/bash
# =============================================================================
# flume/start_flume.sh — Khởi động Apache Flume Agent (Terminal 1)
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
SPOOL_DIR="${PROJECT_ROOT}/data/spool"
RUNTIME_CONF="${SCRIPT_DIR}/flume-runtime.conf"

HADOOP_HOME="${HADOOP_HOME:-$HOME/hadoop-3.2.1}"

echo "=========================================================="
echo " [INFO] KHOI DONG APACHE FLUME AGENT (TERMINAL 1)"
echo " Thu muc Spool dir : $SPOOL_DIR"
echo " Hadoop Home       : $HADOOP_HOME"
echo "=========================================================="

# 1. Đảm bảo thư mục spool tồn tại
mkdir -p "$SPOOL_DIR"

# 2. Sinh file cấu hình runtime với đường dẫn spoolDir tuyệt đối của máy hiện tại
sed "s|@SPOOL_DIR@|${SPOOL_DIR}|g" "${SCRIPT_DIR}/flume-hdfs.conf" > "$RUNTIME_CONF"

# 3. Nạp thư viện Hadoop vào Classpath của Flume để ghi vào HDFS Sink
if [ -d "$HADOOP_HOME" ]; then
    export FLUME_CLASSPATH=$("${HADOOP_HOME}/bin/hadoop" classpath 2>/dev/null || true)
    echo " [INFO] Da nap Hadoop Classpath thanh cong."
else
    echo " [WARN] Khong tim thay HADOOP_HOME tai $HADOOP_HOME. Flume co the thieu thu vien HDFS."
fi

# 4. Kiểm tra lệnh flume-ng
if ! command -v flume-ng &> /dev/null; then
    # Thử tìm trong các đường dẫn cài đặt thông dụng
    for FLUME_DIR in "/opt/flume" "/opt/apache-flume" "$HOME/apache-flume-1.9.0-bin" "$HOME/flume"; do
        if [ -x "${FLUME_DIR}/bin/flume-ng" ]; then
            export PATH="${FLUME_DIR}/bin:$PATH"
            break
        fi
    done
fi

if ! command -v flume-ng &> /dev/null; then
    echo " [ERROR] Khong tim thay lenh 'flume-ng'. Hay cai dat Apache Flume hoac cau hinh PATH."
    exit 1
fi

echo " [INFO] Flume Agent 'a1' bat dau lang nghe tai: $SPOOL_DIR ..."
echo " [INFO] Khi co file log moi, Flume se tu dong nap len HDFS (/data/raw/%Y-%m-%d/)"
echo "----------------------------------------------------------"

exec flume-ng agent \
    --conf-file "$RUNTIME_CONF" \
    --name a1 \
    -Dflume.root.logger=INFO,console
