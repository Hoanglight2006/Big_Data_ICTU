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
echo " KHỞI ĐỘNG APACHE FLUME AGENT (TERMINAL 1)"
echo " Thư mục Spool dir : $SPOOL_DIR"
echo " Hadoop Home       : $HADOOP_HOME"
echo "=========================================================="

# 1. Đảm bảo thư mục spool tồn tại
mkdir -p "$SPOOL_DIR"

# 2. Sinh file cấu hình runtime với đường dẫn spoolDir tuyệt đối của máy hiện tại
sed "s|@SPOOL_DIR@|${SPOOL_DIR}|g" "${SCRIPT_DIR}/flume-hdfs.conf" > "$RUNTIME_CONF"

# 3. Kiểm tra & Tự động khởi động Hadoop nếu chưa chạy
if hdfs dfs -ls / &> /dev/null; then
    echo " [INFO] Dịch vụ Hadoop (HDFS) đang hoạt động."
    hdfs dfsadmin -safemode leave 2>/dev/null || true
else
    CURRENT_USER=$(whoami)
    echo " [CẢNH BÁO] Chưa kết nối được HDFS. Tự động khởi động Hadoop..."
    if [ -f "${HADOOP_HOME}/sbin/start-all.sh" ]; then
        export HDFS_NAMENODE_USER=$CURRENT_USER
        export HDFS_DATANODE_USER=$CURRENT_USER
        export HDFS_SECONDARYNAMENODE_USER=$CURRENT_USER
        export YARN_RESOURCEMANAGER_USER=$CURRENT_USER
        export YARN_NODEMANAGER_USER=$CURRENT_USER
        "${HADOOP_HOME}/sbin/start-all.sh" || true
        
        for i in {1..15}; do
            if hdfs dfs -ls / &> /dev/null; then
                echo " [INFO] Hadoop HDFS đã sẵn sàng."
                break
            fi
            sleep 2
        done
    fi
fi

# 4. Nạp thư viện Hadoop vào Classpath của Flume để ghi vào HDFS Sink
if [ -d "$HADOOP_HOME" ]; then
    export FLUME_CLASSPATH=$("${HADOOP_HOME}/bin/hadoop" classpath 2>/dev/null || true)
    echo " [INFO] Đã nạp Hadoop Classpath."
else
    echo " [CẢNH BÁO] Không tìm thấy HADOOP_HOME tại $HADOOP_HOME. Flume có thể thiếu thư viện HDFS."
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
    echo " [LỖI] Không tìm thấy lệnh 'flume-ng'. Cần cài đặt Apache Flume hoặc cấu hình PATH."
    exit 1
fi

# 5. Cấp đủ RAM cho Flume JVM để tránh OutOfMemoryError khi xử lý file log lớn
export JAVA_OPTS="-Xms256m -Xmx1024m ${JAVA_OPTS:-}"

exec flume-ng agent \
    --conf-file "$RUNTIME_CONF" \
    --name a1 \
    -Dflume.root.logger=INFO,console
