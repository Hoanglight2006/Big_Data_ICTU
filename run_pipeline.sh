#!/bin/bash
# =============================================================================
# run_pipeline.sh — Pipeline phan tich log batch theo ngay
#
# De tai: Log Analysis and Anomaly Detection for Web Servers
# Chuong trinh: Samsung Innovation Campus (SIC) - Group 3
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [ -d "venv" ]; then
    source venv/bin/activate
fi

export LOG_DATE="${1:-${LOG_DATE:-$(date +%F)}}"
export HADOOP_HOME="${HADOOP_HOME:-$HOME/hadoop-3.2.1}"

echo "=========================================================="
echo " BẮT ĐẦU PIPELINE PHÂN TÍCH LOG"
echo " Ngày phân tích: $LOG_DATE"
echo " Hadoop Home   : $HADOOP_HOME"
echo " Thư mục chạy  : $SCRIPT_DIR"
echo "=========================================================="

# -----------------------------------------------------------------------------
# BƯỚC 1: KIỂM TRA & KHỞI ĐỘNG DỊCH VỤ HADOOP CLUSTER
# -----------------------------------------------------------------------------
echo -e "\n[1/6] Kiểm tra dịch vụ Hadoop (HDFS & YARN)..."

if hdfs dfs -ls / &> /dev/null; then
    echo "  [INFO] Cụm Hadoop (HDFS) đang hoạt động."
    hdfs dfsadmin -safemode leave 2>/dev/null || true
else
    CURRENT_USER=$(whoami)
    echo "  [CẢNH BÁO] Chưa kết nối được HDFS. Khởi động Hadoop với user: $CURRENT_USER..."
    if [ -f "${HADOOP_HOME}/sbin/start-all.sh" ]; then
        export HDFS_NAMENODE_USER=$CURRENT_USER
        export HDFS_DATANODE_USER=$CURRENT_USER
        export HDFS_SECONDARYNAMENODE_USER=$CURRENT_USER
        export YARN_RESOURCEMANAGER_USER=$CURRENT_USER
        export YARN_NODEMANAGER_USER=$CURRENT_USER
        "${HADOOP_HOME}/sbin/start-all.sh" || true
        
        echo "  [INFO] Chờ các tiến trình Hadoop khởi động..."
        for i in {1..15}; do
            if hdfs dfs -ls / &> /dev/null; then
                echo "  [INFO] Hadoop đã sẵn sàng."
                break
            fi
            sleep 2
        done
    fi
fi

# Kiểm tra dịch vụ YARN (ResourceManager)
if ! yarn node -list &> /dev/null; then
    echo "  [CẢNH BÁO] YARN ResourceManager chưa chạy. Đang khởi động YARN..."
    CURRENT_USER=$(whoami)
    export YARN_RESOURCEMANAGER_USER=$CURRENT_USER
    export YARN_NODEMANAGER_USER=$CURRENT_USER
    if [ -f "${HADOOP_HOME}/sbin/start-yarn.sh" ]; then
        "${HADOOP_HOME}/sbin/start-yarn.sh" || true
        sleep 5
    fi
else
    echo "  [INFO] Dịch vụ YARN (ResourceManager) đang hoạt động."
fi

# -----------------------------------------------------------------------------
# BƯỚC 2: SINH DỮ LIỆU LOG
# -----------------------------------------------------------------------------
echo -e "\n[2/6] Sinh dữ liệu log web server cho ngày $LOG_DATE..."
python3 data/generate_logs.py

# -----------------------------------------------------------------------------
# BƯỚC 3: INGESTION VÀO HDFS RAW ZONE
# -----------------------------------------------------------------------------
echo -e "\n[3/6] Flume Ingestion nạp dữ liệu vào HDFS (/data/raw/$LOG_DATE)..."
python3 flume/ingest_to_hdfs.py

# -----------------------------------------------------------------------------
# BƯỚC 4: TIỀN XỬ LÝ & LÀM SẠCH PHÂN TÁN TRÊN YARN
# -----------------------------------------------------------------------------
echo -e "\n[4/6] Tiền xử lý Map-only trên YARN (/data/cleaned/$LOG_DATE)..."
bash preprocessing/run_cleaner.sh "$LOG_DATE"

# -----------------------------------------------------------------------------
# BƯỚC 5: TỔNG HỢP MAPREDUCE TRÊN YARN
# -----------------------------------------------------------------------------
echo -e "\n[5/6] MapReduce tổng hợp trên YARN (/data/output/$LOG_DATE)..."
bash mapreduce/run_job.sh "$LOG_DATE"

# -----------------------------------------------------------------------------
# BƯỚC 6: PHÂN TÍCH BẤT THƯỜNG & LƯU BÁO CÁO HDFS
# -----------------------------------------------------------------------------
echo -e "\n[6/6] Phát hiện bất thường từ luồng dữ liệu HDFS..."
python3 anomaly/detector.py --date "$LOG_DATE"

echo "=========================================================="
echo " HOÀN TẤT PIPELINE PHÂN TÍCH LOG"
echo " Thư mục dữ liệu HDFS theo ngày $LOG_DATE:"
echo "   - Raw Data    : /data/raw/$LOG_DATE"
echo "   - Cleaned Data: /data/cleaned/$LOG_DATE"
echo "   - Aggregation : /data/output/$LOG_DATE"
echo "   - Report      : /data/reports/$LOG_DATE"
echo "=========================================================="
