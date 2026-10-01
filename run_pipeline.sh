#!/bin/bash
# =============================================================================
# run_pipeline.sh — Tự Động Hóa 100% Pipeline Phân Tích Log Theo Ngày
#
# Đề tài: Log Analysis and Anomaly Detection for Web Servers
# Chương trình: Samsung Innovation Campus (SIC) - Group 3
# =============================================================================

set -e

# Xác định thư mục dự án linh hoạt
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Kích hoạt Python virtualenv nếu tồn tại
if [ -d "venv" ]; then
    source venv/bin/activate
fi

# Thiết lập các biến môi trường (Mặc định lấy ngày hôm nay: YYYY-MM-DD)
export LOG_DATE="${1:-${LOG_DATE:-$(date +%F)}}"
export HADOOP_HOME="${HADOOP_HOME:-$HOME/hadoop-3.2.1}"

echo "=========================================================="
echo " 🚀 BẮT ĐẦU PIPELINE PHÂN TÍCH LOG BATCH THEO NGÀY (SIC)"
echo " Ngày phân tích: $LOG_DATE"
echo " Hadoop Home   : $HADOOP_HOME"
echo " Thư mục chạy  : $SCRIPT_DIR"
echo "=========================================================="

# -----------------------------------------------------------------------------
# BƯỚC 1: KIỂM TRA & TỰ ĐỘNG BẬT CÁC DỊCH VỤ HADOOP CLUSTER
# -----------------------------------------------------------------------------
echo -e "\n[1/6] Kiểm tra dịch vụ Hadoop (HDFS & YARN)..."

if hdfs dfs -ls / &> /dev/null; then
    echo "  ✅ Cụm Hadoop (HDFS) đang hoạt động và sẵn sàng nhận kết nối!"
    hdfs dfsadmin -safemode leave 2>/dev/null || true
else
    CURRENT_USER=$(whoami)
    echo "  ⚠️ Chưa kết nối được HDFS -> Đang khởi động Hadoop với quyền người dùng: $CURRENT_USER..."
    if [ -f "${HADOOP_HOME}/sbin/start-all.sh" ]; then
        export HDFS_NAMENODE_USER=$CURRENT_USER
        export HDFS_DATANODE_USER=$CURRENT_USER
        export HDFS_SECONDARYNAMENODE_USER=$CURRENT_USER
        export YARN_RESOURCEMANAGER_USER=$CURRENT_USER
        export YARN_NODEMANAGER_USER=$CURRENT_USER
        "${HADOOP_HOME}/sbin/start-all.sh" || true
        
        echo "  Đang đợi các tiến trình Hadoop khởi động ổn định..."
        for i in {1..15}; do
            if hdfs dfs -ls / &> /dev/null; then
                echo "  ✅ Hadoop đã khởi động thành công và sẵn sàng nhận kết nối!"
                break
            fi
            sleep 2
        done
    fi
fi

# Kiểm tra dịch vụ YARN (ResourceManager phục vụ MapReduce)
if ! yarn node -list &> /dev/null; then
    echo "  ⚠️ Dịch vụ YARN (ResourceManager) chưa chạy -> Đang tự động khởi động YARN..."
    CURRENT_USER=$(whoami)
    export YARN_RESOURCEMANAGER_USER=$CURRENT_USER
    export YARN_NODEMANAGER_USER=$CURRENT_USER
    if [ -f "${HADOOP_HOME}/sbin/start-yarn.sh" ]; then
        "${HADOOP_HOME}/sbin/start-yarn.sh" || true
        sleep 5
    fi
else
    echo "  ✅ Dịch vụ YARN (ResourceManager) đang hoạt động sẵn sàng!"
fi

# -----------------------------------------------------------------------------
# BƯỚC 2: SINH DỮ LIỆU LOG (ĐẶNG VĂN VINH)
# -----------------------------------------------------------------------------
echo -e "\n[2/6] [ĐẶNG VĂN VINH] Sinh dữ liệu log giả lập 24 giờ cho ngày $LOG_DATE..."
python3 data/generate_logs.py

# -----------------------------------------------------------------------------
# BƯỚC 3: INGESTION VÀO HDFS THEO NGÀY (ĐẶNG VĂN VINH - FLUME)
# -----------------------------------------------------------------------------
echo -e "\n[3/6] [ĐẶNG VĂN VINH] Thu thập & nạp dữ liệu vào HDFS phân vùng theo ngày (/data/logs/$LOG_DATE)..."
python3 flume/ingest_to_hdfs.py

# -----------------------------------------------------------------------------
# BƯỚC 4: LÀM SẠCH & CHUẨN HÓA DỮ LIỆU (NÔNG MINH TRÍ & TRIỆU VĂN HUY)
# -----------------------------------------------------------------------------
echo -e "\n[4/6] [NÔNG MINH TRÍ & TRIỆU VĂN HUY] Tiền xử lý, lọc rác và chuẩn hóa dữ liệu..."
python3 preprocessing/cleaner.py

# -----------------------------------------------------------------------------
# BƯỚC 5: CHẠY HADOOP STREAMING MAPREDUCE (DƯƠNG ĐÌNH HOÀNG)
# -----------------------------------------------------------------------------
echo -e "\n[5/6] [DƯƠNG ĐÌNH HOÀNG] Thực thi Hadoop MapReduce Streaming phân tán trên YARN..."
bash mapreduce/run_job.sh "$LOG_DATE"

# -----------------------------------------------------------------------------
# BƯỚC 6: PHÂN TÍCH BẤT THƯỜNG & XUẤT BÁO CÁO (DƯƠNG ĐÌNH HOÀNG)
# -----------------------------------------------------------------------------
echo -e "\n[6/6] [DƯƠNG ĐÌNH HOÀNG] Phân tích bất thường (Rule-based Anomaly Detection)..."
python3 anomaly/detector.py

echo "=========================================================="
echo "🎉 HOÀN TẤT TOÀN BỘ PIPELINE BATCH THEO NGÀY THÀNH CÔNG!"
echo " Báo cáo đã lưu tại: data/anomaly_report.json"
echo "=========================================================="
