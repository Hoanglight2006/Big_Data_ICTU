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
export HADOOP_HOME="${HADOOP_HOME:-/home/hoang/hadoop-3.2.1}"

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
else
    echo "  ⚠️ Chưa kết nối được HDFS -> Đang thử khởi động Hadoop nội bộ..."
    if [ -f "${HADOOP_HOME}/sbin/start-all.sh" ]; then
        export HDFS_NAMENODE_USER=root
        export HDFS_DATANODE_USER=root
        export HDFS_SECONDARYNAMENODE_USER=root
        export YARN_RESOURCEMANAGER_USER=root
        export YARN_NODEMANAGER_USER=root
        "${HADOOP_HOME}/sbin/start-all.sh" || true
        sleep 5
    fi
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
