#!/bin/bash
# =============================================================================
# mapreduce/run_job.sh — Thực thi Hadoop Streaming MapReduce Job theo ngày
# =============================================================================

set -e

# Ngày phân tích: lấy từ tham số 1, hoặc biến môi trường LOG_DATE, hoặc mặc định hôm nay
LOG_DATE="${1:-${LOG_DATE:-$(date +%F)}}"

# Xác định HADOOP_HOME linh hoạt
HADOOP_HOME="${HADOOP_HOME:-$HOME/hadoop-3.2.1}"

# Tự động tìm Streaming JAR nếu chưa được chỉ định
if [ -z "$STREAMING_JAR" ]; then
    STREAMING_JAR=$(find "${HADOOP_HOME}/share/hadoop/tools/lib/" -name "hadoop-streaming*.jar" 2>/dev/null | head -n 1 || true)
fi

# Fallback mặc định
if [ -z "$STREAMING_JAR" ] || [ ! -f "$STREAMING_JAR" ]; then
    STREAMING_JAR="${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar"
fi

HDFS_INPUT="/data/logs/${LOG_DATE}"
HDFS_OUTPUT="/data/output/${LOG_DATE}"
LOCAL_RESULT_DIR="data"
LOCAL_RESULT_FILE="${LOCAL_RESULT_DIR}/mapreduce_results.txt"
DAILY_RESULT_FILE="${LOCAL_RESULT_DIR}/mapreduce_results_${LOG_DATE}.txt"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "=========================================================="
echo " [INFO] Bắt đầu chạy Hadoop Streaming MapReduce Job"
echo " Ngày phân tích: $LOG_DATE"
echo " Hadoop Home   : $HADOOP_HOME"
echo " Streaming JAR : $STREAMING_JAR"
echo " HDFS Input    : $HDFS_INPUT"
echo " HDFS Output   : $HDFS_OUTPUT"
echo "=========================================================="

# 1. Kiểm tra file JAR
if [ ! -f "$STREAMING_JAR" ]; then
    echo "[ERROR] Không tìm thấy file hadoop-streaming jar tại: $STREAMING_JAR"
    echo "        Hãy cấu hình đúng HADOOP_HOME hoặc STREAMING_JAR."
    exit 1
fi

# 2. Xóa output folder cũ trên HDFS nếu đã tồn tại
echo "[INFO] Dọn dẹp thư mục output cũ trên HDFS (nếu có)..."
hdfs dfs -rm -r -f "$HDFS_OUTPUT" || true

# 3. Chạy Hadoop Streaming Job
echo "[INFO] Submit MapReduce Job lên YARN/Hadoop cluster..."
hadoop jar "$STREAMING_JAR" \
    -files "${SCRIPT_DIR}/mapper.py,${SCRIPT_DIR}/reducer.py" \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -input "$HDFS_INPUT" \
    -output "$HDFS_OUTPUT"

echo "[INFO] MapReduce hoàn thành thành công!"

# 4. Kéo kết quả từ HDFS về thư mục data/ của project để chạy Rule-based Anomaly Detection
echo "[INFO] Đang tải kết quả từ HDFS về máy host..."
mkdir -p "${PROJECT_ROOT}/${LOCAL_RESULT_DIR}"
rm -f "${PROJECT_ROOT}/${LOCAL_RESULT_FILE}"
hdfs dfs -cat "${HDFS_OUTPUT}/part-*" > "${PROJECT_ROOT}/${LOCAL_RESULT_FILE}"
cp "${PROJECT_ROOT}/${LOCAL_RESULT_FILE}" "${PROJECT_ROOT}/${DAILY_RESULT_FILE}"

echo "=========================================================="
echo "✅ Kết quả MapReduce đã được lưu tại: ${PROJECT_ROOT}/${LOCAL_RESULT_FILE}"
echo "   Xem mẫu 10 dòng đầu:"
head -n 10 "${PROJECT_ROOT}/${LOCAL_RESULT_FILE}"
echo "=========================================================="
