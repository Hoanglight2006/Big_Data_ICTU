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

HDFS_INPUT="/data/cleaned/${LOG_DATE}"
HDFS_OUTPUT="/data/output/${LOG_DATE}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "=========================================================="
echo " [INFO] BAT DAU MAPREDUCE AGGREGATION TREN YARN"
echo " Ngày phân tích : $LOG_DATE"
echo " Hadoop Home    : $HADOOP_HOME"
echo " Streaming JAR  : $STREAMING_JAR"
echo " HDFS Input     : $HDFS_INPUT"
echo " HDFS Output    : $HDFS_OUTPUT"
echo "=========================================================="

# 1. Kiểm tra file JAR
if [ ! -f "$STREAMING_JAR" ]; then
    echo "[ERROR] Không tìm thấy file hadoop-streaming jar tại: $STREAMING_JAR"
    echo "        Hãy cấu hình đúng HADOOP_HOME hoặc STREAMING_JAR."
    exit 1
fi

# 2. Kiểm tra dữ liệu đầu vào đã làm sạch trên HDFS
if ! hdfs dfs -test -e "$HDFS_INPUT"; then
    echo "[ERROR] Không tìm thấy dữ liệu đã làm sạch trên HDFS: $HDFS_INPUT"
    echo "        Hãy chạy bước tiền xử lý (preprocessing/run_cleaner.sh) trước."
    exit 1
fi

# 3. Xóa output folder cũ trên HDFS nếu đã tồn tại
echo "[INFO] Dọn dẹp thư mục output cũ trên HDFS (nếu có)..."
hdfs dfs -rm -r -f "$HDFS_OUTPUT" || true

# 4. Khởi động JobHistoryServer nếu chưa chạy (để tránh lỗi retry port 10020 sau khi job hoàn thành)
if [ -f "${HADOOP_HOME}/sbin/mr-jobhistory-daemon.sh" ]; then
    "${HADOOP_HOME}/sbin/mr-jobhistory-daemon.sh" start historyserver 2>/dev/null || true
fi

# 5. Chạy Hadoop Streaming Job trên YARN
echo "[INFO] Submit MapReduce Job lên cụm YARN..."
export HADOOP_CLASSPATH=$("${HADOOP_HOME}/bin/hadoop" classpath 2>/dev/null || true)

set +e
hadoop jar "$STREAMING_JAR" \
    -D yarn.app.mapreduce.am.env="HADOOP_MAPRED_HOME=${HADOOP_HOME}" \
    -D mapreduce.map.env="HADOOP_MAPRED_HOME=${HADOOP_HOME}" \
    -D mapreduce.reduce.env="HADOOP_MAPRED_HOME=${HADOOP_HOME}" \
    -D mapreduce.job.name="MapReduce_LogAggregation_${LOG_DATE}" \
    -files "${SCRIPT_DIR}/mapper.py,${SCRIPT_DIR}/reducer.py" \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -input "$HDFS_INPUT" \
    -output "$HDFS_OUTPUT"
JOB_STATUS=$?
set -e

# Kiểm tra nếu HDFS output đã có file part-* hoặc _SUCCESS thì Job thực tế đã chạy xong
if [ $JOB_STATUS -ne 0 ]; then
    if hdfs dfs -test -e "${HDFS_OUTPUT}/_SUCCESS" 2>/dev/null || [ $(hdfs dfs -ls "${HDFS_OUTPUT}" 2>/dev/null | grep -c "part-") -gt 0 ]; then
        echo "[WARN] Client không kết nối được JobHistoryServer (10020), nhưng Job MapReduce trên YARN đã hoàn thành và sinh file output thành công."
        JOB_STATUS=0
    else
        echo "[ERROR] MapReduce Job thực sự thất bại."
        exit $JOB_STATUS
    fi
fi

echo "[INFO] MapReduce hoàn thành thành công trên YARN!"
echo "[INFO] Dữ liệu tổng hợp được lưu trực tiếp trên HDFS tại: $HDFS_OUTPUT"
hdfs dfs -ls "$HDFS_OUTPUT"

echo "=========================================================="
echo " [INFO] Mẫu 10 dòng kết quả tổng hợp trên HDFS:"
hdfs dfs -cat "${HDFS_OUTPUT}/part-*" 2>/dev/null | head -n 10
echo "=========================================================="
