set -e

LOG_DATE="${1:-${LOG_DATE:-$(date +%F)}}"
HADOOP_HOME="${HADOOP_HOME:-$HOME/hadoop-3.2.1}"

if [ -z "$STREAMING_JAR" ]; then
    STREAMING_JAR=$(find "${HADOOP_HOME}/share/hadoop/tools/lib/" -name "hadoop-streaming*.jar" 2>/dev/null | head -n 1 || true)
fi

if [ -z "$STREAMING_JAR" ] || [ ! -f "$STREAMING_JAR" ]; then
    STREAMING_JAR="${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar"
fi

HDFS_RAW_INPUT="/data/raw/${LOG_DATE}"
HDFS_CLEANED_OUTPUT="/data/cleaned/${LOG_DATE}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=========================================================="
echo " BẮT ĐẦU TIỀN XỬ LÝ (MAP-ONLY YARN)"
echo " Ngày phân tích : $LOG_DATE"
echo " Hadoop Home    : $HADOOP_HOME"
echo " HDFS Input Raw : $HDFS_RAW_INPUT"
echo " HDFS Output    : $HDFS_CLEANED_OUTPUT"
echo "=========================================================="

if [ ! -f "$STREAMING_JAR" ]; then
    echo "[LỖI] Không tìm thấy file hadoop-streaming jar tại: $STREAMING_JAR"
    exit 1
fi

# 1. Kiểm tra dữ liệu đầu vào trên HDFS
if ! hdfs dfs -test -e "$HDFS_RAW_INPUT"; then
    echo "[LỖI] Thư mục đầu vào không tồn tại trên HDFS: $HDFS_RAW_INPUT"
    echo "      Cần chạy bước Flume Ingestion trước."
    exit 1
fi

# 2. Xóa output cũ trên HDFS nếu đã tồn tại
echo "[INFO] Dọn dẹp thư mục output cũ trên HDFS (nếu có)..."
hdfs dfs -rm -r -f "$HDFS_CLEANED_OUTPUT" || true

# 3. Submit Map-only Job lên cụm YARN
echo "[INFO] Submit Map-only Data Cleaning Job lên cụm YARN..."
export HADOOP_CLASSPATH=$("${HADOOP_HOME}/bin/hadoop" classpath 2>/dev/null || true)

hadoop jar "$STREAMING_JAR" \
    -D yarn.app.mapreduce.am.env="HADOOP_MAPRED_HOME=${HADOOP_HOME}" \
    -D mapreduce.map.env="HADOOP_MAPRED_HOME=${HADOOP_HOME}" \
    -D mapreduce.job.name="Preprocessing_DataCleaning_${LOG_DATE}" \
    -files "${SCRIPT_DIR}/cleaner_mapper.py" \
    -mapper "python3 cleaner_mapper.py" \
    -numReduceTasks 0 \
    -input "$HDFS_RAW_INPUT" \
    -output "$HDFS_CLEANED_OUTPUT"

echo "[INFO] Tiền xử lý hoàn tất."
echo "[INFO] Dữ liệu sạch trên HDFS:"
hdfs dfs -ls "$HDFS_CLEANED_OUTPUT"
echo "[INFO] Mẫu 5 dòng dữ liệu sạch trên HDFS:"
hdfs dfs -cat "${HDFS_CLEANED_OUTPUT}/part-*" 2>/dev/null | head -n 5
echo "=========================================================="
