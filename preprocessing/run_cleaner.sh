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

echo " BAT DAU TIEN XU LY"
echo " Ngày phân tích : $LOG_DATE"
echo " Hadoop Home    : $HADOOP_HOME"
echo " HDFS Input Raw : $HDFS_RAW_INPUT"
echo " HDFS Output    : $HDFS_CLEANED_OUTPUT"

if [ ! -f "$STREAMING_JAR" ]; then
    echo "Không tìm thấy file hadoop-streaming jar tại: $STREAMING_JAR"
    exit 1
fi

# 1. Kiểm tra dữ liệu đầu vào trên HDFS
if ! hdfs dfs -test -e "$HDFS_RAW_INPUT"; then
    echo "Thu muc dau vao khong ton tai tren HDFS: $HDFS_RAW_INPUT"
    echo "        Hay chay buoc Flume Ingestion truoc."
    exit 1
fi

# 2. Xóa output cũ trên HDFS nếu đã tồn tại
echo "Don dep thu muc output cu neu co tren HDFS..."
hdfs dfs -rm -r -f "$HDFS_CLEANED_OUTPUT" || true

# 3. Submit Map-only Job lên cụm YARN
echo "Submit Map-only Data Cleaning Job len YARN cluster..."
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

echo "Tien xu ly da hoan thanh"
echo "Kiem tra du lieu sach tren HDFS:"
hdfs dfs -ls "$HDFS_CLEANED_OUTPUT"
echo "Mau 5 dong du lieu sach tren HDFS:"
hdfs dfs -cat "${HDFS_CLEANED_OUTPUT}/part-*" 2>/dev/null | head -n 5
echo "=========================================================="
