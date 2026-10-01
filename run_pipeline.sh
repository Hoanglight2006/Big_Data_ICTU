#!/bin/bash
# =============================================================================
# run_pipeline.sh — Tự Động Hóa 100% Pipeline
# =============================================================================

set -e

PROJECT_DIR="/home/hoang/log-simulation"
HADOOP_HOME="${HADOOP_HOME:-/home/hoang/hadoop-3.2.1}"
KAFKA_DIR="/home/hoang/kafka_2.12-2.8.1"

cd "$PROJECT_DIR"
source venv/bin/activate

echo "=========================================================="
echo " 🚀 BẮT ĐẦU CHẠY PIPELINE TỰ ĐỘNG (ALL-IN-ONE)"
echo "=========================================================="

# -----------------------------------------------------------------------------
# BƯỚC 1: KIỂM TRA & TỰ ĐỘNG BẬT CÁC DỊCH VỤ NỀN (HADOOP, ZOOKEEPER, KAFKA)
# -----------------------------------------------------------------------------
echo "[1/7] Kiểm tra các dịch vụ nền tảng..."

# 1.1 Kiểm tra Hadoop (NameNode)
if ! jps | grep -q "NameNode"; then
    echo "  ⚠️ Hadoop chưa chạy -> Đang tự động khởi động Hadoop..."
    "${HADOOP_HOME}/sbin/start-all.sh"
    sleep 5
else
    echo "  ✅ Hadoop đang hoạt động."
fi

# 1.2 Kiểm tra ZooKeeper
if ! jps | grep -q "QuorumPeerMain"; then
    echo "  ⚠️ ZooKeeper chưa chạy -> Đang tự động khởi động ZooKeeper (daemon)..."
    "${KAFKA_DIR}/bin/zookeeper-server-start.sh" -daemon "${KAFKA_DIR}/config/zookeeper.properties"
    sleep 3
else
    echo "  ✅ ZooKeeper đang hoạt động."
fi

# 1.3 Kiểm tra Kafka Broker
if ! jps | grep -q "Kafka"; then
    echo "  ⚠️ Kafka chưa chạy -> Đang tự động khởi động Kafka (daemon)..."
    "${KAFKA_DIR}/bin/kafka-server-start.sh" -daemon "${KAFKA_DIR}/config/server.properties"
    sleep 5
else
    echo "  ✅ Kafka broker đang hoạt động."
fi

# 1.4 Đảm bảo Kafka Topic 'app-logs' tồn tại
echo "[2/7] Kiểm tra Kafka Topic..."
if ! "${KAFKA_DIR}/bin/kafka-topics.sh" --list --bootstrap-server localhost:9092 | grep -q "app-logs"; then
    echo "  Tạo mới topic 'app-logs'..."
    "${KAFKA_DIR}/bin/kafka-topics.sh" --create --topic app-logs --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
else
    echo "  ✅ Topic 'app-logs' đã sẵn sàng."
fi

# -----------------------------------------------------------------------------
# BƯỚC 2: SINH DỮ LIỆU FAKE LOGS (TEST NHANH 2,000 DÒNG)
# -----------------------------------------------------------------------------
echo "[3/7] Sinh dữ liệu log giả lập (2,000 dòng có inject bất thường)..."
python3 data/generate_logs.py

# -----------------------------------------------------------------------------
# BƯỚC 3: DỌN DẸP HDFS & BẬT CONSUMER ĐẨY HDFS
# -----------------------------------------------------------------------------
echo "[4/7] Chuẩn bị HDFS và kích hoạt Kafka Consumer..."
hdfs dfs -rm -r -f /data/logs /data/output || true
hdfs dfs -mkdir -p /data/logs

# Bật Consumer chạy nền
python3 kafka/consumer.py > /tmp/consumer.log 2>&1 &
CONSUMER_PID=$!
sleep 2
echo "  ✅ Consumer đang chạy nền (PID: $CONSUMER_PID)"

# -----------------------------------------------------------------------------
# BƯỚC 4: REPLAY BẮN LOGS VÀO KAFKA
# -----------------------------------------------------------------------------
echo "[5/7] Bắt đầu Replay log vào Kafka (Tốc độ siêu nhanh ~10s)..."
python3 simulator/replay.py

# Đợi 5 giây cho Consumer hoàn tất việc nạp các batch cuối vào HDFS
echo "  Đợi 5 giây cho Consumer ghi hết dữ liệu lên HDFS..."
sleep 5

# Dừng Consumer
kill $CONSUMER_PID || true
echo "  ✅ Đã dừng Consumer."

# -----------------------------------------------------------------------------
# BƯỚC 5: CHẠY HADOOP STREAMING MAPREDUCE
# -----------------------------------------------------------------------------
echo "[6/7] Thực thi Hadoop MapReduce Streaming phân tán..."
bash mapreduce/run_job.sh

# -----------------------------------------------------------------------------
# BƯỚC 6: PHÂN TÍCH BẤT THƯỜNG & XUẤT BÁO CÁO
# -----------------------------------------------------------------------------
echo "[7/7] Phân tích bất thường (Rule-based Anomaly Detection)..."
python3 anomaly/detector.py

echo "=========================================================="
echo "🎉 HOÀN TẤT TOÀN BỘ PIPELINE TỰ ĐỘNG THÀNH CÔNG!"
echo "=========================================================="
