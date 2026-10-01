# =============================================================================
# kafka/consumer.py — Đọc log từ Kafka, gom batch, đẩy lên HDFS
#
# NGUYÊN LÝ:
#   - Consumer subscribe vào topic app-logs
#   - Gom đủ BATCH_SIZE message → ghi ra file local tạm
#   - Dùng subprocess chạy: hdfs dfs -put <local_file> <hdfs_dir>
#   - Xóa file local sau khi đẩy HDFS thành công
#   - Lặp lại cho batch tiếp theo
#
# CHẠY: python3 kafka/consumer.py
# =============================================================================

import json
import os
import sys
import subprocess
import time
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import (
    KAFKA_BROKER, KAFKA_TOPIC,
    HDFS_INPUT_DIR, LOCAL_BATCH_DIR, BATCH_SIZE
)

from kafka import KafkaConsumer


def create_consumer():
    """Khởi tạo Kafka Consumer."""
    print(f"[INFO] Kết nối Kafka broker: {KAFKA_BROKER}, topic: {KAFKA_TOPIC}")
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            # Deserialize bytes → dict
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            # Consumer group — nhiều consumer cùng group chia nhau đọc partition
            group_id="log-processor-group",
            # Đọc từ đầu topic nếu group này chưa có offset
            auto_offset_reset="earliest",
            # Tự commit offset sau khi xử lý
            enable_auto_commit=True,
            auto_commit_interval_ms=1000,
        )
        print(f"[INFO] ✅ Consumer kết nối thành công!")
        return consumer
    except Exception as e:
        print(f"[ERROR] ❌ Không kết nối được Kafka tại {KAFKA_BROKER}: {e}")
        sys.exit(1)


def ensure_hdfs_dir():
    """Tạo thư mục trên HDFS nếu chưa có."""
    result = subprocess.run(
        ["hdfs", "dfs", "-mkdir", "-p", HDFS_INPUT_DIR],
        capture_output=True, text=True
    )
    if result.returncode == 0:
        print(f"[INFO] HDFS dir sẵn sàng: {HDFS_INPUT_DIR}")
    else:
        # Có thể đã tồn tại → không phải lỗi
        print(f"[INFO] HDFS dir: {HDFS_INPUT_DIR} (có thể đã tồn tại)")


def write_batch_local(batch, batch_id):
    """
    Ghi batch log ra file local tạm.
    Format: mỗi dòng là 1 JSON string (plain text để Hadoop đọc được)
    """
    os.makedirs(LOCAL_BATCH_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%H%M%S")
    filename = f"batch_{batch_id:04d}_{timestamp}.txt"
    local_path = os.path.join(LOCAL_BATCH_DIR, filename)

    with open(local_path, "w") as f:
        for record in batch:
            f.write(json.dumps(record) + "\n")

    return local_path, filename


def push_to_hdfs(local_path, filename):
    """
    Đẩy file local lên HDFS bằng subprocess.
    Tương đương: hdfs dfs -put <local_path> <HDFS_INPUT_DIR>/<filename>
    """
    hdfs_path = f"{HDFS_INPUT_DIR}/{filename}"
    result = subprocess.run(
        ["hdfs", "dfs", "-put", local_path, hdfs_path],
        capture_output=True, text=True
    )
    if result.returncode == 0:
        return True, hdfs_path
    else:
        print(f"[ERROR] HDFS put thất bại: {result.stderr}")
        return False, None


def consume_and_batch(consumer):
    """
    Vòng lặp chính: đọc từ Kafka, gom batch, đẩy HDFS.
    """
    print(f"\n[INFO] Bắt đầu consume. Batch size = {BATCH_SIZE} messages")
    print(f"       Dữ liệu sẽ đẩy lên HDFS: {HDFS_INPUT_DIR}")
    print(f"       Nhấn Ctrl+C để dừng\n")

    ensure_hdfs_dir()

    batch = []
    batch_id = 0
    total_consumed = 0

    for message in consumer:
        record = message.value
        batch.append(record)
        total_consumed += 1

        # Đủ batch → ghi file + đẩy HDFS
        if len(batch) >= BATCH_SIZE:
            batch_id += 1
            local_path, filename = write_batch_local(batch, batch_id)
            success, hdfs_path = push_to_hdfs(local_path, filename)

            if success:
                os.remove(local_path)  # Xóa file tạm local
                print(f"  [Batch {batch_id:04d}] {len(batch)} msgs → HDFS: {hdfs_path} ✅")
            else:
                print(f"  [Batch {batch_id:04d}] HDFS thất bại, giữ lại: {local_path} ❌")

            batch = []  # Reset batch

        # In tổng mỗi 5000 messages
        if total_consumed % 5000 == 0:
            print(f"  [TOTAL] Đã consume {total_consumed:,} messages")


if __name__ == "__main__":
    consumer = create_consumer()
    try:
        consume_and_batch(consumer)
    except KeyboardInterrupt:
        print(f"\n[INFO] Dừng consumer. Tổng đã consume: nhiều messages.")
    finally:
        consumer.close()
        print("[INFO] Đã đóng Kafka Consumer")
