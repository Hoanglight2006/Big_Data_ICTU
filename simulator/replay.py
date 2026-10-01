# =============================================================================
# simulator/replay.py — Đọc fake log và gửi vào Kafka theo đúng nhịp thời gian
#
# NGUYÊN LÝ TIME SCALING:
#   - Đọc toàn bộ log, đã sort theo timestamp
#   - Tính khoảng cách thời gian giữa 2 log liên tiếp: delta_t
#   - Chia delta_t cho SCALE_FACTOR → thời gian sleep thực tế
#   - sleep() rồi gửi log tiếp theo vào Kafka
#
#   Ví dụ với SCALE_FACTOR=12:
#     Log A lúc 08:00:00, Log B lúc 08:05:00
#     delta_t = 300s → sleep(300/12) = sleep(25s) → gửi B
#
# CHẠY: python3 simulator/replay.py
# =============================================================================

import json
import time
import sys
import os
from datetime import datetime, timezone

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import (
    KAFKA_BROKER, KAFKA_TOPIC,
    SCALE_FACTOR, MAX_SLEEP_SECONDS, OUTPUT_FILE
)

from kafka import KafkaProducer


def create_producer():
    """Khởi tạo Kafka Producer với retry."""
    print(f"[INFO] Kết nối Kafka broker: {KAFKA_BROKER}")
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            # Serialize dict → JSON bytes trước khi gửi
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            # Key serialize (dùng ip làm key → cùng IP vào cùng partition)
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            # Đợi tất cả replica xác nhận (an toàn hơn)
            acks="all",
            # Retry nếu gửi thất bại
            retries=3,
        )
        print(f"[INFO] ✅ Kết nối Kafka thành công!")
        return producer
    except Exception as e:
        print(f"[ERROR] ❌ Không kết nối được Kafka tại {KAFKA_BROKER}: {e}")
        print(f"         Kiểm tra: ss -tlnp | grep 9092")
        sys.exit(1)


def load_logs(filepath):
    """Đọc toàn bộ log từ file JSON (mỗi dòng 1 JSON object)."""
    if not os.path.exists(filepath):
        print(f"[ERROR] Không tìm thấy file: {filepath}")
        print(f"        Chạy trước: python3 data/generate_logs.py")
        sys.exit(1)

    logs = []
    with open(filepath, "r") as f:
        for line in f:
            line = line.strip()
            if line:
                logs.append(json.loads(line))

    print(f"[INFO] Đã load {len(logs):,} logs từ {filepath}")
    return logs


def parse_timestamp(ts_str):
    """Parse ISO 8601 timestamp string → datetime object."""
    return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))


def replay(producer, logs):
    """
    Phát lại log vào Kafka theo đúng nhịp thời gian đã scale.
    """
    total = len(logs)
    print(f"\n[INFO] Bắt đầu replay {total:,} logs với scale_factor={SCALE_FACTOR}x")
    print(f"       24h dữ liệu → {24/SCALE_FACTOR*60:.0f} phút thực tế")
    print(f"       Nhấn Ctrl+C để dừng\n")

    sent = 0
    start_real_time = time.time()

    for i, log in enumerate(logs):
        # Gửi log vào Kafka
        # Key = IP → cùng IP sẽ vào cùng partition (dễ debug)
        producer.send(
            topic=KAFKA_TOPIC,
            key=log.get("ip"),
            value=log
        )
        sent += 1

        # In progress mỗi 1000 message
        if sent % 1000 == 0:
            elapsed = time.time() - start_real_time
            print(f"  [{sent:>6}/{total}] Sent | log_time={log['timestamp']} | "
                  f"elapsed={elapsed:.0f}s | "
                  f"rate={sent/elapsed:.0f} msg/s")

        # Tính thời gian sleep đến log tiếp theo
        if i < total - 1:
            current_ts = parse_timestamp(log["timestamp"])
            next_ts    = parse_timestamp(logs[i + 1]["timestamp"])

            delta_original = (next_ts - current_ts).total_seconds()

            # Scale down khoảng cách thời gian
            sleep_time = delta_original / SCALE_FACTOR

            # Giới hạn sleep tối đa để không chờ quá lâu
            sleep_time = min(sleep_time, MAX_SLEEP_SECONDS)

            # Chỉ sleep nếu delta dương (log đã sort đúng)
            if sleep_time > 0:
                time.sleep(sleep_time)

    # Flush toàn bộ message còn trong buffer
    producer.flush()

    elapsed_total = time.time() - start_real_time
    print(f"\n✅ Replay hoàn thành!")
    print(f"   Tổng sent : {sent:,} messages")
    print(f"   Thời gian : {elapsed_total:.1f}s ({elapsed_total/60:.1f} phút)")
    print(f"   Tốc độ TB : {sent/elapsed_total:.0f} msg/s")


if __name__ == "__main__":
    producer = create_producer()
    logs = load_logs(OUTPUT_FILE)

    try:
        replay(producer, logs)
    except KeyboardInterrupt:
        print("\n[INFO] Dừng replay theo yêu cầu (Ctrl+C)")
    finally:
        producer.close()
        print("[INFO] Đã đóng Kafka Producer")
