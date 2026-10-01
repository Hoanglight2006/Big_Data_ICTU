# =============================================================================
# data/generate_logs.py — Tạo fake log giả lập 24 giờ
#
# NGUYÊN LÝ:
#   1. Chia 24h thành các khung giờ với mật độ traffic khác nhau
#      (đêm thấp, sáng/trưa/chiều cao hơn)
#   2. Phân phối TOTAL_LOGS log vào các khung giờ theo tỉ lệ traffic
#   3. Trong mỗi khung giờ, random timestamp đều
#   4. Inject anomaly vào khung giờ ANOMALY_START_HOUR → ANOMALY_END_HOUR:
#      - Tăng tỉ lệ ERROR và 5xx lên cao
#      - Một IP cụ thể gửi rất nhiều request
#   5. Sort toàn bộ log theo timestamp
#   6. Ghi ra file JSON (mỗi dòng = 1 JSON object)
#
# OUTPUT: data/fake_logs.json
# =============================================================================

import json
import random
import sys
import os
from datetime import datetime, timezone, timedelta

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

# Thêm thư mục gốc vào Python path để import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import (
    LOG_DATE, TOTAL_LOGS, OUTPUT_FILE,
    SERVICES, ENDPOINTS, HTTP_METHODS, STATUS_CODES,
    ANOMALY_START_HOUR, ANOMALY_END_HOUR, ANOMALY_IP
)


# =============================================================================
# BƯỚC 1: Định nghĩa phân phối traffic theo giờ
# =============================================================================
# traffic_weights[h] = tỉ lệ tương đối của traffic trong giờ h (0-23)
# Tổng không cần = 1, chỉ cần tỉ lệ tương đối
TRAFFIC_WEIGHTS = {
     0: 0.5,   1: 0.3,   2: 0.2,   3: 0.2,   4: 0.2,   5: 0.3,
     6: 0.8,   7: 1.5,   8: 2.5,   9: 3.5,  10: 4.0,  11: 4.5,
    12: 5.0,  13: 4.5,  14: 4.0,  15: 3.5,  16: 4.0,  17: 4.5,
    18: 5.0,  19: 4.5,  20: 3.5,  21: 2.5,  22: 1.5,  23: 1.0,
}

def get_logs_per_hour(total_logs):
    """Phân phối total_logs vào 24 giờ theo traffic_weights."""
    total_weight = sum(TRAFFIC_WEIGHTS.values())
    logs_per_hour = {}
    allocated = 0
    hours = list(range(24))
    for h in hours[:-1]:
        count = int(total_logs * TRAFFIC_WEIGHTS[h] / total_weight)
        logs_per_hour[h] = count
        allocated += count
    logs_per_hour[23] = total_logs - allocated  # phần còn lại vào giờ cuối
    return logs_per_hour


# =============================================================================
# BƯỚC 2: Hàm tạo 1 log entry bình thường
# =============================================================================
def make_normal_log(hour, minute, second, ip_pool):
    """Tạo 1 log entry ngẫu nhiên trong giờ 'hour'."""
    # Tạo timestamp ISO 8601 UTC
    dt = datetime(
        *[int(x) for x in LOG_DATE.split("-")],
        hour, minute, second,
        tzinfo=timezone.utc
    )
    timestamp = dt.isoformat()

    # Chọn status code theo xác suất
    status_code = random.choices(
        list(STATUS_CODES.keys()),
        weights=list(STATUS_CODES.values()),
        k=1
    )[0]

    # Level log dựa trên status code
    if status_code >= 500:
        level = "ERROR"
    elif status_code >= 400:
        level = "WARN"
    else:
        level = random.choices(["INFO", "DEBUG"], weights=[0.9, 0.1], k=1)[0]

    # Response time: lỗi thường chậm hơn
    if status_code >= 500:
        response_time = random.randint(500, 5000)
    elif status_code >= 400:
        response_time = random.randint(100, 800)
    else:
        response_time = random.randint(10, 300)

    endpoint = random.choice(ENDPOINTS)
    method = random.choice(HTTP_METHODS)

    return {
        "timestamp": timestamp,
        "level": level,
        "service": random.choice(SERVICES),
        "ip": random.choice(ip_pool),
        "method": method,
        "endpoint": endpoint,
        "status_code": status_code,
        "response_time_ms": response_time,
        "message": f"{method} {endpoint} {status_code}"
    }


# =============================================================================
# BƯỚC 3: Hàm tạo 1 log entry bất thường (anomaly)
# =============================================================================
def make_anomaly_log(hour, minute, second, is_flood_ip=False):
    """Tạo 1 log bất thường: nhiều ERROR, 5xx, và IP flood."""
    dt = datetime(
        *[int(x) for x in LOG_DATE.split("-")],
        hour, minute, second,
        tzinfo=timezone.utc
    )
    timestamp = dt.isoformat()

    # Anomaly: tỉ lệ lỗi cao hơn nhiều
    status_code = random.choices(
        [200, 500, 503, 429],
        weights=[0.3, 0.4, 0.2, 0.1],
        k=1
    )[0]

    level = "ERROR" if status_code >= 500 else "WARN"
    response_time = random.randint(2000, 10000)  # rất chậm

    # IP flood hoặc IP ngẫu nhiên
    ip = ANOMALY_IP if is_flood_ip else f"192.168.{random.randint(1,10)}.{random.randint(1,254)}"

    endpoint = random.choice(ENDPOINTS)
    method = random.choice(["GET", "POST"])

    return {
        "timestamp": timestamp,
        "level": level,
        "service": random.choice(SERVICES),
        "ip": ip,
        "method": method,
        "endpoint": endpoint,
        "status_code": status_code,
        "response_time_ms": response_time,
        "message": f"[ANOMALY] {method} {endpoint} {status_code}"
    }


# =============================================================================
# BƯỚC 4: Main — tạo toàn bộ log
# =============================================================================
def generate_logs():
    print(f"[INFO] Bắt đầu tạo {TOTAL_LOGS:,} log entries cho ngày {LOG_DATE}...")

    # Pool IP bình thường (50 IP giả)
    ip_pool = [f"203.{random.randint(100,200)}.{random.randint(0,255)}.{random.randint(1,254)}"
               for _ in range(50)]

    logs_per_hour = get_logs_per_hour(TOTAL_LOGS)
    all_logs = []

    for hour in range(24):
        count = logs_per_hour[hour]
        is_anomaly_hour = ANOMALY_START_HOUR <= hour < ANOMALY_END_HOUR

        if is_anomaly_hour:
            # Tăng gấp đôi log trong giờ anomaly để mô phỏng spike
            count = count * 2
            print(f"  [ANOMALY] Giờ {hour:02d}:00 → inject {count:,} logs (spike!)")
        else:
            print(f"  Giờ {hour:02d}:00 → tạo {count:,} logs")

        for _ in range(count):
            minute = random.randint(0, 59)
            second = random.randint(0, 59)

            if is_anomaly_hour:
                # 70% log bất thường, 30% bình thường để trông tự nhiên
                if random.random() < 0.7:
                    # 40% trong anomaly là từ IP flood
                    is_flood = random.random() < 0.4
                    log = make_anomaly_log(hour, minute, second, is_flood_ip=is_flood)
                else:
                    log = make_normal_log(hour, minute, second, ip_pool)
            else:
                log = make_normal_log(hour, minute, second, ip_pool)

            all_logs.append(log)

    # Sort theo timestamp
    print(f"\n[INFO] Sort {len(all_logs):,} logs theo timestamp...")
    all_logs.sort(key=lambda x: x["timestamp"])

    # Ghi ra file
    os.makedirs(os.path.dirname(OUTPUT_FILE) if os.path.dirname(OUTPUT_FILE) else ".", exist_ok=True)
    print(f"[INFO] Ghi vào {OUTPUT_FILE}...")
    with open(OUTPUT_FILE, "w") as f:
        for log in all_logs:
            f.write(json.dumps(log) + "\n")

    # Đồng thời ghi vào data/fake_logs.json để tương thích
    try:
        from config.settings import LEGACY_OUTPUT_FILE
        with open(LEGACY_OUTPUT_FILE, "w") as f_legacy:
            for log in all_logs:
                f_legacy.write(json.dumps(log) + "\n")
    except Exception:
        pass

    print(f"\n✅ Hoàn thành! Đã tạo {len(all_logs):,} logs → {OUTPUT_FILE}")
    print(f"   Anomaly được inject vào giờ {ANOMALY_START_HOUR:02d}:00 – {ANOMALY_END_HOUR:02d}:00")
    print(f"   Flood IP: {ANOMALY_IP}")

    # Thống kê nhanh
    error_count = sum(1 for l in all_logs if l["level"] == "ERROR")
    status_5xx  = sum(1 for l in all_logs if l["status_code"] >= 500)
    print(f"\n📊 Thống kê:")
    print(f"   Total logs   : {len(all_logs):,}")
    print(f"   ERROR level  : {error_count:,} ({error_count/len(all_logs)*100:.1f}%)")
    print(f"   HTTP 5xx     : {status_5xx:,}  ({status_5xx/len(all_logs)*100:.1f}%)")


def generate_continuous_logs():
    """
    Chế độ sinh log liên tục (Continuous Generation) — Nhiệm vụ của Đặng Văn Vinh
    Mô phỏng máy chủ Web Server thực tế nhả log liên tục theo thời gian thực vào data/spool/web_access_live.log
    """
    import time
    continuous_file = os.path.join(SPOOL_DIR, f"web_access_live_{LOG_DATE}.log")
    os.makedirs(SPOOL_DIR, exist_ok=True)
    print("================================================================================")
    print(" 🌐 CONTINUOUS WEB SERVER LOG GENERATOR (ĐẶNG VĂN VINH)")
    print(f" Đang ghi log thời gian thực vào: {continuous_file}")
    print(" Nhấn Ctrl + C để dừng bất kỳ lúc nào...")
    print("================================================================================")
    ip_pool = [generate_ip() for _ in range(50)]
    counter = 0
    try:
        with open(continuous_file, "a", encoding="utf-8") as f:
            while True:
                now = datetime.now()
                hour = now.hour
                minute = now.minute
                second = now.second
                log = make_normal_log(hour, minute, second, ip_pool)
                log["timestamp"] = now.strftime("%Y-%m-%dT%H:%M:%SZ")
                line = json.dumps(log)
                f.write(line + "\n")
                f.flush()
                counter += 1
                if counter % 5 == 0:
                    status_emoji = "✅" if log["status_code"] < 400 else "⚠️"
                    print(f"[{log['timestamp']}] {status_emoji} {log['method']} {log['endpoint']:<25} | Status: {log['status_code']} | {log['response_time_ms']}ms | IP: {log['ip']}")
                time.sleep(random.uniform(0.05, 0.2))
    except KeyboardInterrupt:
        print(f"\n🛑 Đã dừng sinh log liên tục! Tổng cộng đã ghi {counter:,} logs vào {continuous_file}.")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] in ["--continuous", "-c", "--stream"]:
        generate_continuous_logs()
    else:
        generate_logs()

