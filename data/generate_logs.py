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
    LOG_DATE, TOTAL_LOGS, OUTPUT_FILE, SPOOL_DIR,
    SERVICES, ENDPOINTS, HTTP_METHODS, STATUS_CODES,
    ANOMALY_START_HOUR, ANOMALY_END_HOUR, ANOMALY_IP
)


# =============================================================================
# BƯỚC 1: Định nghĩa phân phối traffic theo giờ
# =============================================================================
# traffic_weights[h] = tỉ lệ tương đối của traffic trong giờ h (0-23)
# =============================================================================
# BƯỚC 1: Định nghĩa phân phối traffic theo giờ có độ nhiễu ngẫu nhiên
# =============================================================================
BASE_TRAFFIC_WEIGHTS = {
     0: 0.5,   1: 0.3,   2: 0.2,   3: 0.2,   4: 0.2,   5: 0.3,
     6: 0.8,   7: 1.5,   8: 2.5,   9: 3.5,  10: 4.0,  11: 4.5,
    12: 5.0,  13: 4.5,  14: 4.0,  15: 4.5,  16: 4.0,  17: 4.5,
    18: 5.0,  19: 4.5,  20: 3.5,  21: 2.5,  22: 1.5,  23: 1.0,
}

def get_logs_per_hour(total_logs):
    """Phân phối total_logs vào 24 giờ với dao động ngẫu nhiên tự nhiên."""
    # Thêm nhiễu ngẫu nhiên ±15% cho từng giờ để mỗi ngày có phân phối khác nhau
    dynamic_weights = {
        h: w * random.uniform(0.85, 1.20)
        for h, w in BASE_TRAFFIC_WEIGHTS.items()
    }
    total_weight = sum(dynamic_weights.values())
    logs_per_hour = {}
    allocated = 0
    for h in range(23):
        count = int(total_logs * dynamic_weights[h] / total_weight)
        logs_per_hour[h] = count
        allocated += count
    logs_per_hour[23] = max(0, total_logs - allocated)
    return logs_per_hour


# =============================================================================
# BƯỚC 2: Hàm tạo 1 log entry bình thường
# =============================================================================
def make_normal_log(hour, minute, second, ip_pool):
    """Tạo 1 log entry ngẫu nhiên trong giờ 'hour'."""
    dt = datetime(
        *[int(x) for x in LOG_DATE.split("-")],
        hour, minute, second,
        tzinfo=timezone.utc
    )
    timestamp = dt.isoformat()

    status_code = random.choices(
        list(STATUS_CODES.keys()),
        weights=list(STATUS_CODES.values()),
        k=1
    )[0]

    if status_code >= 500:
        level = "ERROR"
    elif status_code >= 400:
        level = "WARN"
    else:
        level = random.choices(["INFO", "DEBUG"], weights=[0.9, 0.1], k=1)[0]

    if status_code >= 500:
        response_time = random.randint(400, 3000)
    elif status_code >= 400:
        response_time = random.randint(100, 800)
    else:
        response_time = random.randint(10, 350)

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
def make_anomaly_log(hour, minute, second, flood_ip=None):
    """Tạo 1 log bất thường: lỗi hệ thống, thời gian xử lý chậm, hoặc IP spam."""
    dt = datetime(
        *[int(x) for x in LOG_DATE.split("-")],
        hour, minute, second,
        tzinfo=timezone.utc
    )
    timestamp = dt.isoformat()

    status_code = random.choices(
        [200, 500, 503, 429, 404],
        weights=[0.25, 0.40, 0.20, 0.10, 0.05],
        k=1
    )[0]

    level = "ERROR" if status_code >= 500 else "WARN"
    response_time = random.randint(2500, 9500)

    ip = flood_ip if flood_ip else f"192.168.{random.randint(1,10)}.{random.randint(1,254)}"
    endpoint = random.choice(["/api/products/search", "/api/orders/create", "/api/payments/process", "/health"])
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
# BƯỚC 4: Hàm tạo bản ghi dữ liệu rác/hỏng (Corrupted Record Injection)
# =============================================================================
def make_corrupted_raw_line(hour, minute, second, ip_pool):
    """Tạo 1 dòng dữ liệu rác/hỏng để kiểm thử tầng tiền xử lý cleaner_mapper.py."""
    corrupt_type = random.choice(["malformed_json", "missing_field", "invalid_ip", "bad_datatype"])
    dt = datetime(*[int(x) for x in LOG_DATE.split("-")], hour, minute, second, tzinfo=timezone.utc)
    ts = dt.isoformat()

    if corrupt_type == "malformed_json":
        # JSON bị cắt cụt, vỡ cú pháp
        return f'{{"timestamp": "{ts}", "ip": "{random.choice(ip_pool)}", "service": "api-gateway", "endpoint": "/api/products'
    elif corrupt_type == "missing_field":
        # Thiếu trường bắt buộc (thiếu status_code hoặc endpoint)
        rec = {"timestamp": ts, "ip": random.choice(ip_pool), "level": "INFO", "response_time_ms": 120}
        return json.dumps(rec)
    elif corrupt_type == "invalid_ip":
        # IP chứa ký tự lạ hoặc vượt dải 255
        bad_ip = random.choice(["999.999.999.999", "192.168.1.abc", "not_an_ip", "300.1.2.3"])
        rec = make_normal_log(hour, minute, second, [bad_ip])
        return json.dumps(rec)
    else:
        # Kiểu dữ liệu sai: status_code là chuỗi chữ
        rec = make_normal_log(hour, minute, second, ip_pool)
        rec["status_code"] = "SERVER_ERROR"
        return json.dumps(rec)


# =============================================================================
# BƯỚC 5: Main — Tạo toàn bộ tập dữ liệu lớn
# =============================================================================
def generate_logs():
    # 1. Tự động xác định số lượng log (tăng cường quy mô từ 150.000 đến 180.000)
    actual_total_logs = TOTAL_LOGS if TOTAL_LOGS != 50000 else random.randint(150000, 180000)
    print(f"[INFO] Bắt đầu sinh tập dữ liệu lớn: {actual_total_logs:,} bản ghi cho ngày {LOG_DATE}...")

    # 2. Ngẫu nhiên hóa khung giờ bất thường nếu không bị ép buộc cấu hình
    if ANOMALY_START_HOUR >= 0 and ANOMALY_END_HOUR > ANOMALY_START_HOUR:
        anomaly_hours = list(range(ANOMALY_START_HOUR, ANOMALY_END_HOUR))
    else:
        # Ngẫu nhiên chọn 1 đến 2 khung giờ trong khoảng từ 9h đến 21h
        candidate_hours = [9, 10, 11, 14, 15, 16, 17, 19, 20, 21]
        num_anomaly_peaks = random.choice([1, 2])
        anomaly_hours = sorted(random.sample(candidate_hours, k=num_anomaly_peaks))

    # 3. Ngẫu nhiên hóa địa chỉ IP tấn công (Flood IPs)
    if ANOMALY_IP:
        flood_ips = [ANOMALY_IP]
    else:
        flood_ips = [f"10.0.{random.randint(0, 5)}.{random.randint(20, 220)}" for _ in range(random.choice([1, 2]))]

    print(f"   [CẤU HÌNH NGẪU NHIÊN] Khung giờ bất thường : {[f'{h:02d}:00' for h in anomaly_hours]}")
    print(f"   [CẤU HÌNH NGẪU NHIÊN] IP gửi yêu cầu quá tải: {flood_ips}")

    # Pool IP người dùng đa dạng (300 IP thuộc các dải mạng thực tế)
    ip_pool = []
    subnets = ["14.161", "27.72", "113.160", "118.69", "123.30", "171.244", "203.162"]
    for _ in range(300):
        subnet = random.choice(subnets)
        ip_pool.append(f"{subnet}.{random.randint(1, 254)}.{random.randint(1, 254)}")

    logs_per_hour = get_logs_per_hour(actual_total_logs)
    all_raw_lines = []
    corrupted_count = 0

    for hour in range(24):
        base_count = logs_per_hour[hour]
        is_anomaly_hour = hour in anomaly_hours

        if is_anomaly_hour:
            # Tăng mạnh số lượng log trong giờ bất thường (tăng thêm 60% - 100%)
            spike_multiplier = random.uniform(1.6, 2.2)
            count = int(base_count * spike_multiplier)
            print(f"  [BẤT THƯỜNG] Giờ {hour:02d}:00 -> Phát sinh lưu lượng tăng vọt ({count:,} log)")
        else:
            count = base_count
            print(f"  Giờ {hour:02d}:00 -> Tạo {count:,} log bình thường")

        for _ in range(count):
            minute = random.randint(0, 59)
            second = random.randint(0, 59)

            # Chèn 1% dữ liệu rác/hỏng ngẫu nhiên vào để kiểm thử tầng làm sạch YARN
            if random.random() < 0.01:
                corrupted_line = make_corrupted_raw_line(hour, minute, second, ip_pool)
                all_raw_lines.append((hour, minute, second, corrupted_line))
                corrupted_count += 1
                continue

            if is_anomaly_hour:
                if random.random() < 0.65:
                    # Chọn ngẫu nhiên IP flood trong danh sách
                    chosen_flood_ip = random.choice(flood_ips) if random.random() < 0.50 else None
                    log_obj = make_anomaly_log(hour, minute, second, flood_ip=chosen_flood_ip)
                else:
                    log_obj = make_normal_log(hour, minute, second, ip_pool)
            else:
                log_obj = make_normal_log(hour, minute, second, ip_pool)

            all_raw_lines.append((hour, minute, second, json.dumps(log_obj)))

    # Sắp xếp các dòng log theo trình tự thời gian trong ngày
    print(f"\n[INFO] Sắp xếp {len(all_raw_lines):,} bản ghi theo mốc thời gian...")
    all_raw_lines.sort(key=lambda x: (x[0], x[1], x[2]))

    # Ghi ra file
    os.makedirs(os.path.dirname(OUTPUT_FILE) if os.path.dirname(OUTPUT_FILE) else ".", exist_ok=True)
    print(f"[INFO] Ghi dữ liệu vào {OUTPUT_FILE}...")
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        for item in all_raw_lines:
            f.write(item[3] + "\n")

    # Đồng thời ghi vào data/fake_logs.json để tương thích
    try:
        from config.settings import LEGACY_OUTPUT_FILE
        with open(LEGACY_OUTPUT_FILE, "w", encoding="utf-8") as f_legacy:
            for item in all_raw_lines:
                f_legacy.write(item[3] + "\n")
    except Exception:
        pass

    print(f"\n================================================================================")
    print(f" [THÀNH CÔNG] ĐÃ TẠO XONG TẬP DỮ LIỆU LỚN")
    print(f" Tổng số bản ghi sinh ra : {len(all_raw_lines):,} dòng")
    print(f" Bản ghi rác/hỏng chèn vào: {corrupted_count:,} dòng (để kiểm thử tầng Tiền xử lý)")
    print(f" Khung giờ bất thường    : {[f'{h:02d}:00' for h in anomaly_hours]}")
    print(f" IP gửi yêu cầu quá tải  : {flood_ips}")
    print(f" File lưu trữ            : {OUTPUT_FILE}")
    print(f"================================================================================")


def generate_continuous_logs():
    """Chế độ sinh log liên tục theo thời gian thực."""
    import time
    continuous_file = os.path.join(SPOOL_DIR, f"web_access_live_{LOG_DATE}.log")
    os.makedirs(SPOOL_DIR, exist_ok=True)
    print("================================================================================")
    print(" CONTINUOUS WEB SERVER LOG GENERATOR")
    print(f" Ghi log thời gian thực vào: {continuous_file}")
    print(" Nhấn Ctrl + C để dừng...")
    print("================================================================================")
    ip_pool = [f"14.161.{random.randint(1,254)}.{random.randint(1,254)}" for _ in range(50)]
    counter = 0
    try:
        with open(continuous_file, "a", encoding="utf-8") as f:
            while True:
                now = datetime.now()
                log = make_normal_log(now.hour, now.minute, now.second, ip_pool)
                log["timestamp"] = now.strftime("%Y-%m-%dT%H:%M:%SZ")
                f.write(json.dumps(log) + "\n")
                f.flush()
                counter += 1
                if counter % 10 == 0:
                    print(f"[{log['timestamp']}] {log['method']} {log['endpoint']:<25} | Status: {log['status_code']} | {log['response_time_ms']}ms | IP: {log['ip']}")
                time.sleep(random.uniform(0.02, 0.1))
    except KeyboardInterrupt:
        print(f"\n[INFO] Đã dừng sinh log liên tục. Tổng cộng: {counter:,} logs.")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] in ["--continuous", "-c", "--stream"]:
        generate_continuous_logs()
    else:
        generate_logs()

