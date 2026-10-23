import sys
import json

def parse_hour(timestamp_str):
    """
    Trích xuất giờ từ ISO 8601 timestamp: 2024-01-15T15:23:45Z -> '15'
    Trả về None nếu định dạng không hợp lệ để tránh tạo giờ '00' giả.
    """
    try:
        if not timestamp_str or "T" not in timestamp_str:
            return None
        time_part = timestamp_str.split("T")[1]
        hour_str = time_part.split(":")[0]
        hour_int = int(hour_str)
        if 0 <= hour_int <= 23:
            return f"{hour_int:02d}"
        return None
    except Exception:
        return None

def main():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        try:
            log = json.loads(line)
        except Exception:
            # Bỏ qua dòng lỗi format nếu có
            continue

        timestamp = log.get("timestamp", "")
        hour = parse_hour(timestamp)
        if hour is None:
            # Bỏ qua bản ghi có timestamp không hợp lệ để tránh sai lệch dữ liệu giờ
            continue

        ip = log.get("ip", "unknown")
        try:
            status_code = int(log.get("status_code", 200))
        except (ValueError, TypeError):
            continue

        # Đồng nhất tên trường 'level' với fallback 'log_level'
        raw_level = log.get("level") or log.get("log_level", "INFO")
        log_level = str(raw_level).strip().upper()

        try:
            response_time = float(log.get("response_time_ms", 0))
        except (ValueError, TypeError):
            response_time = 0.0

        endpoint = log.get("endpoint", "")

        # 1. Đếm tổng số request theo giờ -> Key: hour_req:<hour>
        print(f"hour_req:{hour}\t1")

        # 2. Đếm số ERROR log theo giờ -> Key: hour_err:<hour>
        if log_level == "ERROR":
            print(f"hour_err:{hour}\t1")

        # 3. Đếm số HTTP 5xx theo giờ -> Key: hour_5xx:<hour>
        if status_code >= 500:
            print(f"hour_5xx:{hour}\t1")

        # 4. Gom response time theo giờ để tính TB -> Key: hour_resp:<hour> (value: response_time)
        print(f"hour_resp:{hour}\t{response_time}")

        # 5. Đếm request theo từng IP trong từng giờ -> Key: ip_req:<hour>:<ip>
        print(f"ip_req:{hour}:{ip}\t1")

        # 6. Đếm lỗi theo endpoint -> Key: endpoint_err:<endpoint>
        if status_code >= 400:
            print(f"endpoint_err:{endpoint}\t1")

if __name__ == "__main__":
    main()
