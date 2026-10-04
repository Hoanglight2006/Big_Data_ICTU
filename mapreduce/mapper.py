import sys
import json

def parse_hour(timestamp_str):
    """
    Trích xuất giờ từ ISO 8601 timestamp: 2024-01-15T15:23:45Z -> '15'
    """
    try:
        # Cắt chuỗi nhanh không cần thư viện datetime để tối ưu tốc độ Mapper
        time_part = timestamp_str.split("T")[1]
        return time_part.split(":")[0]
    except Exception:
        return "00"

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
        ip = log.get("ip", "unknown")
        status_code = int(log.get("status_code", 200))
        log_level = log.get("log_level", "INFO")
        response_time = float(log.get("response_time_ms", 0))
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
