import sys
import json
import re
from datetime import datetime

# Biểu thức chính quy kiểm tra IPv4
IPV4_REGEX = re.compile(r"^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$")
REQUIRED_FIELDS = ["timestamp", "ip", "status_code", "endpoint", "response_time_ms", "level"]

def is_valid_ipv4(ip_str):
    if not ip_str or not IPV4_REGEX.match(ip_str):
        return False
    parts = ip_str.split(".")
    return all(0 <= int(part) <= 255 for part in parts)

def clean_record(line):
    line = line.strip()
    if not line:
        return None
    try:
        record = json.loads(line)
    except Exception:
        # Tăng Hadoop counter đếm bản ghi JSON hỏng
        sys.stderr.write("reporter:counter:DataQuality,Corrupted_JSON,1\n")
        return None

    # Kiểm tra trường bắt buộc
    for field in REQUIRED_FIELDS:
        if field not in record or record[field] is None:
            sys.stderr.write(f"reporter:counter:DataQuality,MissingField_{field},1\n")
            return None

    # Kiểm tra IPv4
    ip = str(record["ip"]).strip()
    if not is_valid_ipv4(ip):
        sys.stderr.write("reporter:counter:DataQuality,Invalid_IPv4,1\n")
        return None

    # Kiểm tra status code (phải thuộc khoảng mã chuẩn HTTP 100-599)
    try:
        status_code = int(record["status_code"])
        if not (100 <= status_code <= 599):
            raise ValueError("Status code out of HTTP range")
    except (ValueError, TypeError):
        sys.stderr.write("reporter:counter:DataQuality,Invalid_StatusCode,1\n")
        return None

    # Kiểm tra response time (phải là số không âm)
    try:
        resp_time = float(record["response_time_ms"])
        if resp_time < 0:
            raise ValueError("Negative response time")
    except (ValueError, TypeError):
        sys.stderr.write("reporter:counter:DataQuality,Invalid_ResponseTime,1\n")
        return None

    # Kiểm tra và chuẩn hóa cấu trúc timestamp (phải đúng định dạng ISO 8601 hợp lệ)
    raw_ts = str(record["timestamp"]).strip()
    try:
        clean_ts = raw_ts.replace("Z", "+00:00")
        parsed_dt = datetime.fromisoformat(clean_ts)
        formatted_ts = parsed_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        sys.stderr.write("reporter:counter:DataQuality,Invalid_Timestamp,1\n")
        return None

    # Chuẩn hóa dữ liệu đạt chuẩn High-Quality
    clean_data = {
        "timestamp": formatted_ts,
        "service": str(record.get("service", "unknown")).strip().lower(),
        "ip": ip,
        "method": str(record.get("method", "GET")).strip().upper(),
        "endpoint": str(record["endpoint"]).strip(),
        "status_code": status_code,
        "response_time_ms": round(resp_time, 2),
        "level": str(record["level"]).strip().upper()
    }
    return json.dumps(clean_data)

def main():
    for line in sys.stdin:
        cleaned = clean_record(line)
        if cleaned:
            sys.stdout.write(cleaned + "\n")

if __name__ == "__main__":
    main()
