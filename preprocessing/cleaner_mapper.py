#!/usr/bin/env python3
# =============================================================================
# preprocessing/cleaner_mapper.py — Hadoop Streaming Mapper for Data Cleaning
#
# Chương trình: Samsung Innovation Campus (SIC) - Big Data Course
# Nhóm: Group 3
# Thành viên phụ trách: Nông Minh Trí & Triệu Văn Huy
#
# Chức năng:
#   - Nhận log thô từng dòng từ HDFS qua sys.stdin trên các worker nodes của cụm YARN.
#   - Lọc bỏ dòng lỗi cú pháp JSON, thiếu trường, sai định dạng IPv4.
#   - Chuẩn hóa kiểu dữ liệu nghiêm ngặt.
#   - Đẩy bản ghi sạch ra sys.stdout trực tiếp vào HDFS (/data/cleaned/<LOG_DATE>).
#   - KHÔNG tải file trung gian về máy host.
# =============================================================================

import sys
import json
import re

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

    # Kiểm tra status code
    try:
        status_code = int(record["status_code"])
    except (ValueError, TypeError):
        sys.stderr.write("reporter:counter:DataQuality,Invalid_StatusCode,1\n")
        return None

    # Kiểm tra response time
    try:
        resp_time = float(record["response_time_ms"])
    except (ValueError, TypeError):
        sys.stderr.write("reporter:counter:DataQuality,Invalid_ResponseTime,1\n")
        return None

    # Chuẩn hóa dữ liệu
    clean_data = {
        "timestamp": str(record["timestamp"]).strip(),
        "ip": ip,
        "method": str(record.get("method", "GET")).upper(),
        "endpoint": str(record["endpoint"]).strip(),
        "status_code": status_code,
        "response_time_ms": round(resp_time, 2),
        "level": str(record["level"]).upper()
    }
    return json.dumps(clean_data)

def main():
    for line in sys.stdin:
        cleaned = clean_record(line)
        if cleaned:
            sys.stdout.write(cleaned + "\n")

if __name__ == "__main__":
    main()
