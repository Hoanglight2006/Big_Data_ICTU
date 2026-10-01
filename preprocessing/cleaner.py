#!/usr/bin/env python3
# =============================================================================
# preprocessing/cleaner.py — Làm Sạch & Chuẩn Hóa Dữ Liệu Log Web Server
#
# Chương trình: Samsung Innovation Campus (SIC) - Big Data Course
# Nhóm: Group 3
# Thành viên phụ trách: NÔNG MINH TRÍ & TRIỆU VĂN HUY
#
# Nhiệm vụ trong Action Plan:
#   "Receive the raw HDFS data from Dang Van Vinh, cleaning and structuring it
#    to ensure high-quality inputs for the MapReduce analysis."
#
# Nguyên lý hoạt động:
#   1. Đọc luồng log thô từ file nguồn hoặc HDFS.
#   2. Kiểm tra tính toàn vẹn của JSON, lọc bỏ các dòng bị hỏng (corrupted / malformed).
#   3. Kiểm tra các trường bắt buộc (timestamp, ip, status_code, endpoint, response_time_ms).
#   4. Chuẩn hóa định dạng IP, loại bỏ ký tự lạ, chuyển đổi kiểu dữ liệu nghiêm ngặt.
#   5. Tách và ghi nhận các bản ghi rác/hỏng vào 'data/corrupted_logs.txt'.
#   6. Xuất tập dữ liệu sạch chất lượng cao 'data/cleaned_logs.json' sẵn sàng cho MapReduce.
# =============================================================================

import os
import sys
import json
import re
from datetime import datetime

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import LOG_DATE, OUTPUT_FILE

# Biểu thức chính quy kiểm tra định dạng IPv4
IPV4_REGEX = re.compile(r"^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$")

# Danh sách các trường bắt buộc phải có trong mỗi bản ghi
REQUIRED_FIELDS = ["timestamp", "ip", "status_code", "endpoint", "response_time_ms", "level"]

def is_valid_ipv4(ip_str):
    """Kiểm tra địa chỉ IPv4 hợp lệ."""
    if not ip_str or not IPV4_REGEX.match(ip_str):
        return False
    parts = ip_str.split(".")
    return all(0 <= int(part) <= 255 for part in parts)

def clean_and_structure_logs(input_path="data/fake_logs.json", 
                             output_clean_path="data/cleaned_logs.json", 
                             output_corrupt_path="data/corrupted_logs.txt"):
    """
    Tiến hành làm sạch và chuẩn hóa dữ liệu log.
    """
    print("================================================================================")
    print(" TIỀN XỬ LÝ & LÀM SẠCH DỮ LIỆU LOG (PREPROCESSING & DATA CLEANING)")
    print(" Phụ trách: Nông Minh Trí & Triệu Văn Huy (Group 3)")
    print(f" Nguồn log thô       : {input_path}")
    print(f" File dữ liệu sạch   : {output_clean_path}")
    print(f" File log lỗi bị loại: {output_corrupt_path}")
    print("================================================================================")

    if not os.path.exists(input_path):
        # Fallback thử tìm ở thư mục spool nếu không có fake_logs.json
        if os.path.exists(OUTPUT_FILE):
            input_path = OUTPUT_FILE
        else:
            print(f"[ERROR] Không tìm thấy file nguồn tại {input_path} hoặc {OUTPUT_FILE}")
            print("        Hãy chạy module sinh log của bạn Vinh trước: python data/generate_logs.py")
            sys.exit(1)

    total_records = 0
    clean_records = 0
    corrupted_records = 0

    clean_logs = []
    corrupted_logs = []

    # Đọc file nguồn với cơ chế fallback encoding
    lines = []
    for enc in ["utf-8-sig", "utf-8", "utf-16", "cp1252"]:
        try:
            with open(input_path, "r", encoding=enc) as f:
                lines = f.readlines()
            break
        except (UnicodeDecodeError, UnicodeError):
            continue

    total_records = len(lines)
    print(f"[INFO] Bắt đầu quét và làm sạch {total_records:,} bản ghi log thô...\n")

    for idx, line in enumerate(lines, 1):
        line = line.strip()
        if not line:
            continue

        # 1. Kiểm tra định dạng JSON
        try:
            record = json.loads(line)
        except json.JSONDecodeError as e:
            corrupted_records += 1
            corrupted_logs.append(f"Line {idx} [JSON_ERROR]: {line} -> {e}")
            continue

        # 2. Kiểm tra các trường bắt buộc
        missing_fields = [f for f in REQUIRED_FIELDS if f not in record or record[f] is None]
        if missing_fields:
            corrupted_records += 1
            corrupted_logs.append(f"Line {idx} [MISSING_FIELDS {missing_fields}]: {line}")
            continue

        # 3. Chuẩn hóa & kiểm tra địa chỉ IP
        ip = str(record["ip"]).strip()
        if not is_valid_ipv4(ip):
            corrupted_records += 1
            corrupted_logs.append(f"Line {idx} [INVALID_IP]: {ip}")
            continue

        # 4. Chuẩn hóa mã phản hồi HTTP (status_code)
        try:
            status_code = int(record["status_code"])
            if not (100 <= status_code <= 599):
                raise ValueError("Status code out of HTTP range")
        except (ValueError, TypeError):
            corrupted_records += 1
            corrupted_logs.append(f"Line {idx} [INVALID_STATUS_CODE]: {record.get('status_code')}")
            continue

        # 5. Chuẩn hóa thời gian phản hồi (response_time_ms)
        try:
            resp_time = float(record["response_time_ms"])
            if resp_time < 0:
                raise ValueError("Negative latency")
        except (ValueError, TypeError):
            corrupted_records += 1
            corrupted_logs.append(f"Line {idx} [INVALID_RESPONSE_TIME]: {record.get('response_time_ms')}")
            continue

        # 6. Chuẩn hóa cấu trúc ngày giờ timestamp
        raw_ts = str(record["timestamp"]).strip()
        try:
            # Hỗ trợ cả định dạng ISO 8601 có hoặc không có Z
            clean_ts = raw_ts.replace("Z", "+00:00")
            parsed_dt = datetime.fromisoformat(clean_ts)
        except Exception:
            corrupted_records += 1
            corrupted_logs.append(f"Line {idx} [INVALID_TIMESTAMP]: {raw_ts}")
            continue

        # 7. Bản ghi đạt chuẩn High-Quality Data -> Đưa vào danh sách sạch
        clean_record = {
            "timestamp": parsed_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "service": str(record.get("service", "unknown")).strip().lower(),
            "ip": ip,
            "method": str(record.get("method", "GET")).strip().upper(),
            "endpoint": str(record.get("endpoint", "/")).strip(),
            "status_code": status_code,
            "response_time_ms": round(resp_time, 2),
            "level": str(record.get("level", "INFO")).strip().upper()
        }
        clean_logs.append(clean_record)
        clean_records += 1

    # Lưu dữ liệu sạch ra file
    os.makedirs(os.path.dirname(output_clean_path) if os.path.dirname(output_clean_path) else ".", exist_ok=True)
    with open(output_clean_path, "w", encoding="utf-8") as f_out:
        for log in clean_logs:
            f_out.write(json.dumps(log) + "\n")

    # Lưu danh sách lỗi (nếu có)
    if corrupted_logs:
        with open(output_corrupt_path, "w", encoding="utf-8") as f_err:
            f_err.write("\n".join(corrupted_logs) + "\n")

    # In kết quả thống kê
    clean_percent = (clean_records / total_records * 100) if total_records > 0 else 0
    corrupt_percent = (corrupted_records / total_records * 100) if total_records > 0 else 0

    print("KẾT QUẢ TIỀN XỬ LÝ & LÀM SẠCH:")
    print(f"   - Tổng số log nhận vào     : {total_records:,}")
    print(f"   - Số bản ghi SẠCH (Clean)  : {clean_records:,} ({clean_percent:.2f}%)")
    print(f"   - Số bản ghi HỎNG/RÁC      : {corrupted_records:,} ({corrupt_percent:.2f}%)")
    print(f"\nDữ liệu sạch đã lưu tại: {output_clean_path}")
    print(f"Đã chuẩn hóa định dạng đầu vào cho bước MapReduce.\n")

if __name__ == "__main__":
    src_file = sys.argv[1] if len(sys.argv) > 1 else "data/fake_logs.json"
    clean_and_structure_logs(input_path=src_file)
