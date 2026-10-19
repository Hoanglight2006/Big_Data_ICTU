# =============================================================================
# spark_ml/export_cleaned_to_csv.py
#
# Chuyển đổi dữ liệu log đã làm sạch (data/cleaned_logs.json) của chính project
# thành file CSV (data/web_log_dataset.csv) để nạp vào PySpark MLlib và Google Colab
# =============================================================================

import os
import sys
import json
import csv

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

INPUT_JSON = "data/cleaned_logs.json"
OUTPUT_CSV = "data/web_log_dataset.csv"

def convert_cleaned_logs_to_csv():
    if not os.path.exists(INPUT_JSON):
        # Fallback tìm fake_logs.json nếu chưa có cleaned_logs.json
        if os.path.exists("data/fake_logs.json"):
            src_file = "data/fake_logs.json"
        else:
            print(f"[ERROR] Không tìm thấy file {INPUT_JSON}. Hãy chạy pipeline trước.")
            sys.exit(1)
    else:
        src_file = INPUT_JSON

    print(f"[INFO] Đang chuyển đổi dữ liệu thực tế từ: {src_file} -> {OUTPUT_CSV}...")

    header = ["service", "method", "endpoint", "status_code", "response_time_ms", "level", "is_anomaly"]
    rows = []

    with open(src_file, "r", encoding="utf-8", errors="ignore") as f:
        for idx, line in enumerate(f):
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except Exception:
                continue

            service = rec.get("service", "unknown")
            method = rec.get("method", "GET")
            endpoint = rec.get("endpoint", "/")
            status_code = int(rec.get("status_code", 200))
            response_time_ms = float(rec.get("response_time_ms", 0.0))
            level = rec.get("level", "INFO")

            # Tiêu chí nghiệp vụ của project:
            # Lỗi server 500/503 hoặc response_time >= 2000ms hoặc ERROR log là Bất thường (Anomaly)
            is_anomaly = "Anomaly" if (status_code >= 500 or response_time_ms >= 2000 or level == "ERROR") else "Normal"

            rows.append([service, method, endpoint, status_code, response_time_ms, level, is_anomaly])

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)

    print(f"[SUCCESS] Đã xuất thành công {len(rows):,} dòng dữ liệu thực tế của project sang: {OUTPUT_CSV}")

if __name__ == "__main__":
    convert_cleaned_logs_to_csv()
