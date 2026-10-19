# =============================================================================
# spark_ml/export_dataset.py — Trích xuất tập dữ liệu CSV phục vụ PySpark MLlib
#
# Tạo tập dữ liệu chuẩn cho bài toán phân loại bất thường (Anomaly Classification)
# Cấu trúc cột tương thích hoàn toàn với quy trình PySpark MLlib (RandomForestClassifier)
# =============================================================================

import os
import sys
import csv
import random

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import SERVICES, ENDPOINTS, HTTP_METHODS

OUTPUT_CSV = "data/web_log_anomaly_dataset.csv"

def generate_csv_dataset(n_samples=10000):
    os.makedirs("data", exist_ok=True)
    print(f"[INFO] Đang tạo tập dữ liệu huấn luyện PySpark ML: {n_samples:,} dòng...")

    header = [
        "log_id",
        "service",
        "method",
        "endpoint",
        "status_code",
        "response_time_ms",
        "hour",
        "is_anomaly"  # Label: 'Anomaly' hoặc 'Normal'
    ]

    records = []
    for i in range(1, n_samples + 1):
        # 8% dữ liệu là Anomaly (tỉ lệ ngoại lai thực tế)
        is_bad = random.random() < 0.08

        if is_bad:
            label = "Anomaly"
            # Đặc trưng bất thường: Thời gian xử lý cao, lỗi 500/503 hoặc 401/404 liên tục
            service = random.choice(["auth-service", "payment-service", "api-gateway"])
            endpoint = random.choice(["/api/users/login", "/api/payments/process", "/api/orders/create"])
            method = random.choice(["POST", "POST", "GET"])
            status_code = random.choice([500, 503, 401, 429])
            response_time_ms = round(random.gauss(2500, 600), 2)
            if response_time_ms < 500:
                response_time_ms = 1800.0
            hour = random.choice([9, 10, 11, 14, 15, 20])
        else:
            label = "Normal"
            service = random.choice(SERVICES)
            endpoint = random.choice(ENDPOINTS)
            method = random.choice(HTTP_METHODS)
            status_code = random.choices([200, 201, 400, 404], weights=[0.80, 0.10, 0.05, 0.05])[0]
            response_time_ms = round(max(10.0, random.gauss(150, 80)), 2)
            hour = random.randint(0, 23)

        records.append([
            f"REQ_{i:06d}",
            service,
            method,
            endpoint,
            status_code,
            response_time_ms,
            hour,
            label
        ])

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(records)

    print(f"[SUCCESS] Đã tạo thành công tập dữ liệu tại: {OUTPUT_CSV}")
    print(f"          - Tổng số mẫu  : {n_samples:,}")
    print(f"          - Cột nhãn mục tiêu: 'is_anomaly' ('Normal' vs 'Anomaly')")

if __name__ == "__main__":
    generate_csv_dataset(15000)
