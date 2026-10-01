#!/usr/bin/env python3
# =============================================================================
# anomaly/detector.py — Rule-based Anomaly Detection
#
# NGUYÊN LÝ HOẠT ĐỘNG:
#   - Đọc kết quả tổng hợp từ MapReduce (data/mapreduce_results.txt)
#   - Phân tích thống kê baseline (Mean, Standard Deviation) cho traffic theo giờ
#   - Áp dụng 6 Rules phát hiện bất thường:
#       + Rule 1 (Traffic Spike): Traffic theo giờ vượt mean + 2*std (WARNING) hoặc mean + 3*std (CRITICAL)
#       + Rule 2 (High Error Rate): Tỉ lệ log ERROR theo giờ vượt 5%
#       + Rule 3 (High 5xx Rate): Tỉ lệ mã lỗi HTTP 5xx theo giờ vượt 5%
#       + Rule 4 (IP Flood): Một IP gửi > 1,000 request trong 1 giờ
#       + Rule 5 (High Response Time): Response time trung bình của 1 giờ > 2,000ms
#       + Rule 6 (Endpoint Failure): Endpoint có số lượng lỗi lớn đột biến
#   - Xuất báo cáo tổng quan trực quan ra màn hình và file JSON report
# =============================================================================

import os
import sys
import math
import json
from collections import defaultdict

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import (
    THRESHOLD_WARNING_STD,
    THRESHOLD_CRITICAL_STD,
    MAX_5XX_RATE_PERCENT,
    MAX_IP_REQUESTS_PER_HOUR,
    MAX_RESPONSE_TIME_MS
)

RESULTS_FILE = "data/mapreduce_results.txt"
REPORT_OUTPUT_FILE = "data/anomaly_report.json"

def calculate_stats(values):
    """Tính Mean và Standard Deviation (độ lệch chuẩn)."""
    if not values:
        return 0.0, 0.0
    n = len(values)
    mean = sum(values) / n
    variance = sum((x - mean) ** 2 for x in values) / (n - 1) if n > 1 else 0.0
    std_dev = math.sqrt(variance)
    return mean, std_dev

def load_mapreduce_results(filepath):
    """Parse file kết quả MapReduce thành các dictionary dữ liệu."""
    if not os.path.exists(filepath):
        print(f"[ERROR] Không tìm thấy file kết quả MapReduce: {filepath}")
        print("        Hãy chạy mapreduce/run_job.sh trước.")
        sys.exit(1)

    hour_requests = defaultdict(int)
    hour_errors = defaultdict(int)
    hour_5xx = defaultdict(int)
    hour_avg_resp = defaultdict(float)
    ip_requests = defaultdict(lambda: defaultdict(int)) # hour -> ip -> count
    endpoint_errors = defaultdict(int)

    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) != 2:
                continue
            key, val_str = parts[0], parts[1]

            if key.startswith("hour_req:"):
                h = key.split(":")[1]
                hour_requests[h] = int(float(val_str))
            elif key.startswith("hour_err:"):
                h = key.split(":")[1]
                hour_errors[h] = int(float(val_str))
            elif key.startswith("hour_5xx:"):
                h = key.split(":")[1]
                hour_5xx[h] = int(float(val_str))
            elif key.startswith("hour_resp:"):
                h = key.split(":")[1]
                hour_avg_resp[h] = float(val_str)
            elif key.startswith("ip_req:"):
                # key dạng ip_req:<hour>:<ip>
                subparts = key.split(":")
                if len(subparts) == 3:
                    h, ip = subparts[1], subparts[2]
                    ip_requests[h][ip] = int(float(val_str))
            elif key.startswith("endpoint_err:"):
                endpoint = key.split(":", 1)[1]
                endpoint_errors[endpoint] = int(float(val_str))

    return {
        "hour_requests": hour_requests,
        "hour_errors": hour_errors,
        "hour_5xx": hour_5xx,
        "hour_avg_resp": hour_avg_resp,
        "ip_requests": ip_requests,
        "endpoint_errors": endpoint_errors,
    }

def detect_anomalies(data):
    hour_reqs = data["hour_requests"]
    hour_errs = data["hour_errors"]
    hour_5xx = data["hour_5xx"]
    hour_resp = data["hour_avg_resp"]
    ip_reqs = data["ip_requests"]
    endpoint_errs = data["endpoint_errors"]

    # 1. Tính toán baseline cho Traffic theo giờ
    req_counts = list(hour_reqs.values())
    mean_req, std_req = calculate_stats(req_counts)
    warn_threshold = mean_req + THRESHOLD_WARNING_STD * std_req
    crit_threshold = mean_req + THRESHOLD_CRITICAL_STD * std_req

    anomalies = []

    print("\n" + "=" * 80)
    print("                BÁO CÁO PHÁT HIỆN BẤT THƯỜNG (ANOMALY DETECTION REPORT)                ")
    print("=" * 80)
    print(f"📊 Thống kê Traffic theo giờ: Mean = {mean_req:.1f} reqs/h | StdDev = {std_req:.1f}")
    print(f"   Ngưỡng Cảnh Báo (WARNING):  > {warn_threshold:.1f} reqs/h (Mean + 2*Std)")
    print(f"   Ngưỡng Nghiêm Trọng (CRIT): > {crit_threshold:.1f} reqs/h (Mean + 3*Std)")
    print("-" * 80)

    # Đánh giá theo từng giờ (00 -> 23)
    print(f"{'Giờ':<6} | {'Tổng Req':<10} | {'ERROR (%)':<12} | {'5xx (%)':<12} | {'Avg Resp (ms)':<15} | {'Trạng thái'}")
    print("-" * 80)

    for h_int in range(24):
        h = f"{h_int:02d}"
        total = hour_reqs.get(h, 0)
        err_count = hour_errs.get(h, 0)
        c5xx = hour_5xx.get(h, 0)
        avg_resp = hour_resp.get(h, 0.0)

        err_rate = (err_count / total * 100) if total > 0 else 0.0
        r5xx_rate = (c5xx / total * 100) if total > 0 else 0.0

        status_flags = []

        # Rule 1: Traffic Spike
        if total >= crit_threshold:
            status_flags.append("CRITICAL: Traffic Spike")
            anomalies.append({
                "rule": "R01_TRAFFIC_SPIKE",
                "severity": "CRITICAL",
                "hour": h,
                "message": f"Traffic giờ {h}:00 đạt {total:,} requests (vượt ngưỡng Critical {crit_threshold:.1f})"
            })
        elif total >= warn_threshold:
            status_flags.append("WARNING: High Traffic")
            anomalies.append({
                "rule": "R01_TRAFFIC_SPIKE",
                "severity": "WARNING",
                "hour": h,
                "message": f"Traffic giờ {h}:00 đạt {total:,} requests (vượt ngưỡng Warning {warn_threshold:.1f})"
            })

        # Rule 2: High Error Rate
        if err_rate >= MAX_5XX_RATE_PERCENT:
            status_flags.append("HIGH_ERROR")
            anomalies.append({
                "rule": "R02_ERROR_RATE",
                "severity": "CRITICAL" if err_rate > 10 else "WARNING",
                "hour": h,
                "message": f"Giờ {h}:00 có {err_count} lỗi ({err_rate:.1f}% tổng traffic)"
            })

        # Rule 3: High 5xx Rate
        if r5xx_rate >= MAX_5XX_RATE_PERCENT:
            status_flags.append("HIGH_5XX")
            anomalies.append({
                "rule": "R03_HTTP_5XX_RATE",
                "severity": "CRITICAL",
                "hour": h,
                "message": f"Giờ {h}:00 có {c5xx} lỗi 5xx Server Error ({r5xx_rate:.1f}%)"
            })

        # Rule 5: Response Time
        if avg_resp >= MAX_RESPONSE_TIME_MS:
            status_flags.append("SLOW_LATENCY")
            anomalies.append({
                "rule": "R05_RESPONSE_TIME",
                "severity": "WARNING",
                "hour": h,
                "message": f"Response time TB giờ {h}:00 cao bất thường: {avg_resp:.1f}ms"
            })

        status_text = ", ".join(status_flags) if status_flags else "NORMAL ✅"
        badge = "🚨 " if "CRITICAL" in status_text or "HIGH" in status_text else "   "
        print(f"{badge}{h}:00  | {total:<10,} | {err_count} ({err_rate:4.1f}%)   | {c5xx} ({r5xx_rate:4.1f}%)   | {avg_resp:<15.1f} | {status_text}")

    print("=" * 80)

    # Rule 4: IP Flood Detection
    print("\n🚨 KIỂM TRA PHÁT HIỆN TẤN CÔNG / IP FLOOD (Ngưỡng > 1,000 req/h):")
    found_flood = False
    for h, ip_map in ip_reqs.items():
        for ip, count in ip_map.items():
            if count >= MAX_IP_REQUESTS_PER_HOUR:
                found_flood = True
                print(f"   [ALERT] IP: {ip:<15} | Giờ: {h}:00 | Số request: {count:,} reqs/h -> CÓ DẤU HIỆU FLOOD/DDOS!")
                anomalies.append({
                    "rule": "R04_IP_FLOOD",
                    "severity": "CRITICAL",
                    "hour": h,
                    "ip": ip,
                    "message": f"IP {ip} gửi {count:,} request trong giờ {h}:00 (Vượt ngưỡng {MAX_IP_REQUESTS_PER_HOUR})"
                })
    if not found_flood:
        print("   Không phát hiện IP nào vượt ngưỡng flood.")

    # Rule 6: Top Endpoints gặp lỗi
    print("\n📌 TOP ENDPOINTS GẶP LỖI NHIỀU NHẤT:")
    sorted_endpoints = sorted(endpoint_errs.items(), key=lambda x: x[1], reverse=True)[:5]
    for ep, count in sorted_endpoints:
        print(f"   - {ep:<30}: {count:,} lỗi")

    print("\n" + "=" * 80)
    print(f"✅ TỔNG KẾT: Đã phát hiện {len(anomalies)} cảnh báo bất thường.")
    print("=" * 80)

    # Lưu kết quả ra file JSON
    report_data = {
        "baseline": {
            "mean_hourly_traffic": mean_req,
            "std_hourly_traffic": std_req,
            "warning_threshold": warn_threshold,
            "critical_threshold": crit_threshold
        },
        "total_anomalies_detected": len(anomalies),
        "anomalies": anomalies
    }
    with open(REPORT_OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(report_data, f, indent=2, ensure_ascii=False)
    print(f"[INFO] Báo cáo chi tiết đã được lưu tại: {REPORT_OUTPUT_FILE}\n")

if __name__ == "__main__":
    data = load_mapreduce_results(RESULTS_FILE)
    detect_anomalies(data)
