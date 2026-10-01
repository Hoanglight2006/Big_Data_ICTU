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

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import (
    LOG_DATE,
    THRESHOLD_WARNING_STD,
    THRESHOLD_CRITICAL_STD,
    MAX_5XX_RATE_PERCENT,
    MAX_IP_REQUESTS_PER_HOUR,
    MAX_RESPONSE_TIME_MS
)

RESULTS_FILE = "data/mapreduce_results.txt"
REPORT_OUTPUT_FILE = "data/anomaly_report.json"
DAILY_REPORT_OUTPUT_FILE = f"data/anomaly_report_{LOG_DATE}.json"

def calculate_stats(values):
    """Tính Mean và Standard Deviation (độ lệch chuẩn)."""
    if not values:
        return 0.0, 0.0
    n = len(values)
    mean = sum(values) / n
    variance = sum((x - mean) ** 2 for x in values) / (n - 1) if n > 1 else 0.0
    std_dev = math.sqrt(variance)
    return mean, std_dev

import argparse
import subprocess

def load_mapreduce_lines():
    """
    Đọc kết quả MapReduce từ nhiều nguồn theo thứ tự ưu tiên:
    1. Stream stdin (piped từ lệnh hdfs dfs -cat)
    2. Đọc trực tiếp từ thư mục HDFS (--hdfs /data/output/<LOG_DATE>)
    3. Đọc từ file local (fallback)
    """
    parser = argparse.ArgumentParser(description="Rule-based Anomaly Detector")
    parser.add_argument("--hdfs", default=None, help="Đường dẫn HDFS chứa output MapReduce")
    parser.add_argument("--file", default=None, help="Đường dẫn file kết quả local")
    parser.add_argument("--date", default=LOG_DATE, help="Ngày phân tích (YYYY-MM-DD)")
    args, _ = parser.parse_known_args()

    date_str = args.date
    lines = []

    # 1. Kiểm tra stream stdin
    if not sys.stdin.isatty():
        for line in sys.stdin:
            lines.append(line)
        if lines:
            return lines, date_str

    # 2. Đọc trực tiếp từ HDFS không qua file trung gian
    hdfs_target = args.hdfs or os.getenv("HDFS_OUTPUT_DIR") or f"/data/output/{date_str}"
    try:
        proc = subprocess.run(["hdfs", "dfs", "-cat", f"{hdfs_target}/part-*"],
                              capture_output=True, text=True)
        if proc.returncode == 0 and proc.stdout.strip():
            lines = proc.stdout.splitlines()
            return lines, date_str
    except Exception:
        pass

    # 3. Fallback đọc file local nếu có
    local_target = args.file or RESULTS_FILE
    if os.path.exists(local_target):
        for enc in ["utf-8-sig", "utf-8", "utf-16", "cp1252"]:
            try:
                with open(local_target, "r", encoding=enc) as f:
                    lines = f.readlines()
                break
            except Exception:
                continue
        return lines, date_str

    print(f"[ERROR] Không tìm thấy kết quả MapReduce trên HDFS ({hdfs_target}) hoặc file local ({local_target}).")
    sys.exit(1)

def parse_mapreduce_data(lines):
    """Parse danh sách dòng kết quả MapReduce thành các dictionary dữ liệu."""
    hour_requests = defaultdict(int)
    hour_errors = defaultdict(int)
    hour_5xx = defaultdict(int)
    hour_avg_resp = defaultdict(float)
    ip_requests = defaultdict(lambda: defaultdict(int)) # hour -> ip -> count
    endpoint_errors = defaultdict(int)

    for line in lines:
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

def detect_anomalies(data, date_str=LOG_DATE):
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
    print(f"Thống kê Traffic theo giờ: Mean = {mean_req:.1f} reqs/h | StdDev = {std_req:.1f}")
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

        status_text = ", ".join(status_flags) if status_flags else "NORMAL"
        badge = "[!] " if "CRITICAL" in status_text or "HIGH" in status_text else "    "
        print(f"{badge}{h}:00  | {total:<10,} | {err_count} ({err_rate:4.1f}%)   | {c5xx} ({r5xx_rate:4.1f}%)   | {avg_resp:<15.1f} | {status_text}")

    print("=" * 80)

    # Rule 4: IP Flood Detection
    print("\nKiểm tra phát hiện tấn công / IP Flood (Ngưỡng > 1,000 req/h):")
    found_flood = False
    for h, ip_map in ip_reqs.items():
        for ip, count in ip_map.items():
            if count >= MAX_IP_REQUESTS_PER_HOUR:
                found_flood = True
                print(f"   [ALERT] IP: {ip:<15} | Giờ: {h}:00 | Số request: {count:,} reqs/h -> Co dau hieu IP Flood!")
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
    print("\nTop endpoints gặp lỗi nhiều nhất:")
    sorted_endpoints = sorted(endpoint_errs.items(), key=lambda x: x[1], reverse=True)[:5]
    for ep, count in sorted_endpoints:
        print(f"   - {ep:<30}: {count:,} lỗi")

    print("\n" + "=" * 80)
    print(f"Tổng kết: Đã phát hiện {len(anomalies)} cảnh báo bất thường.")
    print("=" * 80)

    # Cấu trúc báo cáo JSON
    report_data = {
        "report_date": date_str,
        "baseline": {
            "mean_hourly_traffic": round(mean_req, 2),
            "std_hourly_traffic": round(std_req, 2),
            "warning_threshold": round(warn_threshold, 2),
            "critical_threshold": round(crit_threshold, 2)
        },
        "total_anomalies_detected": len(anomalies),
        "anomalies": anomalies
    }

    report_json_str = json.dumps(report_data, indent=2, ensure_ascii=False)

    # 1. Lưu trực tiếp lên HDFS (Không cần tải file về máy)
    hdfs_report_dir = f"/data/reports/{date_str}"
    try:
        subprocess.run(["hdfs", "dfs", "-mkdir", "-p", hdfs_report_dir], capture_output=True)
        put_proc = subprocess.run(["hdfs", "dfs", "-put", "-f", "-", f"{hdfs_report_dir}/anomaly_report.json"],
                                  input=report_json_str, text=True, capture_output=True)
        if put_proc.returncode == 0:
            print(f"[INFO] Báo cáo bất thường đã được lưu trực tiếp trên HDFS:")
            print(f"       -> {hdfs_report_dir}/anomaly_report.json")
    except Exception:
        pass

    # 2. Lưu fallback local nếu thư mục data/ có sẵn
    if os.path.exists("data"):
        try:
            with open(REPORT_OUTPUT_FILE, "w", encoding="utf-8") as f:
                f.write(report_json_str)
            daily_file = f"data/anomaly_report_{date_str}.json"
            with open(daily_file, "w", encoding="utf-8") as f:
                f.write(report_json_str)
        except Exception:
            pass

if __name__ == "__main__":
    lines, date_str = load_mapreduce_lines()
    data = parse_mapreduce_data(lines)
    detect_anomalies(data, date_str)
