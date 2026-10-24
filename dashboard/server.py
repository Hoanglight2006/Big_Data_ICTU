#!/usr/bin/env python3
# =============================================================================
# dashboard/server.py — Máy chủ web Dashboard hiển thị kết quả phân tích HDFS
# =============================================================================
import os
import sys
import json
import socket
import subprocess
import urllib.parse
from http.server import HTTPServer, SimpleHTTPRequestHandler
from datetime import datetime

LAST_HDFS_STATUS = "UNKNOWN"

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

# Thêm thư mục gốc vào path để import cấu hình
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(ROOT_DIR)

try:
    from config.settings import LOG_DATE, HDFS_REPORT_DIR, HDFS_OUTPUT_DIR
except ImportError:
    LOG_DATE = datetime.now().strftime("%Y-%m-%d")
    HDFS_REPORT_DIR = f"/data/reports/{LOG_DATE}"
    HDFS_OUTPUT_DIR = f"/data/output/{LOG_DATE}"

DASHBOARD_DIR = os.path.join(ROOT_DIR, "dashboard")


def check_hdfs_alive():
    """Kiểm tra trạng thái cụm HDFS bằng socket nhanh hoặc lệnh hdfs."""
    global LAST_HDFS_STATUS

    # 1. Thử kết nối socket nhanh tới NameNode (9870 UI hoặc 9000/8020 RPC)
    for port in [9870, 9000, 8020]:
        try:
            with socket.create_connection(("localhost", port), timeout=0.8):
                LAST_HDFS_STATUS = "CONNECTED"
                return "CONNECTED"
        except Exception:
            pass

    # 2. Nếu vừa đọc dữ liệu HDFS thành công gần đây
    if LAST_HDFS_STATUS == "CONNECTED":
        return "CONNECTED"

    # 3. Fallback gọi lệnh hdfs với timeout hợp lý (8s)
    try:
        proc = subprocess.run(
            ["hdfs", "dfs", "-test", "-e", "/"],
            capture_output=True,
            text=True,
            timeout=8
        )
        if proc.returncode == 0:
            LAST_HDFS_STATUS = "CONNECTED"
            return "CONNECTED"
    except Exception:
        pass

    LAST_HDFS_STATUS = "DISCONNECTED"
    return "DISCONNECTED"


def read_report_from_hdfs_or_local(date_str):
    """
    Ưu tiên đọc trực tiếp từ HDFS: /data/reports/<date>/anomaly_report.json
    Nếu không có kết nối HDFS, fallback đọc file local trong data/
    """
    global LAST_HDFS_STATUS
    hdfs_target = f"/data/reports/{date_str}/anomaly_report.json"
    
    # 1. Thử đọc từ HDFS
    try:
        proc = subprocess.run(
            ["hdfs", "dfs", "-cat", hdfs_target],
            capture_output=True,
            text=True,
            timeout=8
        )
        if proc.returncode == 0 and proc.stdout.strip():
            LAST_HDFS_STATUS = "CONNECTED"
            data = json.loads(proc.stdout)
            data["_source"] = "HDFS"
            data["_source_path"] = hdfs_target
            return data
    except Exception as e:
        pass

    # 2. Fallback đọc file local
    candidate_paths = [
        os.path.join(ROOT_DIR, "data", f"anomaly_report_{date_str}.json"),
        os.path.join(ROOT_DIR, "data", "anomaly_report.json")
    ]
    for p in candidate_paths:
        if os.path.exists(p):
            try:
                with open(p, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    data["_source"] = "Local File (Fallback)"
                    data["_source_path"] = p
                    return data
            except Exception:
                pass

    return {
        "error": f"Không tìm thấy báo cáo cho ngày {date_str} trên HDFS hoặc file cục bộ.",
        "report_date": date_str,
        "_source": "None"
    }


def read_mapreduce_metrics(date_str):
    """
    Đọc dữ liệu tổng hợp chi tiết từ /data/output/<date>/part-* trên HDFS
    hoặc fallback từ file local.
    """
    lines = []
    hdfs_target = f"/data/output/{date_str}/part-*"
    source_type = "None"

    global LAST_HDFS_STATUS
    try:
        proc = subprocess.run(
            ["hdfs", "dfs", "-cat", hdfs_target],
            capture_output=True,
            text=True,
            timeout=8
        )
        if proc.returncode == 0 and proc.stdout.strip():
            LAST_HDFS_STATUS = "CONNECTED"
            lines = proc.stdout.splitlines()
            source_type = "HDFS"
    except Exception:
        pass

    if not lines:
        local_target = os.path.join(ROOT_DIR, "data", "mapreduce_results.txt")
        if os.path.exists(local_target):
            try:
                with open(local_target, "r", encoding="utf-8") as f:
                    lines = f.readlines()
                source_type = "Local File"
            except Exception:
                pass

    # Parse 24 giờ
    hour_reqs = {f"{h:02d}": 0 for h in range(24)}
    hour_errs = {f"{h:02d}": 0 for h in range(24)}
    hour_5xx = {f"{h:02d}": 0 for h in range(24)}
    hour_resp = {f"{h:02d}": 0.0 for h in range(24)}
    ip_reqs = {}
    endpoint_errs = {}

    for line in lines:
        line = line.strip()
        if not line or "\t" not in line:
            continue
        key, val_str = line.split("\t", 1)
        try:
            val = float(val_str)
        except ValueError:
            continue

        if key.startswith("hour_req:"):
            h = key.split(":")[1]
            hour_reqs[h] = int(val)
        elif key.startswith("hour_err:"):
            h = key.split(":")[1]
            hour_errs[h] = int(val)
        elif key.startswith("hour_5xx:"):
            h = key.split(":")[1]
            hour_5xx[h] = int(val)
        elif key.startswith("hour_resp:"):
            h = key.split(":")[1]
            hour_resp[h] = round(val, 2)
        elif key.startswith("ip_req:"):
            sub = key.split(":")
            if len(sub) == 3:
                h, ip = sub[1], sub[2]
                if h not in ip_reqs:
                    ip_reqs[h] = {}
                ip_reqs[h][ip] = int(val)
        elif key.startswith("endpoint_err:"):
            ep = key.split(":", 1)[1]
            endpoint_errs[ep] = int(val)

    return {
        "source": source_type,
        "date": date_str,
        "hour_requests": hour_reqs,
        "hour_errors": hour_errs,
        "hour_5xx": hour_5xx,
        "hour_avg_resp": hour_resp,
        "ip_requests": ip_reqs,
        "endpoint_errors": endpoint_errs
    }


class DashboardHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=DASHBOARD_DIR, **kwargs)

    def do_GET(self):
        parsed_url = urllib.parse.urlparse(self.path)
        path = parsed_url.path
        query = urllib.parse.parse_qs(parsed_url.query)
        target_date = query.get("date", [LOG_DATE])[0]

        if path == "/api/report":
            data = read_report_from_hdfs_or_local(target_date)
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps(data, ensure_ascii=False).encode("utf-8"))
            return

        elif path == "/api/metrics":
            data = read_mapreduce_metrics(target_date)
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps(data, ensure_ascii=False).encode("utf-8"))
            return

        elif path == "/api/status":
            hdfs_status = check_hdfs_alive()

            status_payload = {
                "hdfs_status": hdfs_status,
                "current_date": target_date,
                "server_time": datetime.now().isoformat()
            }
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps(status_payload).encode("utf-8"))
            return

        return super().do_GET()


def run_server(port=8050):
    server_address = ("", port)
    httpd = HTTPServer(server_address, DashboardHandler)
    print("=" * 70)
    print(" [BIG DATA WEB SERVER LOG ANALYTICS DASHBOARD]")
    print(f" -> Localhost URL: http://localhost:{port}")
    print(f" -> Thư mục phục vụ: {DASHBOARD_DIR}")
    print(" -> Nguồn dữ liệu  : Tự động kết nối HDFS (Hadoop 3.2.1) & Fallback cục bộ")
    print(" Nhấn Ctrl + C để dừng máy chủ...")
    print("=" * 70)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n[INFO] Đã dừng Dashboard server.")
        httpd.server_close()


if __name__ == "__main__":
    p = int(sys.argv[1]) if len(sys.argv) > 1 else 8050
    run_server(p)
