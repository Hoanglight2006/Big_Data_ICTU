#!/usr/bin/env python3
# =============================================================================
# flume/ingest_to_hdfs.py — Ingestion Runner vào HDFS
#
# Chức năng:
#   - Đảm bảo thư mục HDFS /data/logs/<LOG_DATE> tồn tại.
#   - Đẩy file log từ data/spool/ lên HDFS theo đúng phân vùng ngày.
#   - Hoạt động như Ingestion Agent tự động cho Pipeline.
# =============================================================================

import os
import sys
import subprocess
import glob

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import LOG_DATE, HDFS_INPUT_DIR, SPOOL_DIR, OUTPUT_FILE

def run_command(cmd):
    """Thực thi lệnh shell và in log."""
    print(f"[CMD] {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"[WARN/ERR] {result.stderr.strip()}")
    else:
        if result.stdout.strip():
            print(f"[OUT] {result.stdout.strip()}")
    return result.returncode == 0

def main():
    print("==========================================================")
    print(" [INFO] INGESTION: NAP DU LIEU LOG VAO HDFS")
    print(f" Ngay phan tich : {LOG_DATE}")
    print(f" HDFS Input Dir : {HDFS_INPUT_DIR}")
    print(f" Local Spool Dir: {SPOOL_DIR}")
    print("==========================================================")

    # 0. Đảm bảo NameNode không bị kẹt ở chế độ Safe Mode (Read-only)
    run_command(["hdfs", "dfsadmin", "-safemode", "leave"])

    # 1. Tạo thư mục trên HDFS
    print(f"\n[1/3] Tao thu muc tren HDFS: {HDFS_INPUT_DIR}")
    run_command(["hdfs", "dfs", "-mkdir", "-p", HDFS_INPUT_DIR])

    # 2. Tìm file log trong spool dir
    log_files = glob.glob(os.path.join(SPOOL_DIR, f"web_access_{LOG_DATE}.log"))
    if not log_files:
        # Fallback tìm các file log khác hoặc fake_logs.json
        if os.path.exists(OUTPUT_FILE):
            log_files = [OUTPUT_FILE]
        elif os.path.exists("data/fake_logs.json"):
            log_files = ["data/fake_logs.json"]

    if not log_files:
        print(f"[ERROR] Khong tim thay file log cho ngay {LOG_DATE} tai {SPOOL_DIR}")
        print("        Hay chay: python3 data/generate_logs.py truoc.")
        sys.exit(1)

    target_file = log_files[0]
    print(f"\n[2/3] Nap file log [{target_file}] len HDFS [{HDFS_INPUT_DIR}]...")
    success = run_command(["hdfs", "dfs", "-put", "-f", target_file, f"{HDFS_INPUT_DIR}/access.log"])

    if success:
        print(f"\n[3/3] [INFO] Nap du lieu vao HDFS thanh cong.")
        run_command(["hdfs", "dfs", "-ls", HDFS_INPUT_DIR])
    else:
        print(f"\n[3/3] [ERROR] Nap du lieu vao HDFS that bai. Hay kiem tra dich vu Hadoop.")
        sys.exit(1)

if __name__ == "__main__":
    main()
