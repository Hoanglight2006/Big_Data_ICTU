import os
import sys
import subprocess
import glob
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import LOG_DATE, HDFS_RAW_DIR, SPOOL_DIR, OUTPUT_FILE

def run_command(cmd):
    """Thực thi lệnh shell và in log."""
    print(f"[CMD] {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        if result.stderr.strip():
            print(f"[WARN/ERR] {result.stderr.strip()}")
    else:
        if result.stdout.strip():
            print(f"[OUT] {result.stdout.strip()}")
    return result.returncode == 0

def check_hdfs_files():
    """Kiểm tra xem thư mục HDFS Raw đã có file log hay chưa."""
    res = subprocess.run(["hdfs", "dfs", "-ls", HDFS_RAW_DIR], capture_output=True, text=True)
    if res.returncode == 0 and res.stdout.strip():
        # Kiểm tra có file log thực sự
        lines = [l for l in res.stdout.splitlines() if not l.startswith("Found")]
        if len(lines) > 0:
            return True
    return False

def main():
    print("==========================================================")
    print(" NẠP DỮ LIỆU LOG VÀO HDFS")
    print(f" Ngày phân tích : {LOG_DATE}")
    print(f" Thư mục HDFS   : {HDFS_RAW_DIR}")
    print(f" Thư mục cục bộ : {SPOOL_DIR}")
    print("==========================================================")

    # Đảm bảo NameNode không ở chế độ Safe Mode (chỉ đọc)
    run_command(["hdfs", "dfsadmin", "-safemode", "leave"])

    # 1. Tạo thư mục trên HDFS
    print(f"\n[1/3] Tạo thư mục trên HDFS: {HDFS_RAW_DIR}")
    run_command(["hdfs", "dfs", "-mkdir", "-p", HDFS_RAW_DIR])

    target_log = os.path.join(SPOOL_DIR, f"web_access_{LOG_DATE}.log")
    completed_log = f"{target_log}.COMPLETED"

    # 2. Kiểm tra tiến trình Apache Flume Agent ở Terminal 1
    print(f"\n[2/3] Kiểm tra Apache Flume Agent ở Terminal 1...")
    flume_ingested = False

    # Chờ tối đa 15 giây để Flume xử lý file
    for i in range(15):
        if os.path.exists(completed_log) or check_hdfs_files():
            flume_ingested = True
            break
        time.sleep(1)

    if flume_ingested:
        print("      [OK] Apache Flume Agent đã nạp file vào HDFS.")
    else:
        print("      [CẢNH BÁO] Không thấy Flume Agent ở Terminal 1.")
        print("                 Nạp trực tiếp qua lệnh HDFS put...")
        
        # Tìm file nguồn để nạp
        source_file = None
        if os.path.exists(target_log):
            source_file = target_log
        elif os.path.exists(completed_log):
            source_file = completed_log
        elif os.path.exists(OUTPUT_FILE):
            source_file = OUTPUT_FILE

        if not source_file:
            print(f"[LỖI] Không tìm thấy file log tại {SPOOL_DIR}")
            sys.exit(1)

        run_command(["hdfs", "dfs", "-put", "-f", source_file, f"{HDFS_RAW_DIR}/access.log"])

    # 3. Xác nhận kết quả
    print(f"\n[3/3] Danh sách file trên HDFS Raw:")
    run_command(["hdfs", "dfs", "-ls", HDFS_RAW_DIR])

if __name__ == "__main__":
    main()
