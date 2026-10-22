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

    # 2. Chờ Apache Flume Agent ở Terminal 1 nạp file vào HDFS
    print(f"\n[2/3] Chờ Apache Flume Agent (Terminal 1) xử lý và nạp log vào HDFS...")
    flume_ingested = False

    # Chờ tối đa 120 giây để Flume Agent đọc file spool và ghi vào HDFS
    max_wait_seconds = 120
    for second in range(1, max_wait_seconds + 1):
        if os.path.exists(completed_log) or check_hdfs_files():
            flume_ingested = True
            break
        if second % 10 == 0:
            print(f"      ... đang chờ Flume nạp dữ liệu ({second}/{max_wait_seconds}s)")
        time.sleep(1)

    if not flume_ingested:
        print("\n" + "=" * 65)
        print(" [LỖI] Dữ liệu chưa được nạp vào HDFS bởi Apache Flume!")
        print(" Hệ thống yêu cầu Apache Flume hoạt động thực tế.")
        print(" Hãy mở Terminal 1 và khởi động Flume Agent trước:")
        print("     bash flume/start_flume.sh")
        print("=" * 65)
        sys.exit(1)

    print("      [OK] Apache Flume Agent đã nạp log vào HDFS thành công.")

    # 3. Xác nhận kết quả trên HDFS
    print(f"\n[3/3] Danh sách file do Flume nạp tại HDFS Raw:")
    run_command(["hdfs", "dfs", "-ls", HDFS_RAW_DIR])

if __name__ == "__main__":
    main()
