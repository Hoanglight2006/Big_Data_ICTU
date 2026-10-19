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
    print(" [INFO] INGESTION: NAP DU LIEU LOG VAO HDFS")
    print(f" Ngay phan tich : {LOG_DATE}")
    print(f" HDFS Raw Dir   : {HDFS_RAW_DIR}")
    print(f" Local Spool Dir: {SPOOL_DIR}")
    print("==========================================================")

    # 0. Đảm bảo NameNode không bị kẹt ở chế độ Safe Mode (Read-only)
    run_command(["hdfs", "dfsadmin", "-safemode", "leave"])

    # 1. Tạo thư mục trên HDFS
    print(f"\n[1/3] Tao thu muc tren HDFS: {HDFS_RAW_DIR}")
    run_command(["hdfs", "dfs", "-mkdir", "-p", HDFS_RAW_DIR])

    target_log = os.path.join(SPOOL_DIR, f"web_access_{LOG_DATE}.log")
    completed_log = f"{target_log}.COMPLETED"

    # 2. Kiểm tra tiến trình Apache Flume Agent ở Terminal 1
    print(f"\n[2/3] Kiem tra Apache Flume Agent dang chay o Terminal 1...")
    flume_ingested = False

    # Lắng nghe tối đa 15 giây xem Flume có xử lý file không
    for i in range(15):
        if os.path.exists(completed_log) or check_hdfs_files():
            flume_ingested = True
            break
        time.sleep(1)

    if flume_ingested:
        print(f"      [SUCCESS] Phat hien Apache Flume Agent da tu dong nap file vao HDFS!")
    else:
        print(f"      [WARN] Khong phat hien Flume Agent hoat dong tai Terminal 1 (bash flume/start_flume.sh).")
        print(f"             Dang tu dong fallback nap truc tiep de dam bao pipeline khong bi gian doan...")
        
        # Tìm file nguồn để nạp
        source_file = None
        if os.path.exists(target_log):
            source_file = target_log
        elif os.path.exists(completed_log):
            source_file = completed_log
        elif os.path.exists(OUTPUT_FILE):
            source_file = OUTPUT_FILE

        if not source_file:
            print(f"[ERROR] Khong tim thay file log tai {SPOOL_DIR}")
            sys.exit(1)

        run_command(["hdfs", "dfs", "-put", "-f", source_file, f"{HDFS_RAW_DIR}/access.log"])

    # 3. Xác nhận kết quả
    print(f"\n[3/3] [INFO] Du lieu da co san tren HDFS Raw Zone:")
    run_command(["hdfs", "dfs", "-ls", HDFS_RAW_DIR])

if __name__ == "__main__":
    main()
