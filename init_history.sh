#!/bin/bash
# =============================================================================
# init_history.sh — Khởi tạo dữ liệu lịch sử nhiều ngày (200MB - 300MB) trên HDFS
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [ -d "venv" ]; then
    source venv/bin/activate
fi

# Số ngày cần tạo lịch sử (mặc định 7 ngày)
NUM_DAYS="${1:-7}"
# Số bản ghi log mỗi ngày (200.000 logs ~ 32MB/ngày -> 7 ngày ~ 225MB, 10 ngày ~ 320MB)
LOGS_PER_DAY="${2:-200000}"

export HADOOP_HOME="${HADOOP_HOME:-$HOME/hadoop-3.2.1}"

echo "=========================================================="
echo " KHỞI TẠO TẬP DỮ LIỆU LỊCH SỬ NHIỀU NGÀY CHO HỆ THỐNG"
echo " Số ngày khởi tạo   : $NUM_DAYS ngày"
echo " Số log mỗi ngày    : $LOGS_PER_DAY bản ghi (~32 MB/ngày)"
echo " Dự kiến tổng log   : $(( NUM_DAYS * LOGS_PER_DAY )) bản ghi (~$(( NUM_DAYS * 32 )) MB)"
echo " Hadoop Home        : $HADOOP_HOME"
echo "=========================================================="

# 1. Kiểm tra NameNode và Safe Mode
hdfs dfsadmin -safemode leave 2>/dev/null || true

# 2. Sinh danh sách ngày lịch sử lùi từ hôm qua
DATES=()
for (( i=NUM_DAYS; i>=1; i-- )); do
    # Hỗ trợ cả Linux và MacOS date
    if date -d "$i days ago" +%F &>/dev/null; then
        DATES+=( $(date -d "$i days ago" +%F) )
    else
        DATES+=( $(date -v-${i}d +%F) )
    fi
done

echo -e "\nDanh sách $NUM_DAYS ngày sẽ xử lý:"
for d in "${DATES[@]}"; do
    echo "  - $d"
done
echo "----------------------------------------------------------"

COUNT=1
for TARGET_DATE in "${DATES[@]}"; do
    echo -e "\n=========================================================="
    echo " [$COUNT/$NUM_DAYS] BẮT ĐẦU XỬ LÝ LÔ DỮ LIỆU NGÀY: $TARGET_DATE"
    echo "=========================================================="

    export LOG_DATE="$TARGET_DATE"

    # Bước 1: Sinh log vào thư mục Spool của Flume
    echo "  [1/4] Sinh $LOGS_PER_DAY bản ghi log vào data/spool/..."
    python3 data/generate_logs.py --date "$TARGET_DATE" --logs "$LOGS_PER_DAY"

    # Bước 2: Chờ Flume Agent nạp vào HDFS Raw
    echo "  [2/4] Chờ Flume Agent (Terminal 1) nạp vào HDFS /data/raw/$TARGET_DATE..."
    python3 flume/ingest_to_hdfs.py

    # Bước 3: Tiền xử lý làm sạch Map-only trên YARN
    echo "  [3/4] Tiền xử lý làm sạch trên YARN (/data/cleaned/$TARGET_DATE)..."
    bash preprocessing/run_cleaner.sh "$TARGET_DATE"

    # Bước 4: Tổng hợp MapReduce trên YARN
    echo "  [4/4] Tổng hợp chỉ số MapReduce trên YARN (/data/output/$TARGET_DATE)..."
    bash mapreduce/run_job.sh "$TARGET_DATE"

    COUNT=$(( COUNT + 1 ))
done

echo -e "\n=========================================================="
echo " HOÀN TẤT KHỞI TẠO DỮ LIỆU LỊCH SỬ TRÊN HDFS!"
echo " Tổng cộng: $NUM_DAYS ngày (~$(( NUM_DAYS * 32 )) MB log thô)"
echo " Kết quả tổng hợp đã sẵn sàng tại các thư mục HDFS:"
hdfs dfs -ls -d /data/output/*
echo "=========================================================="
echo ""
echo ">> BƯỚC TIẾP THEO: Chạy lệnh sau để huấn luyện mô hình KMeans (Luồng 1):"
echo "   python3 anomaly/train_kmeans_model.py"
echo "=========================================================="
