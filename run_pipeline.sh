#!/bin/bash
# =============================================================================
# run_pipeline.sh — Pipeline phan tich log batch theo ngay
#
# De tai: Log Analysis and Anomaly Detection for Web Servers
# Chuong trinh: Samsung Innovation Campus (SIC) - Group 3
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [ -d "venv" ]; then
    source venv/bin/activate
fi

export LOG_DATE="${1:-${LOG_DATE:-$(date +%F)}}"
export HADOOP_HOME="${HADOOP_HOME:-$HOME/hadoop-3.2.1}"

echo "=========================================================="
echo " [INFO] BAT DAU PIPELINE PHAN TICH LOG"
echo " Ngay phan tich: $LOG_DATE"
echo " Hadoop Home   : $HADOOP_HOME"
echo " Thu muc chay  : $SCRIPT_DIR"
echo "=========================================================="

# -----------------------------------------------------------------------------
# BUOC 1: KIEM TRA & KHOI DONG DICH VU HADOOP CLUSTER
# -----------------------------------------------------------------------------
echo -e "\n[1/6] Kiem tra dich vu Hadoop (HDFS & YARN)..."

if hdfs dfs -ls / &> /dev/null; then
    echo "  [INFO] Cum Hadoop (HDFS) dang hoat dong."
    hdfs dfsadmin -safemode leave 2>/dev/null || true
else
    CURRENT_USER=$(whoami)
    echo "  [WARN] Chua ket noi duoc HDFS. Khoi dong Hadoop voi user: $CURRENT_USER..."
    if [ -f "${HADOOP_HOME}/sbin/start-all.sh" ]; then
        export HDFS_NAMENODE_USER=$CURRENT_USER
        export HDFS_DATANODE_USER=$CURRENT_USER
        export HDFS_SECONDARYNAMENODE_USER=$CURRENT_USER
        export YARN_RESOURCEMANAGER_USER=$CURRENT_USER
        export YARN_NODEMANAGER_USER=$CURRENT_USER
        "${HADOOP_HOME}/sbin/start-all.sh" || true
        
        echo "  [INFO] Cho cac tien trinh Hadoop khoi dong on dinh..."
        for i in {1..15}; do
            if hdfs dfs -ls / &> /dev/null; then
                echo "  [INFO] Hadoop da khoi dong thanh cong."
                break
            fi
            sleep 2
        done
    fi
fi

# Kiem tra dich vu YARN (ResourceManager)
if ! yarn node -list &> /dev/null; then
    echo "  [WARN] YARN ResourceManager chua chay. Dang khoi dong YARN..."
    CURRENT_USER=$(whoami)
    export YARN_RESOURCEMANAGER_USER=$CURRENT_USER
    export YARN_NODEMANAGER_USER=$CURRENT_USER
    if [ -f "${HADOOP_HOME}/sbin/start-yarn.sh" ]; then
        "${HADOOP_HOME}/sbin/start-yarn.sh" || true
        sleep 5
    fi
else
    echo "  [INFO] Dich vu YARN (ResourceManager) dang hoat dong."
fi

# Khoi dong JobHistoryServer (tranh loi client khong ket noi duoc port 10020 sau khi job ket thuc)
if [ -f "${HADOOP_HOME}/sbin/mr-jobhistory-daemon.sh" ]; then
    "${HADOOP_HOME}/sbin/mr-jobhistory-daemon.sh" start historyserver 2>/dev/null || true
fi

# -----------------------------------------------------------------------------
# BUOC 2: SINH DU LIEU LOG
# -----------------------------------------------------------------------------
echo -e "\n[2/6] Sinh du lieu log web server cho ngay $LOG_DATE..."
python3 data/generate_logs.py

# -----------------------------------------------------------------------------
# BUOC 3: KIEM TRA FLUME AGENT NAP DU LIEU VAO HDFS RAW ZONE
# -----------------------------------------------------------------------------
echo -e "\n[3/6] Cho Apache Flume Agent nap log vao HDFS (/data/raw/$LOG_DATE)..."
FLUME_INGESTED=false
for i in {1..30}; do
    if hdfs dfs -test -e "/data/raw/$LOG_DATE" 2>/dev/null && [ $(hdfs dfs -ls "/data/raw/$LOG_DATE" 2>/dev/null | grep -c "\.log") -gt 0 ]; then
        FLUME_INGESTED=true
        break
    fi
    sleep 1
done

if [ "$FLUME_INGESTED" = false ]; then
    echo "  [ERROR] Flume chua nap du lieu vao /data/raw/$LOG_DATE!"
    echo "          Hay dam bao Apache Flume Agent dang chay o Terminal 1:"
    echo "          flume-ng agent --conf flume/ --conf-file flume/flume-hdfs.conf --name a1 -Dflume.root.logger=INFO,console"
    exit 1
fi
echo "  [INFO] Flume Agent da nap du lieu vao HDFS thanh cong:"
hdfs dfs -ls "/data/raw/$LOG_DATE"

# -----------------------------------------------------------------------------
# BUOC 4: TIEN XU LY & LAM SACH PHAN TAN TREN YARN
# -----------------------------------------------------------------------------
echo -e "\n[4/6] Preprocessing Map-only Job tren YARN (/data/cleaned/$LOG_DATE)..."
bash preprocessing/run_cleaner.sh "$LOG_DATE"

# -----------------------------------------------------------------------------
# BUOC 5: TONG HOP MAPREDUCE TREN YARN
# -----------------------------------------------------------------------------
echo -e "\n[5/6] Thuc thi Hadoop MapReduce Streaming tren YARN (/data/output/$LOG_DATE)..."
bash mapreduce/run_job.sh "$LOG_DATE"

# -----------------------------------------------------------------------------
# BUOC 6: PHAN TICH BAT THUONG & LUU BAO CAO HDFS
# -----------------------------------------------------------------------------
echo -e "\n[6/6] Phat hien bat thuong truc tiep tu HDFS stream..."
python3 anomaly/detector.py --date "$LOG_DATE"

echo "=========================================================="
echo " [INFO] HOAN TAT TOAN BO PIPELINE PHAN TICH LOG (YARN & HDFS)"
echo " Cụm dữ liệu HDFS theo ngày $LOG_DATE:"
echo "   - Raw Data    : /data/raw/$LOG_DATE"
echo "   - Cleaned Data: /data/cleaned/$LOG_DATE"
echo "   - Aggregation : /data/output/$LOG_DATE"
echo "   - Report      : /data/reports/$LOG_DATE"
echo "=========================================================="
