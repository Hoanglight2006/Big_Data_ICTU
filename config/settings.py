# =============================================================================
# config/settings.py — Cấu hình trung tâm cho toàn bộ project
# =============================================================================
import os
import glob

from datetime import datetime

# --- Ngày phân tích (Mặc định lấy ngày hôm nay YYYY-MM-DD) ---
TODAY_STR = datetime.now().strftime("%Y-%m-%d")
LOG_DATE = os.getenv("LOG_DATE", TODAY_STR)

# --- HDFS & Hadoop ---
HADOOP_HOME = os.getenv("HADOOP_HOME", "/home/hoang/hadoop-3.2.1")
HDFS_BASE_DIR = os.getenv("HDFS_BASE_DIR", "/data/logs")
HDFS_INPUT_DIR = f"{HDFS_BASE_DIR}/{LOG_DATE}"         # Thư mục HDFS theo ngày
HDFS_OUTPUT_BASE_DIR = os.getenv("HDFS_OUTPUT_BASE_DIR", "/data/output")
HDFS_OUTPUT_DIR = f"{HDFS_OUTPUT_BASE_DIR}/{LOG_DATE}" # Thư mục kết quả MapReduce theo ngày

# Đường dẫn thư mục Spool cho Flume
SPOOL_DIR = os.getenv("SPOOL_DIR", "data/spool")

# Tự động tìm kiếm file Hadoop Streaming JAR
DEFAULT_JAR = os.path.join(HADOOP_HOME, "share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar")
found_jars = glob.glob(os.path.join(HADOOP_HOME, "share/hadoop/tools/lib/hadoop-streaming*.jar"))
HADOOP_STREAMING_JAR = os.getenv("HADOOP_STREAMING_JAR", found_jars[0] if found_jars else DEFAULT_JAR)

# --- Fake Log Generation ---
TOTAL_LOGS = int(os.getenv("TOTAL_LOGS", "50000"))      # Tổng số log sinh ra trong 24h
OUTPUT_FILE = os.path.join(SPOOL_DIR, f"web_access_{LOG_DATE}.log")
LEGACY_OUTPUT_FILE = "data/fake_logs.json"

# Danh sách service giả lập
SERVICES = ["api-gateway", "auth-service", "user-service",
            "product-service", "order-service", "payment-service"]

# Danh sách endpoint theo service
ENDPOINTS = [
    "/api/users", "/api/users/login", "/api/users/logout",
    "/api/products", "/api/products/search",
    "/api/orders", "/api/orders/create",
    "/api/payments/process", "/api/payments/verify",
    "/health", "/metrics",
]

HTTP_METHODS = ["GET", "POST", "PUT", "DELETE"]

# Phân phối status code (xác suất)
STATUS_CODES = {
    200: 0.70,   # 70% thành công
    201: 0.08,   # 8% created
    400: 0.08,   # 8% bad request
    401: 0.04,   # 4% unauthorized
    404: 0.04,   # 4% not found
    500: 0.04,   # 4% server error
    503: 0.02,   # 2% service unavailable
}

# --- Anomaly Injection ---
# Khoảng thời gian inject bất thường (giờ trong ngày)
ANOMALY_START_HOUR = 15    # 15:00
ANOMALY_END_HOUR = 16      # 16:00

# IP bị inject flood (DDoS kịch bản)
ANOMALY_IP = "10.0.0.99"

# --- MapReduce Output ---
MAPREDUCE_OUTPUT_FILE = "data/mapreduce_results.txt"

# --- Anomaly Detection Thresholds ---
THRESHOLD_WARNING_STD = 2.0   # WARNING: vượt mean + 2*std
THRESHOLD_CRITICAL_STD = 3.0  # CRITICAL: vượt mean + 3*std
MAX_5XX_RATE_PERCENT = 5.0    # Tỉ lệ 5xx tối đa cho phép (%)
MAX_IP_REQUESTS_PER_HOUR = 1000 # Số request tối đa từ 1 IP mỗi giờ
MAX_RESPONSE_TIME_MS = 2000   # Response time tối đa (ms)
