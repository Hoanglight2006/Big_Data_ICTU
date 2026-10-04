# =============================================================================
# config/settings.py — Cấu hình trung tâm cho toàn bộ project
# =============================================================================
import os
import glob

from datetime import datetime

# --- Ngày phân tích (Mặc định lấy ngày hôm nay YYYY-MM-DD) ---
TODAY_STR = datetime.now().strftime("%Y-%m-%d")
LOG_DATE = os.getenv("LOG_DATE", TODAY_STR)

# --- HDFS & Hadoop Cluster Paths ---
DEFAULT_HADOOP = os.path.expanduser("~/hadoop-3.2.1")
HADOOP_HOME = os.getenv("HADOOP_HOME", DEFAULT_HADOOP)

# Cấu trúc lưu trữ phân vùng theo ngày trên HDFS (Không tải file về máy)
HDFS_RAW_DIR = os.getenv("HDFS_RAW_DIR", f"/data/raw/{LOG_DATE}")             # Log thô từ Flume
HDFS_CLEANED_DIR = os.getenv("HDFS_CLEANED_DIR", f"/data/cleaned/{LOG_DATE}") # Log sạch sau khi YARN tiền xử lý
HDFS_OUTPUT_DIR = os.getenv("HDFS_OUTPUT_DIR", f"/data/output/{LOG_DATE}")   # Kết quả MapReduce tổng hợp
HDFS_REPORT_DIR = os.getenv("HDFS_REPORT_DIR", f"/data/reports/{LOG_DATE}")   # Báo cáo bất thường trên HDFS

# Tương thích ngược
HDFS_INPUT_DIR = HDFS_RAW_DIR

# Đường dẫn thư mục Spool cho Flume
SPOOL_DIR = os.getenv("SPOOL_DIR", "data/spool")

# Tự động tìm kiếm file Hadoop Streaming JAR
DEFAULT_JAR = os.path.join(HADOOP_HOME, "share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar")
found_jars = glob.glob(os.path.join(HADOOP_HOME, "share/hadoop/tools/lib/hadoop-streaming*.jar"))
HADOOP_STREAMING_JAR = os.getenv("HADOOP_STREAMING_JAR", found_jars[0] if found_jars else DEFAULT_JAR)

# --- Fake Log Generation ---
TOTAL_LOGS = int(os.getenv("TOTAL_LOGS", "150000"))      # Tổng số log sinh ra trong 24h (mặc định 150.000)
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

# --- Anomaly Injection (Nếu không cấu hình, generate_logs.py sẽ tự động random) ---
ANOMALY_START_HOUR = int(os.getenv("ANOMALY_START_HOUR", "-1"))  # -1 nghĩa là tự động random khung giờ
ANOMALY_END_HOUR = int(os.getenv("ANOMALY_END_HOUR", "-1"))
ANOMALY_IP = os.getenv("ANOMALY_IP", "")                         # Để trống nghĩa là tự động random IP tấn công

# --- MapReduce Output ---
MAPREDUCE_OUTPUT_FILE = "data/mapreduce_results.txt"

# --- Anomaly Detection Thresholds ---
THRESHOLD_WARNING_STD = 2.0   # WARNING: vượt mean + 2*std
THRESHOLD_CRITICAL_STD = 3.0  # CRITICAL: vượt mean + 3*std
MAX_5XX_RATE_PERCENT = 5.0    # Tỉ lệ 5xx tối đa cho phép (%)
MAX_IP_REQUESTS_PER_HOUR = 1000 # Số request tối đa từ 1 IP mỗi giờ
MAX_RESPONSE_TIME_MS = 2000   # Response time tối đa (ms)
