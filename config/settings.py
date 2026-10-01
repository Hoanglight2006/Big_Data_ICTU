# =============================================================================
# config/settings.py — Cấu hình trung tâm cho toàn bộ project
# =============================================================================
import os

# --- Kafka ---
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "app-logs"
KAFKA_PARTITIONS = 3
KAFKA_REPLICATION_FACTOR = 1

# --- Replay Simulator ---
# scale_factor = 12 → 24h dữ liệu phát trong 2h thực tế
# scale_factor = 24 → 24h dữ liệu phát trong 1h thực tế
# scale_factor = 144 → 24h dữ liệu phát trong 10 phút (chỉ để test nhanh)
SCALE_FACTOR = 5000
MAX_SLEEP_SECONDS = 0.5

# --- HDFS & Hadoop ---
HADOOP_HOME = os.environ.get("HADOOP_HOME", "/home/hoang/hadoop-3.2.1")
HDFS_INPUT_DIR  = "/data/logs"           # Thư mục chứa batch log trên HDFS
HDFS_OUTPUT_DIR = "/data/output"         # Thư mục kết quả MapReduce trên HDFS
LOCAL_BATCH_DIR = "/tmp/log_batches"     # Thư mục tạm local trước khi đẩy HDFS
BATCH_SIZE = 200                         # 200 msgs/batch để có ~10 batches cho 2000 logs

# Đường dẫn Hadoop Streaming JAR
HADOOP_STREAMING_JAR = os.path.join(
    HADOOP_HOME,
    "share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar"
)

# --- Fake Log ---
LOG_DATE = "2024-01-15"          # Ngày giả lập
TOTAL_LOGS = 2000                # Tổng số log test nhanh (2000 dòng)
OUTPUT_FILE = "data/fake_logs.json"

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

# IP bị inject flood
ANOMALY_IP = "10.0.0.99"

# --- MapReduce Output ---
MAPREDUCE_OUTPUT_FILE = "data/mapreduce_results.json"

# --- Anomaly Detection Thresholds ---
# N x std deviation để coi là bất thường
THRESHOLD_WARNING_STD = 2.0   # WARNING: vượt mean + 2*std
THRESHOLD_CRITICAL_STD = 3.0  # CRITICAL: vượt mean + 3*std

# Tỉ lệ 5xx tối đa cho phép (%)
MAX_5XX_RATE_PERCENT = 5.0

# Số request tối đa từ 1 IP mỗi giờ (đối với test 2000 logs)
MAX_IP_REQUESTS_PER_HOUR = 50

# Response time tối đa (ms)
MAX_RESPONSE_TIME_MS = 2000
