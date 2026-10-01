# Thư Mục `config/`

Chứa toàn bộ các tham số cấu hình dùng chung cho toàn bộ dự án, giúp quản lý tập trung và tránh hard-code.

## Các tệp tin

- **`settings.py`**:
  - **Kafka Config:** Broker (`localhost:9092`), Topic (`app-logs`), Partitions (`3`).
  - **Replay Config:** `SCALE_FACTOR = 288` (tua nhanh 24h dữ liệu phát trong ~5 phút).
  - **HDFS & Hadoop Config:** Đường dẫn HDFS Input (`/data/logs`), Output (`/data/output`), Batch size (`1000`), Hadoop Streaming JAR.
  - **Anomaly Thresholds:** Ngưỡng cảnh báo thống kê (`Mean + 2*Std` và `Mean + 3*Std`), tỉ lệ lỗi tối đa (`5%`), giới hạn request từ 1 IP (`1000 req/h`).
