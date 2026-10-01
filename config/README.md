# Thư Mục `config/` — Quản Lý Cấu Hình Tập Trung

Chứa toàn bộ các tham số cấu hình dùng chung cho toàn bộ dự án, hỗ trợ đọc linh hoạt từ biến môi trường (`os.getenv`) để chạy được trên mọi môi trường (máy cá nhân, Docker, máy ảo Ubuntu).

---

## Các Tệp Tin

- **`settings.py`**:
  - **Ngày phân tích (`LOG_DATE`):** Tự động nhận diện ngày hiện tại (`YYYY-MM-DD`).
  - **HDFS & Hadoop Config:** Đường dẫn HDFS Input phân vùng theo ngày (`/data/logs/<LOG_DATE>`), Output (`/data/output/<LOG_DATE>`), tự động phát hiện `hadoop-streaming*.jar`.
  - **Flume Spool Config:** Thư mục đệm `data/spool` cho Apache Flume Agent.
  - **Anomaly Thresholds:** 
    - Ngưỡng cảnh báo thống kê Traffic Spike: `Mean + 2*Std` (Warning) và `Mean + 3*Std` (Critical).
    - Tỉ lệ lỗi máy chủ tối đa: `5%` cho HTTP 5xx.
    - Giới hạn request tối đa từ 1 IP: `1,000 reqs/h`.
    - Ngưỡng độ trễ tối đa: `2,000 ms`.
