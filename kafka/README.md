# Thư Mục `kafka/`

Đóng vai trò là **Kafka Consumer & Tầng Micro-batching**, kết nối giữa luồng Streaming thời gian thực và hệ thống lưu trữ phân tán HDFS.

## Các tệp tin

- **`consumer.py`**:
  - Đăng ký (subscribe) vào Kafka Topic `app-logs` theo Consumer Group `log-processor-group`.
  - Tiếp nhận các dòng log, gom đủ số lượng `BATCH_SIZE = 1000` dòng.
  - Ghi tạm ra file local và tự động gọi lệnh `hdfs dfs -put` để đẩy file lên thư mục `/data/logs/` trên HDFS.
  - Tự động dọn dẹp file tạm local sau khi nạp HDFS thành công.
