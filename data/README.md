# Thư Mục `data/`

Quản lý việc sinh dữ liệu giả lập và lưu trữ các kết quả trung gian, báo cáo cuối cùng của pipeline.

## Các tệp tin

- **`generate_logs.py`**:
  - Script sinh dữ liệu giả lập 24 giờ hoạt động của microservices (~50,000 dòng log định dạng JSON).
  - Tự động cấy (inject) kịch bản bất thường vào khung giờ **15:00 - 16:00**:
    - Flood IP `10.0.0.99` gửi hàng ngàn request liên tục.
    - Ép tỉ lệ lỗi HTTP 500/503 và ERROR lên cao đột biến.
    - Đẩy thời gian phản hồi (response time) tăng vọt.
- **`fake_logs.json`** *(sinh ra khi chạy)*: Tập dữ liệu log thô ban đầu.
- **`mapreduce_results.txt`** *(sinh ra sau MapReduce)*: Kết quả thống kê đã gộp từ HDFS.
- **`anomaly_report.json`** *(sinh ra sau Anomaly Detector)*: Báo cáo kết quả phát hiện bất thường dạng JSON.
