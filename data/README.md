# Thư Mục `data/` — Sinh Dữ Liệu & Quản Lý Tập Dữ Liệu

> **Thành viên phụ trách (theo Action Plan):** Đặng Văn Vinh  
> **Nhiệm vụ:** *"Manages the continuous generation of web server logs, utilizing Flume to read and store the raw daily data into HDFS."*

---

## 1. Các Tệp Tin

- **`generate_logs.py`**:
  - Script mô phỏng máy chủ Web Server phát sinh nhật ký truy cập (Access Log) định dạng JSON.
  - Tự động cấy (inject) kịch bản bất thường vào khung giờ **15:00 - 16:00**:
    - IP Flood: `10.0.0.99` gửi hàng ngàn request liên tục.
    - Ép tỉ lệ lỗi HTTP 500/503 tăng đột biến (>40%).
    - Đẩy thời gian phản hồi (response time) tăng vọt (>4,000 ms).
  - **Hỗ trợ 2 chế độ vận hành:**
    1. **Chế độ Batch (Mặc định):** Sinh trọn vẹn 50,000 log của 24 giờ cho ngày hôm nay để nạp vào MapReduce xử lý theo lô.
       ```bash
       python data/generate_logs.py
       ```
    2. **Chế độ Continuous Stream:** Mô phỏng máy chủ đang hoạt động thực tế, liên tục nhả log thời gian thực ra file và màn hình.
       ```bash
       python data/generate_logs.py --continuous
       ```

---

## 2. Các File Dữ Liệu Được Sinh Ra Khi Chạy Pipeline

- **`data/spool/web_access_<LOG_DATE>.log`**: File log xuất ra cho Apache Flume Agent theo dõi và nạp lên HDFS.
- **`data/cleaned_logs.json`**: Dữ liệu sạch sau khi qua bộ lọc tiền xử lý của Nông Minh Trí & Triệu Văn Huy.
- **`data/mapreduce_results.txt`**: Kết quả tính toán phân tán sau khi chạy MapReduce của Dương Đình Hoàng.
- **`data/anomaly_report.json`**: Báo cáo tổng hợp các sự cố bất thường được phát hiện.
