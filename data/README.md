# Thư Mục `data/` — Sinh Dữ Liệu & Quản Lý Tập Dữ Liệu

> **Thành viên phụ trách (theo Action Plan):** Đặng Văn Vinh  
> **Nhiệm vụ:** *"Manages the continuous generation of web server logs, utilizing Flume to read and store the raw daily data into HDFS."*

---

## 1. Các Tệp Tin

- **`generate_logs.py`**:
  - Script mô phỏng máy chủ Web Server phát sinh nhật ký truy cập (Access Log) định dạng JSON.
  - Tự động sinh dữ liệu quy mô lớn (từ 150.000 đến 180.000+ bản ghi cho 24 giờ).
  - Tự động ngẫu nhiên hóa các kịch bản bất thường (không khóa cứng):
    - Tự động chọn ngẫu nhiên 1 đến 2 khung giờ trong ngày có lưu lượng tăng vọt (Spike).
    - Ngẫu nhiên sinh địa chỉ IP gửi yêu cầu quá tải (Spam / DDoS) vượt ngưỡng cho phép.
    - Ép tỉ lệ lỗi HTTP 500/503 tăng cao và độ trễ phản hồi bị kéo dài.
    - Chèn ngẫu nhiên khoảng 1% dữ liệu rác/hỏng (JSON lỗi cú pháp, IP sai định dạng, thiếu trường dữ liệu) để kiểm thử bộ lọc làm sạch của tầng tiền xử lý.
  - **Hỗ trợ 2 chế độ vận hành:**
    1. **Chế độ Batch (Mặc định):** Sinh trọn vẹn tập dữ liệu lớn của 24 giờ để nạp vào MapReduce xử lý theo lô.
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
