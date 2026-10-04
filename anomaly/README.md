# Thư Mục `anomaly/` — Phát Hiện Dấu Hiệu Bất Thường

> **Thành viên phụ trách:** Dương Đình Hoàng 
> **Nhiệm vụ:** Xây dựng các quy tắc tự động phát hiện dấu hiệu bất thường, tính toán ngưỡng cảnh báo và xuất báo cáo.

---

## 1. Vai Trò & Phương Pháp Đánh Giá

Đọc kết quả tổng hợp trực tiếp từ HDFS theo luồng (stream), không cần tải file về máy tính cá nhân, áp dụng các quy tắc để phát hiện sự cố:
- **Ngưỡng so sánh:** Tính lượng truy cập trung bình mỗi giờ và độ lệch để xác định mức bình thường của hệ thống.
- **Đánh giá đa chiều:** Kết hợp theo dõi lượng truy cập, số lượng log lỗi, lỗi từ phía máy chủ (mã 500, 503), tốc độ phản hồi và hành vi gửi yêu cầu của từng địa chỉ IP.

---

## 2. Các Tệp Tin

- **`detector.py`**:
  - Nhận dữ liệu trực tiếp từ HDFS (`/data/output/<LOG_DATE>/part-*`) hoặc qua cổng đầu vào `sys.stdin`.
  - Áp dụng 6 quy tắc đánh giá:
    1. **Quy tắc 1 (Lượng truy cập tăng vọt):** Số lượng yêu cầu trong một giờ cao hơn nhiều so với mức trung bình.
    2. **Quy tắc 2 (Tỉ lệ log lỗi cao):** Số log báo lỗi (`ERROR`) chiếm hơn 5% lượng truy cập trong giờ.
    3. **Quy tắc 3 (Tỉ lệ lỗi máy chủ cao):** Các mã lỗi 500/503 từ máy chủ chiếm hơn 5% tổng truy cập.
    4. **Quy tắc 4 (Spam / Quá tải theo IP):** Một địa chỉ IP gửi liên tục trên 1.000 yêu cầu trong 1 giờ.
    5. **Quy tắc 5 (Hệ thống xử lý chậm):** Thời gian phản hồi trung bình trong một giờ vượt quá 2 giây (2.000 ms).
    6. **Quy tắc 6 (Đường dẫn gặp lỗi nhiều nhất):** Liệt kê các đường dẫn chức năng bị lỗi nhiều lần nhất.
  - In bảng tổng kết rõ ràng, dễ đọc ra màn hình terminal.
  - Tự động lưu file báo cáo JSON lên HDFS: `/data/reports/<LOG_DATE>/anomaly_report.json`.

---

## 3. Cách Sử Dụng

### Chạy trực tiếp từ HDFS:
```bash
python3 anomaly/detector.py --date 2026-10-01
```

### Hoặc nhận dữ liệu từ lệnh HDFS cat:
```bash
hdfs dfs -cat /data/output/2026-10-01/part-* | python3 anomaly/detector.py --date 2026-10-01
```
