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
  - Áp dụng 6 quy tắc đánh giá để lọc thô các dấu hiệu nghi vấn ban đầu.
  - Phối hợp với `kmeans_verifier.py` để thẩm định và xuất báo cáo JSON lên HDFS: `/data/reports/<LOG_DATE>/anomaly_report.json`.

- **`kmeans_verifier.py`**:
  - Sử dụng thuật toán phân cụm KMeans từ thư viện Machine Learning (`sklearn.cluster.KMeans`, `StandardScaler`).
  - Phân tích đa chiều trên 24 khung giờ và các IP nghi vấn.
  - Đo khoảng cách tới tâm cụm bình thường để loại bỏ báo động giả (False Positives) của Rule, nâng cao độ chính xác của ứng dụng.

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
