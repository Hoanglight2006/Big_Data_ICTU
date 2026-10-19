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

- **`train_kmeans_model.py` (Luồng 1 - Huấn luyện ngoại tuyến):**
  - Thu thập dữ liệu tổng hợp lịch sử từ nhiều ngày (HDFS `/data/output/*` hoặc local).
  - Áp dụng PySpark MLlib KMeans (`from pyspark.ml.clustering import KMeans`).
  - Đánh giá Silhouette score, xác định trọng tâm các cụm (`clusterCenters`).
  - Lưu mô hình trực tiếp lên HDFS: `/models/kmeans_anomaly_model`.

- **`kmeans_verifier.py` (Luồng 2 - Thẩm định lô hàng ngày):**
  - Nạp mô hình đã lưu từ HDFS (`KMeansModel.load`).
  - Thẩm định dữ liệu lô ngày hôm nay trong vài giây (không tốn thời gian train lại).
  - Loại bỏ các báo động giả (False Positives) của Rule, nâng cao độ chính xác của ứng dụng.

- **`detector.py`:**
  - Nhận dữ liệu trực tiếp từ HDFS (`/data/output/<LOG_DATE>/part-*`).
  - Áp dụng 6 quy tắc đánh giá để lọc thô các dấu hiệu nghi vấn ban đầu.
  - Phối hợp với `kmeans_verifier.py` để xuất báo cáo JSON hoàn chỉnh lên HDFS: `/data/reports/<LOG_DATE>/anomaly_report.json`.

---

## 3. Cách Sử Dụng

### 1. Huấn luyện mô hình KMeans phân tán trên cụm YARN (Luồng 1):
```bash
spark-submit --master yarn anomaly/train_kmeans_model.py
```
*(Hoặc chạy test cục bộ: `python3 anomaly/train_kmeans_model.py`)*

### 2. Vận hành thẩm định lô & xuất báo cáo (Luồng 2):
```bash
python3 anomaly/detector.py --date 2026-10-01
```

### Hoặc nhận dữ liệu từ lệnh HDFS cat:
```bash
hdfs dfs -cat /data/output/2026-10-01/part-* | python3 anomaly/detector.py --date 2026-10-01
```
