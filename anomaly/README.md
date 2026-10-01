# Thư Mục `anomaly/`

Chứa module **Rule-based Anomaly Detection**, chịu trách nhiệm phân tích thống kê và đưa ra cảnh báo bất thường dựa trên kết quả MapReduce.

## Các tệp tin

- **`detector.py`**:
  - Đọc file kết quả tổng hợp `data/mapreduce_results.txt`.
  - Tính toán baseline thống kê: Giá trị trung bình (Mean) và Độ lệch chuẩn (Standard Deviation) cho lượng truy cập theo giờ.
  - Áp dụng 6 quy tắc (Rules) phát hiện sự cố:
    1. **Rule 1 (Traffic Spike):** Lượng truy cập vượt ngưỡng `Mean + 2*Std` (Cảnh báo) hoặc `Mean + 3*Std` (Nghiêm trọng).
    2. **Rule 2 (High Error Rate):** Tỉ lệ log `ERROR` vượt quá 5%.
    3. **Rule 3 (High 5xx Rate):** Tỉ lệ mã lỗi máy chủ `5xx` vượt quá 5%.
    4. **Rule 4 (IP Flood / DDoS):** Một IP gửi quá 1,000 requests trong một khung giờ.
    5. **Rule 5 (High Latency):** Thời gian phản hồi trung bình vượt quá 2,000 ms.
    6. **Rule 6 (Top Failing Endpoints):** Liệt kê các endpoint chịu số lượng lỗi lớn nhất.
  - In bảng báo cáo trực quan lên màn hình và lưu cấu trúc JSON tại `data/anomaly_report.json`.
