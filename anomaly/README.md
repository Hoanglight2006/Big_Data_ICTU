# Thư Mục `anomaly/` — Phát Hiện Bất Thường Dựa Trên Quy Tắc (Rule-based Detection)

> **Thành viên phụ trách (theo Action Plan):** Dương Đình Hoàng (Leader)  
> **Nhiệm vụ:** *"Building the core anomaly detection rules, calculating statistical baselines and anomaly thresholds."*

---

## 1. Vai Trò & Phương Pháp Thống Kê

Đọc dữ liệu tổng hợp trực tiếp từ cụm Hadoop HDFS qua stream, không cần tải file trung gian về máy host, áp dụng các mô hình toán học thống kê nhằm phát hiện sự cố:
- **Baseline Thống Kê:** Tính toán giá trị trung bình ($\mu$) và độ lệch chuẩn ($\sigma$) của số lượng request theo từng khung giờ trong ngày.
- **Phát hiện đa chiều:** Kết hợp lưu lượng truy cập (Traffic), tỉ lệ lỗi máy chủ (5xx), độ trễ (Latency) và hành vi của từng địa chỉ IP.

---

## 2. Các Tệp Tin

- **`detector.py`**:
  - Nhận dữ liệu stream trực tiếp từ HDFS (`/data/output/<LOG_DATE>/part-*`) hoặc qua pipe `sys.stdin`.
  - Áp dụng 6 quy tắc phát hiện:
    1. **Rule 1 (Traffic Spike):** Lượng truy cập vượt ngưỡng `Mean + 2*Std` (Cảnh báo) hoặc `Mean + 3*Std` (Nghiêm trọng).
    2. **Rule 2 (High Error Rate):** Tỉ lệ log `ERROR` vượt quá 5%.
    3. **Rule 3 (High 5xx Rate):** Tỉ lệ mã lỗi máy chủ HTTP 500/503 vượt quá 5%.
    4. **Rule 4 (IP Flood / DDoS):** Một IP gửi vượt ngưỡng cho phép (1,000 requests/giờ).
    5. **Rule 5 (High Latency):** Thời gian phản hồi trung bình vượt quá 2,000 ms.
    6. **Rule 6 (Top Failing Endpoints):** Bóc tách danh sách các API gặp tỉ lệ lỗi cao nhất.
  - In bảng báo cáo trực quan lên màn hình terminal.
  - Lưu báo cáo cấu trúc JSON trực tiếp lên HDFS: `/data/reports/<LOG_DATE>/anomaly_report.json`.

---

## 3. Cách Sử Dụng

### Chạy trực tiếp từ HDFS:
```bash
python3 anomaly/detector.py --date 2026-10-01
```

### Hoặc nhận stream từ lệnh HDFS cat qua pipe:
```bash
hdfs dfs -cat /data/output/2026-10-01/part-* | python3 anomaly/detector.py --date 2026-10-01
```
