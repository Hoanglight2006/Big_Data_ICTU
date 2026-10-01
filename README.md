# Hệ Thống Phân Tích Nhật Ký Web Server & Phát Hiện Bất Thường (Big Data)

> **Khóa học:** Samsung Innovation Campus (SIC) - Big Data Course  
> **Nhóm thực hiện:** Group 3  
> **Thành viên:** 
> - **Dương Đình Hoàng (Leader):** Kiến trúc hệ thống, Hadoop MapReduce & Bộ luật Anomaly Detection.
> - **Đặng Văn Vinh:** Sinh dữ liệu web logs, cấu hình Apache Flume Ingestion nạp HDFS.
> - **Nông Minh Trí & Triệu Văn Huy:** Quản lý cấu trúc lưu trữ HDFS phân vùng theo ngày, tiền xử lý dữ liệu.
> - **Lý Việt Hoàng:** Kiểm thử hệ thống, tổng hợp kết quả, viết báo cáo & thiết kế Slide.

---

## 1. Kiến Trúc Luồng Dữ Liệu Hoàn Chỉnh (Daily Batch Architecture)

Hệ thống hoạt động theo mô hình **Batch Processing ngoại tuyến (Offline Log Analytics)** để phân tích toàn diện 24 giờ dữ liệu của ngày hôm trước:

```text
[Web Server Logs / Generator] (Đặng Văn Vinh)
       │ (Sinh log liên tục hoặc 50,000 logs/24h)
       ▼
 [Apache Flume Agent] (Đặng Văn Vinh)
       │ (Tự động nạp và phân vùng động theo ngày)
       ▼
[Hadoop HDFS Storage] (/data/logs/YYYY-MM-DD/access.log)
       │
       ▼
[Data Preprocessing & Cleaning] (Nông Minh Trí & Triệu Văn Huy)
       │ (Lọc log hỏng, kiểm tra IPv4, chuẩn hóa dữ liệu sạch)
       ▼
[Hadoop Streaming MapReduce] (Dương Đình Hoàng - Leader)
       │ (Tính toán phân tán trên YARN Cluster)
       ▼
 [Thống Kê Tổng Hợp] (data/mapreduce_results.txt)
       │
       ▼
[Rule-based Anomaly Detector] (Dương Đình Hoàng - Leader) ──► [Daily Anomaly Report]
```

---

## 2. Cấu Trúc Thư Mục Dự Án

```text
log-simulation/
├── config/             # Cấu hình trung tâm (HDFS paths, thresholds, dynamic JARs)
├── data/               # Module sinh log web server (Đặng Văn Vinh)
├── flume/              # Cấu hình Apache Flume Agent nạp HDFS & Ingestion Runner
├── preprocessing/      # Tiền xử lý, lọc rác & làm sạch dữ liệu (Nông Minh Trí & Triệu Văn Huy)
├── mapreduce/          # Hadoop Streaming Mapper & Reducer (Dương Đình Hoàng)
├── anomaly/            # Bộ lọc 6 quy tắc phát hiện bất thường & báo cáo (Dương Đình Hoàng)
├── Dockerfile          # Docker image đóng gói ứng dụng pipeline & Hadoop client
├── docker-compose.yml  # Triển khai trọn gói cụm Hadoop (HDFS, YARN) & Pipeline Runner
├── hadoop.env          # Biến môi trường chuẩn cho cụm Hadoop
├── run_pipeline.sh     # Script chạy tự động 1-chạm kết nối toàn bộ luồng 5 thành viên
└── README.md           # Hướng dẫn chi tiết
```

---

## 3. Hướng Dẫn Vận Hành

### Cách 1: Chạy Siêu Tốc Bằng Docker Compose (Khuyên Dùng — Chạy Được Trên Mọi Máy)

Không cần cài đặt thủ công Java hay Hadoop, chỉ cần máy tính đã cài **Docker Desktop**:

```bash
# 1. Khởi động toàn bộ cụm Hadoop và Pipeline runner
docker compose up -d

# 2. Xem tiến trình chạy tự động
docker logs -f log-pipeline-runner

# 3. Xem giao diện Web UI:
# - Hadoop HDFS NameNode: http://localhost:9870
# - YARN Resource Manager: http://localhost:8088
```

Kết quả báo cáo `data/anomaly_report.json` và `data/mapreduce_results.txt` sẽ tự động xuất hiện ngay trên máy của bạn (thông qua Docker volume).

---

### Cách 2: Chạy Trực Tiếp Trên Máy Ảo Ubuntu / Linux Có Sẵn Hadoop

#### Chạy tự động 1 lệnh duy nhất:
```bash
bash run_pipeline.sh 2024-01-15
```

#### Hoặc chạy thủ công từng bước:
1. **Sinh dữ liệu log ngày hôm trước (kèm kịch bản tấn công lúc 15:00):**
   ```bash
   python3 data/generate_logs.py
   ```
2. **Nạp dữ liệu vào HDFS phân vùng theo ngày:**
   ```bash
   python3 flume/ingest_to_hdfs.py
   ```
3. **Thực thi Hadoop MapReduce Streaming:**
   ```bash
   bash mapreduce/run_job.sh 2024-01-15
   ```
4. **Phân tích và xuất báo cáo bất thường:**
   ```bash
   python3 anomaly/detector.py
   ```

---

## 4. Kịch Bản Bất Thường Được Phát Hiện (6 Rules)

Hệ thống tự động phát hiện chính xác các dấu hiệu bất thường đã được cài cắm:
1. **Rule 1 (Traffic Spike):** Giờ 15:00 tăng vọt số lượng request (vượt ngưỡng $\mu + 2\sigma$ hoặc $\mu + 3\sigma$).
2. **Rule 2 (High Error Rate):** Tỉ lệ log `ERROR` lúc 15:00 vượt quá 5%.
3. **Rule 3 (High 5xx Rate):** Tỉ lệ lỗi máy chủ HTTP 500/503 lúc 15:00 chiếm hơn 40%.
4. **Rule 4 (IP Flood / DDoS):** IP `10.0.0.99` gửi hàng ngàn request trong 1 khung giờ.
5. **Rule 5 (High Latency):** Thời gian phản hồi trung bình giờ 15:00 vọt lên > 4,000 ms.
6. **Rule 6 (Top Failing Endpoints):** Trích xuất danh sách các endpoint chịu tỉ lệ lỗi lớn nhất (ví dụ `/api/payments/process`).
