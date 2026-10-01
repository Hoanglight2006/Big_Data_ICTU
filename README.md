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
       │ (Sinh 50,000 logs/24h)
       ▼
 [Apache Flume Ingestion] (Đặng Văn Vinh)
       │ (Nạp trực tiếp vào HDFS Raw Zone)
       ▼
[HDFS Raw Zone] (/data/raw/YYYY-MM-DD/access.log)
       │
       ▼
[YARN Preprocessing Job] (Nông Minh Trí & Triệu Văn Huy)
       │ (Map-only Streaming lọc rác & chuẩn hóa trên YARN)
       ▼
[HDFS Cleaned Zone] (/data/cleaned/YYYY-MM-DD/part-*)
       │
       ▼
[YARN MapReduce Aggregation] (Dương Đình Hoàng - Leader)
       │ (Tính toán phân tán trên cụm máy chủ YARN)
       ▼
[HDFS Output Zone] (/data/output/YYYY-MM-DD/part-*)
       │
       ▼
[Rule-based Anomaly Detector] (Dương Đình Hoàng - Leader)
       │ (Phân tích trực tiếp từ HDFS stream)
       ▼
[HDFS Reports Zone] (/data/reports/YYYY-MM-DD/anomaly_report.json)
```

---

## 2. Cấu Trúc Thư Mục Dự Án

```text
log-simulation/
├── config/             # Cấu hình trung tâm (HDFS paths, thresholds, dynamic JARs)
├── data/               # Module sinh log web server (Đặng Văn Vinh)
├── flume/              # Cấu hình Apache Flume Agent nạp HDFS & Ingestion Runner
├── preprocessing/      # Tiền xử lý phân tán Map-only trên YARN (Nông Minh Trí & Triệu Văn Huy)
├── mapreduce/          # Hadoop Streaming Mapper & Reducer trên YARN (Dương Đình Hoàng)
├── anomaly/            # Bộ lọc 6 quy tắc phát hiện bất thường & xuất báo cáo HDFS (Dương Đình Hoàng)
├── Dockerfile          # Docker image đóng gói ứng dụng pipeline & Hadoop client
├── docker-compose.yml  # Cấu hình cụm Hadoop (HDFS, YARN) và Pipeline Runner
├── hadoop.env          # Biến môi trường cho cụm Hadoop
├── run_pipeline.sh     # Script chạy tự động toàn bộ pipeline trên YARN & HDFS
└── README.md           # Hướng dẫn chi tiết
```

---

## 3. Hướng Dẫn Vận Hành

### Cách 1: Triển khai bằng Docker Compose

Yêu cầu máy chủ đã cài đặt Docker và Docker Compose:

```bash
# 1. Khởi động cụm Hadoop và Pipeline runner
docker compose up -d

# 2. Theo dõi tiến trình thực thi
docker logs -f log-pipeline-runner

# 3. Giao diện Web UI quản trị:
# - Hadoop HDFS NameNode: http://localhost:9870
# - YARN Resource Manager: http://localhost:8088
```

Dữ liệu xử lý được lưu trữ tập trung trên HDFS, không tải file trung gian về máy host.

---

### Cách 2: Chạy trực tiếp trên máy ảo Ubuntu / Linux đã cấu hình Hadoop

#### Chạy toàn bộ pipeline tự động (6 bước):
```bash
# Mặc định lấy ngày hiện tại:
bash run_pipeline.sh

# (Tùy chọn) Truyền ngày cụ thể để xử lý:
# bash run_pipeline.sh 2026-10-01
```

#### Hoặc chạy thủ công từng bước theo phân công:
1. **[Đặng Văn Vinh] Sinh dữ liệu log:**
   ```bash
   python3 data/generate_logs.py
   ```
2. **[Đặng Văn Vinh] Nạp dữ liệu vào HDFS Raw Zone:**
   ```bash
   python3 flume/ingest_to_hdfs.py
   ```
3. **[Nông Minh Trí & Triệu Văn Huy] Tiền xử lý Map-only Job trên YARN:**
   ```bash
   bash preprocessing/run_cleaner.sh
   ```
4. **[Dương Đình Hoàng] MapReduce Aggregation Job trên YARN:**
   ```bash
   bash mapreduce/run_job.sh
   ```
5. **[Dương Đình Hoàng] Phát hiện bất thường từ HDFS stream:**
   ```bash
   python3 anomaly/detector.py
   ```

---

## 4. Các Quy Tắc Phát Hiện Bất Thường (6 Rules)

Hệ thống phát hiện các sự cố bất thường dựa trên 6 quy tắc:
1. **Rule 1 (Traffic Spike):** Giờ 15:00 tăng vọt số lượng request (vượt ngưỡng $\mu + 2\sigma$ hoặc $\mu + 3\sigma$).
2. **Rule 2 (High Error Rate):** Tỉ lệ log `ERROR` lúc 15:00 vượt quá 5%.
3. **Rule 3 (High 5xx Rate):** Tỉ lệ lỗi máy chủ HTTP 500/503 lúc 15:00 chiếm hơn 40%.
4. **Rule 4 (IP Flood / DDoS):** IP `10.0.0.99` gửi hàng ngàn request trong 1 khung giờ.
5. **Rule 5 (High Latency):** Thời gian phản hồi trung bình giờ 15:00 vọt lên > 4,000 ms.
6. **Rule 6 (Top Failing Endpoints):** Trích xuất danh sách các endpoint chịu tỉ lệ lỗi lớn nhất (ví dụ `/api/payments/process`).
