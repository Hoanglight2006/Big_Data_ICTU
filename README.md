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
[Web Server Logs / Generator] 
       │ (Sinh 150,000+ logs/24h)
       ▼
 [Apache Flume Ingestion]
       │ (Nạp trực tiếp vào HDFS Raw Zone)
       ▼
[HDFS Raw Zone] (/data/raw/YYYY-MM-DD/*.log)
       │
       ▼
[YARN Preprocessing Job]
       │ (Map-only Streaming lọc rác & chuẩn hóa trên YARN)
       ▼
[HDFS Cleaned Zone] (/data/cleaned/YYYY-MM-DD/part-*)
       │
       ▼
[YARN MapReduce Aggregation]
       │ (Tính toán phân tán trên cụm máy chủ YARN)
       ▼
[HDFS Output Zone] (/data/output/YYYY-MM-DD/part-*)
       │
       ▼
[Rule-based Anomaly Detector]
       │ (Phân tích trực tiếp từ HDFS stream)
       ▼
[HDFS Reports Zone] (/data/reports/YYYY-MM-DD/anomaly_report.json)
```

---

## 2. Cấu Trúc Thư Mục Dự Án

```text
log-simulation/
├── config/             # Cấu hình trung tâm (HDFS paths, thresholds, dynamic JARs)
├── data/               # Module sinh log web server
├── flume/              # Cấu hình Apache Flume Agent nạp HDFS & Ingestion Runner
├── preprocessing/      # Tiền xử lý phân tán Map-only trên YARN 
├── mapreduce/          # Hadoop Streaming Mapper & Reducer trên YARN
├── anomaly/            # Bộ lọc 6 quy tắc phát hiện bất thường & xuất báo cáo HDFS
├── Dockerfile          # Docker image đóng gói ứng dụng pipeline & Hadoop client
├── docker-compose.yml  # Cấu hình cụm Hadoop và Pipeline Runner
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
1. **Sinh dữ liệu log:**
   ```bash
   python3 data/generate_logs.py
   ```
2. **Nạp dữ liệu vào HDFS Raw Zone:**
   ```bash
   python3 flume/ingest_to_hdfs.py
   ```
3. **Tiền xử lý Map-only Job trên YARN:**
   ```bash
   bash preprocessing/run_cleaner.sh
   ```
4. **MapReduce Aggregation Job trên YARN:**
   ```bash
   bash mapreduce/run_job.sh
   ```
5. **Phát hiện bất thường từ HDFS stream:**
   ```bash
   python3 anomaly/detector.py
   ```

---

## 4. 6 rule phát hiện bất thường

Hệ thống phát hiện các sự cố bất thường dựa trên 6 quy tắc:
1. **Quy tắc 1 (Lượng truy cập tăng vọt):** Số lượng yêu cầu trong một giờ tăng đột biến, cao hơn nhiều so với mức trung bình của cả ngày.
2. **Quy tắc 2 (Tỉ lệ log lỗi cao):** Số lượng log báo lỗi (`ERROR`) trong một giờ chiếm hơn 5% tổng lượng truy cập.
3. **Quy tắc 3 (Tỉ lệ lỗi máy chủ cao):** Các mã lỗi từ phía máy chủ (mã HTTP 500, 503) chiếm hơn 5% lượng truy cập trong giờ đó.
4. **Quy tắc 4 (Spam truy cập / Tấn công làm nghẽn mạng):** Một địa chỉ IP gửi liên tục hơn 1.000 yêu cầu trong một giờ (có dấu hiệu spam hoặc tấn công từ chối dịch vụ).
5. **Quy tắc 5 (Thời gian xử lý quá chậm):** Thời gian phản hồi trung bình của hệ thống trong một giờ vượt quá 2 giây (2.000 ms).
6. **Quy tắc 6 (Đường dẫn gặp lỗi nhiều nhất):** Liệt kê các đường dẫn chức năng trên trang web (như thanh toán, tìm kiếm) bị lỗi nhiều lần nhất trong ngày.
