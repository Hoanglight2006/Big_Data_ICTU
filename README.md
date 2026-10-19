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
├── spark_ml/           # Mô hình PySpark MLlib (Random Forest) phát hiện bất thường
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

### Cách 2: Chạy trực tiếp trên máy ảo Ubuntu / Linux (Mô hình 2 Terminal chuẩn)

Để quay video demo trực quan và vận hành Apache Flume thật:

#### Terminal 1: Khởi động dịch vụ Apache Flume Ingestion (Chạy nền)
```bash
bash flume/start_flume.sh
```
*Terminal này sẽ thường trực lắng nghe thư mục `data/spool/`. Khi có log mới, Flume sẽ cuộn log nạp thẳng vào HDFS Raw Zone.*

#### Terminal 2: Chạy Pipeline điều phối & phân tích tự động
```bash
# Cài đặt thư viện AI (tùy chọn nhưng khuyên dùng cho mô hình Isolation Forest):
pip install -r requirements.txt

# Chạy toàn bộ pipeline:
bash run_pipeline.sh

# (Tùy chọn) Chạy cho ngày cụ thể:
# bash run_pipeline.sh 2026-10-01
```

---

## 4. Cơ chế Phát hiện Bất thường: Quy trình 2 Lớp (Rule Screening + AI Verification)

Hệ thống kết hợp giữa **Bộ luật Heuristic** và **Học máy không giám sát (Isolation Forest)** để loại bỏ báo động giả (False Positive):

1. **Quy tắc 1 (Lượng truy cập tăng vọt):** Số lượng yêu cầu trong một giờ vượt ngưỡng $Mean + 2 \times StdDev$.
2. **Quy tắc 2 (Tỉ lệ log lỗi cao):** Số log báo lỗi (`ERROR`) trong một giờ chiếm hơn 5% tổng lượng truy cập.
3. **Quy tắc 3 (Tỉ lệ lỗi máy chủ cao):** Các mã lỗi 500/503 chiếm hơn 5% lượng truy cập trong giờ.
4. **Quy tắc 4 (Quy trình 2 lớp kiểm tra IP):**
   - **Cấp 1 (Sàng lọc bằng Rule):** Chọn ra các IP có lưu lượng cao bất thường ($> 1.000$ req/giờ) đưa vào danh sách nghi vấn.
   - **Cấp 2 (Thẩm định bằng AI - Isolation Forest):** Đánh giá đa chiều (Lưu lượng request + Tỉ lệ gây lỗi). Phân biệt rạch ròi giữa **Khách hàng lớn / Đối tác lành tính** (Request cao nhưng tỉ lệ lỗi $\approx 0\%$) và **Tấn công Botnet / Flooding độc hại** (Request cao kèm tỉ lệ lỗi cao).
5. **Quy tắc 5 (Thời gian xử lý quá chậm):** Thời gian phản hồi trung bình trong một giờ vượt quá 2.000 ms.
6. **Quy tắc 6 (Đường dẫn gặp lỗi nhiều nhất):** Liệt kê Top 5 endpoints bị lỗi nhiều lần nhất trong ngày.

