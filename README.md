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
├── flume/              # Cấu hình Apache Flume Agent nạp HDFS (start_flume.sh cho Terminal 1)
├── preprocessing/      # Tiền xử lý phân tán Map-only trên YARN 
├── mapreduce/          # Hadoop Streaming Mapper & Reducer trên YARN
├── anomaly/            # Bộ lọc Rule-based & Mô hình Machine Learning (KMeans) tăng độ chính xác
├── Dockerfile          # Docker image đóng gói ứng dụng pipeline & Hadoop client
├── docker-compose.yml  # Cấu hình cụm Hadoop và Pipeline Runner
├── hadoop.env          # Biến môi trường cho cụm Hadoop
├── requirements.txt    # Thư viện Machine Learning (scikit-learn, numpy)
├── run_pipeline.sh     # Script chạy tự động toàn bộ pipeline trên YARN & HDFS (Terminal 2)
└── README.md           # Hướng dẫn chi tiết
```

---

## 3. Hướng Dẫn Vận Hành (Mô hình 2 Terminal chuẩn)

#### Terminal 1: Khởi động dịch vụ Apache Flume Ingestion (Chạy nền)
```bash
bash flume/start_flume.sh
```
*Terminal này sẽ thường trực lắng nghe thư mục `data/spool/`. Khi có log mới, Flume sẽ cuộn log nạp thẳng vào HDFS Raw Zone.*

#### Terminal 2: Chạy Pipeline điều phối & phân tích tự động
```bash
# Cài đặt thư viện Machine Learning:
pip install -r requirements.txt

# Chạy toàn bộ pipeline:
bash run_pipeline.sh

# (Tùy chọn) Chạy cho ngày cụ thể:
# bash run_pipeline.sh 2026-10-01
```

---

## 4. Cơ Chế Phát Hiện Bất Thường & Tăng Độ Chính Xác (Rule + KMeans ML)

Hệ thống kết hợp giữa **Bộ luật** và **Mô hình Học máy KMeans** theo quy trình 2 giai đoạn:

* **Giai đoạn 1 (Bộ luật - Lọc thô):**
  1. *Quy tắc 1 (Lượng truy cập tăng vọt):* Số lượng yêu cầu trong một giờ vượt ngưỡng $Mean + 2 \times StdDev$.
  2. *Quy tắc 2 (Tỉ lệ log lỗi cao):* Số log báo lỗi (`ERROR`) trong một giờ chiếm hơn 5% tổng lượng truy cập.
  3. *Quy tắc 3 (Tỉ lệ lỗi máy chủ cao):* Các mã lỗi 500/503 chiếm hơn 5% lượng truy cập trong giờ.
  4. *Quy tắc 4 (Lưu lượng IP cao):* Phát hiện các địa chỉ IP gửi trên 1.000 req/giờ.
  5. *Quy tắc 5 (Thời gian xử lý quá chậm):* Thời gian phản hồi trung bình trong một giờ vượt quá 2.000 ms.
  6. *Quy tắc 6 (Đường dẫn gặp lỗi nhiều nhất):* Liệt kê Top 5 endpoints bị lỗi nhiều lần nhất trong ngày.

* **Giai đoạn 2 (Thẩm định bằng KMeans - Nâng cao độ chính xác):**
  - Tiếp nhận danh sách các khung giờ và IP nghi vấn từ Giai đoạn 1.
  - Sử dụng mô hình `KMeans` phân cụm đa chiều `[Traffic, Lỗi %, Lỗi 5xx %, Độ trễ]` sau khi chuẩn hóa bằng `StandardScaler`.
  - Đo khoảng cách từ mỗi điểm tới tâm cụm bình thường để thẩm định:
    - **Loại bỏ báo động giả (False Positives):** Các trường hợp chỉ vì lưu lượng hơi cao nhưng không gây lỗi (như khách lớn/bot lành tính) được xác nhận an toàn, không cảnh báo sai.
    - **Xác thực bất thường thực sự (True Anomalies):** Các khung giờ hoặc IP dị biệt văng xa khỏi tâm cụm được gắn cờ báo động chính xác.


