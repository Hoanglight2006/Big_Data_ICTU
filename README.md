# Phân Tích Nhật Ký Web Server & Phát Hiện Bất Thường (Big Data)

- **Môn học:** Samsung Innovation Campus (SIC) - Big Data Course
- **Nhóm thực hiện:** Group 3
- **Thành viên:**
  - **Dương Đình Hoàng (Leader):** Kiến trúc hệ thống, Docker, Pipeline tự động, PySpark MLlib KMeans.
  - **Đặng Văn Vinh:** Giả lập dữ liệu web log, cấu hình Apache Flume Ingestion nạp HDFS.
  - **Nông Minh Trí:** Cấu trúc phân vùng lưu trữ HDFS, tiền xử lý dữ liệu Map-only trên YARN.
  - **Triệu Văn Huy:** Tổng hợp số liệu phân tán bằng Hadoop MapReduce trên YARN.
  - **Lý Việt Hoàng:** Bộ luật phát hiện bất thường (Rule-based), kiểm thử benchmark, báo cáo.

---

## 1. Kiến Trúc Luồng Dữ Liệu

Mô hình xử lý lô (Daily Batch) cho dữ liệu nhật ký 24 giờ:

```text
[Web Server Logs / Generator]
       │ (150,000+ logs/ngày)
       ▼
 [Apache Flume Ingestion]
       │ (Nạp vào HDFS Raw Zone)
       ▼
[HDFS Raw Zone] (/data/raw/YYYY-MM-DD/*.log)
       │
       ▼
[YARN Preprocessing Job]
       │ (Map-only Streaming lọc rác & chuẩn hóa)
       ▼
[HDFS Cleaned Zone] (/data/cleaned/YYYY-MM-DD/part-*)
       │
       ▼
[YARN MapReduce Aggregation]
       │ (Tính toán lưu lượng, lỗi, độ trễ theo giờ/IP)
       ▼
[HDFS Output Zone] (/data/output/YYYY-MM-DD/part-*)
       │
       ├──► [Lịch sử N ngày] ──► Huấn luyện PySpark MLlib KMeans ──► [HDFS Model]
       │                                                                   │
       └──► [Ngày hiện tại]  ──► 6 Quy tắc lọc thô (Rule-based)           │
                                            │                              │
                                            └──► Thẩm định khử báo động giả ◄┘
                                                        │
                                                        ▼
                                       [HDFS Reports] (/data/reports/YYYY-MM-DD/anomaly_report.json)
```

---

## 2. Cấu Trúc Thư Mục

```text
log-simulation/
├── config/             # Cấu hình HDFS paths, ngưỡng cảnh báo, JARs
├── data/               # Module sinh dữ liệu log giả lập
├── flume/              # Cấu hình và script chạy Apache Flume Agent
├── preprocessing/      # Tiền xử lý Map-only trên YARN
├── mapreduce/          # Mapper & Reducer Hadoop Streaming trên YARN
├── anomaly/            # Bộ lọc 6 rule & mô hình PySpark KMeans
├── Dockerfile          # Image đóng gói pipeline & Hadoop client
├── docker-compose.yml  # Cụm Hadoop (NameNode, DataNode, YARN) + Pipeline Runner
├── hadoop.env          # Biến môi trường cụm Hadoop
├── init_history.sh     # Script sinh và xử lý log lịch sử nhiều ngày (200MB - 300MB)
├── run_pipeline.sh     # Script chạy tự động pipeline hàng ngày
└── requirements.txt    # Thư viện Python (pyspark, numpy)
```

---

## 3. Hướng Dẫn Vận Hành

### Cách 1: Chạy bằng Docker Compose (Khuyên dùng)

Toàn bộ cụm Hadoop, YARN và ứng dụng pipeline được đóng gói sẵn.

1. **Build image và khởi động cụm:**
   ```bash
   docker-compose build
   docker-compose up -d
   ```

2. **Kiểm tra trạng thái container:**
   ```bash
   docker-compose ps
   ```
   Web UI theo dõi:
   - HDFS NameNode: `http://localhost:9870`
   - YARN ResourceManager: `http://localhost:8088`

3. **Theo dõi tiến trình chạy pipeline:**
   ```bash
   docker-compose logs -f pipeline
   ```

4. **Truy cập vào container pipeline để chạy lệnh thủ công (nếu cần):**
   ```bash
   docker exec -it log-pipeline-runner bash
   ```

5. **Dừng cụm:**
   ```bash
   docker-compose down
   ```

---

### Cách 2: Vận hành trực tiếp trên máy ảo Ubuntu

> **Quan trọng:** Phải chạy Terminal 1 trước và đảm bảo Flume Agent **đã khởi động hoàn toàn**, sau đó mới chạy Terminal 2. Các lệnh `init_history.sh` và `run_pipeline.sh` ở Terminal 2 đều gọi `ingest_to_hdfs.py` để chờ Flume nạp log — nếu Flume chưa chạy, script sẽ báo lỗi và thoát sau 30 giây.

#### Terminal 1 (mở trước, giữ chạy thường trực)
```bash
bash flume/start_flume.sh
```
Script tự khởi động Hadoop/YARN nếu chưa bật, sau đó lắng nghe thư mục `data/spool/`.  
**Đợi đến khi thấy dòng `INFO lifecycleSupervisor` trên Terminal 1** trước khi sang Terminal 2.

#### Terminal 2 — Tạo dữ liệu lịch sử (lần đầu duy nhất)
```bash
bash init_history.sh 7
```
Lặp qua 7 ngày, mỗi ngày: sinh log vào `data/spool/` → chờ Flume nạp HDFS → tiền xử lý YARN → MapReduce tổng hợp. Kết quả lưu tại `/data/output/<DATE>/`.

#### Terminal 2 — Huấn luyện mô hình KMeans (lần đầu duy nhất, sau khi có lịch sử)
```bash
spark-submit --master yarn anomaly/train_kmeans_model.py
```
Mô hình lưu lên HDFS tại `/models/kmeans_anomaly_model`.

#### Terminal 2 — Chạy pipeline phân tích hàng ngày
```bash
bash run_pipeline.sh

# Hoặc chỉ định ngày cụ thể:
# bash run_pipeline.sh 2026-10-15
```
Sinh log ngày hiện tại → Flume nạp HDFS → tiền xử lý → MapReduce → phát hiện bất thường → lưu báo cáo HDFS `/data/reports/<DATE>/anomaly_report.json`.

---

## 4. Cơ Chế Phát Hiện Bất Thường

Quy trình kết hợp 2 giai đoạn:

1. **Giai đoạn 1 — Lọc thô bằng 6 Rule (`anomaly/detector.py`):**
   - **Rule 1 (Spike lưu lượng):** Số request/giờ vượt ngưỡng `Mean + 2 * StdDev`.
   - **Rule 2 (Tỷ lệ log lỗi):** Log cấp độ `ERROR` chiếm > 5% tổng request trong giờ.
   - **Rule 3 (Lỗi Server 5xx):** Mã HTTP 500/503 chiếm > 5% request trong giờ.
   - **Rule 4 (IP bất thường):** Một IP gửi > 1.000 request/giờ.
   - **Rule 5 (Độ trễ cao):** Thời gian phản hồi trung bình trong giờ > 2.000 ms.
   - **Rule 6 (Top lỗi Endpoint):** 5 URL có số lần lỗi nhiều nhất trong ngày.

2. **Giai đoạn 2 — Thẩm định bằng KMeans (`anomaly/kmeans_verifier.py`):**
   - Đưa các điểm nghi vấn từ Giai đoạn 1 vào mô hình KMeans 4 chiều: `[Traffic, Error %, 5xx %, Latency]`.
   - Đo khoảng cách tới tâm cụm bình thường:
     - **Bỏ báo động giả (False Positive):** Khung giờ traffic cao nhưng không phát sinh lỗi và độ trễ thấp được xếp về bình thường.
     - **Xác nhận bất thường thực sự (True Anomaly):** Điểm lệch xa tâm cụm được đánh dấu cảnh báo vào file báo cáo JSON trên HDFS `/data/reports/<DATE>/anomaly_report.json`.
