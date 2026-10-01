# Hệ Thống Thu Thập, Phân Tích Log Và Phát Hiện Bất Thường Web Server

> **Đề tài:** Phân tích truy cập và phát hiện bất thường từ nhật ký Web Server bằng Big Data Pipeline (Web Simulator ➔ Kafka ➔ HDFS ➔ Hadoop MapReduce ➔ Rule-based Anomaly Detection).

---

## 1. Kiến Trúc Luồng Hoàn Chỉnh (End-to-End Pipeline)

```
[Fake Logs Generator]
        │ (52,822 logs / 24h, kèm kịch bản tấn công)
        ▼
 [Web Simulator (replay.py)] 
        │ (Kafka Producer - tua nhanh 288x)
        ▼
   [Apache Kafka] (Topic: app-logs, 3 Partitions)
        │
        ▼
 [Log Consumer (consumer.py)]
        │ (Gom micro-batch 1,000 logs)
        ▼
 [HDFS Distributed Storage] (/data/logs/batch_XXXX.txt)
        │
        ▼
[Hadoop Streaming MapReduce] (mapper.py + reducer.py)
        │ (Tính toán phân tán trên YARN)
        ▼
 [Kết Quả Thống Kê] (data/mapreduce_results.txt)
        │
        ▼
[Anomaly Detector (detector.py)] ──► [Báo Cáo Bất Thường] (data/anomaly_report.json)
```

---

## 2. Cấu Trúc Thư Mục

```text
log-simulation/
├── config/             # Cấu hình trung tâm (Kafka, HDFS, Thresholds)
├── data/               # Script sinh dữ liệu giả lập & lưu trữ kết quả
├── simulator/          # Producer phát lại log mô phỏng Web Server thời gian thực
├── kafka/              # Consumer gom batch đẩy lên HDFS
├── mapreduce/          # Hadoop Streaming Mapper & Reducer
├── anomaly/            # Module thống kê và 6 Rules phát hiện bất thường
├── run_pipeline.sh     # Script chạy tự động toàn bộ luồng (1 chạm)
└── README.md           # Hướng dẫn tổng quan
```

---

## 3. Hướng Dẫn Vận Hành

<!-- ### Cách 1: Chạy Tự Động 1 Lệnh (Khuyên Dùng)

Đảm bảo ZooKeeper, Kafka Broker và Hadoop cluster đã được bật, sau đó chỉ cần chạy:

```bash
bash /home/hoang/log-simulation/run_pipeline.sh
``` --> 
## Đang lỗi, dùng cách 2

---

### Cách 2: Chạy Từng Bước (Thao Tác Thủ Công)

1. **Sinh dữ liệu log giả lập:**
   ```bash
   python3 data/generate_logs.py
   ```

2. **Tạo Topic Kafka & Thư mục HDFS:**
   ```bash
   /home/hoang/kafka_2.12-2.8.1/bin/kafka-topics.sh --create --topic app-logs --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
   hdfs dfs -mkdir -p /data/logs /data/output
   ```

3. **Bật Consumer đón log đẩy lên HDFS (Terminal 1):**
   ```bash
   python3 kafka/consumer.py
   ```

4. **Bật Replay phát log vào Kafka (Terminal 2):**
   ```bash
   python3 simulator/replay.py
   ```

5. **Chạy Hadoop MapReduce tổng hợp log:**
   ```bash
   bash mapreduce/run_job.sh
   ```

6. **Chạy phát hiện bất thường:**
   ```bash
   python3 anomaly/detector.py
   ```

---

## 4. Kịch Bản Bất Thường Được Phát Hiện

Hệ thống tự động phát hiện chính xác các dấu hiệu bất thường đã được cài cắm:
- **Tấn công IP Flood/DDoS:** IP `10.0.0.99` gửi > 1,500 requests trong khung giờ 15:00.
- **Traffic Spike:** Giờ 15:00 tăng vọt lên 5,644 requests (vượt ngưỡng `Mean + 2*Std`).
- **Máy chủ quá tải (High 5xx Rate):** Tỉ lệ lỗi HTTP 500/503 giờ 15:00 chiếm **43.9%**.
- **Độ trễ cao (High Latency):** Thời gian phản hồi trung bình giờ 15:00 vọt lên **4,319 ms**.
