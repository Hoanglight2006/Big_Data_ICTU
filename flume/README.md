# Thư Mục `flume/` — Tầng Thu Thập & Nạp Dữ Liệu (Ingestion Layer)

> **Thành viên phụ trách (theo Action Plan):** Đặng Văn Vinh  
> **Nhiệm vụ:** Thu thập log phát sinh từ Web Server và tự động lưu trữ phân vùng theo ngày trên HDFS.

---

## 1. Các Tệp Tin

- **`flume-hdfs.conf`**:
  - File cấu hình chuẩn của **Apache Flume Agent (`a1`)**:
    - **Source (`r1`):** `spooldir` — Tự động theo dõi thư mục `data/spool/`. Khi có file log mới của ngày hôm trước được xuất ra, Flume tự động đọc dữ liệu.
    - **Interceptor (`i1`):** `timestamp` — Gắn timestamp vào từng sự kiện log để HDFS Sink phân vùng thư mục động theo `%Y-%m-%d`.
    - **Channel (`c1`):** `memory` — Bộ đệm trung gian trong RAM để đảm bảo tốc độ cao.
    - **Sink (`k1`):** `hdfs` — Ghi luồng log trực tiếp vào hệ thống file phân tán tại `hdfs://localhost:9000/data/logs/%Y-%m-%d/`.
- **`ingest_to_hdfs.py`**:
  - Script tự động hóa việc đưa file log vào phân vùng HDFS `/data/logs/<LOG_DATE>/access.log`.
  - Hỗ trợ chạy kiểm thử tự động (CI/CD hoặc 1-click test) độc lập khi môi trường chưa khởi động daemon Flume.

---

## 2. Cách Khởi Động Flume Agent (Thủ công)

Nếu trên máy đã cài đặt Apache Flume, bạn có thể khởi động Agent bằng lệnh:

```bash
flume-ng agent \
  --conf-file flume/flume-hdfs.conf \
  --name a1 \
  -Dflume.root.logger=INFO,console
```

Khi chạy, bất kỳ file log nào xuất hiện trong thư mục `data/spool/` sẽ được Flume nạp ngay lập tức lên HDFS phân vùng theo ngày tương ứng.
