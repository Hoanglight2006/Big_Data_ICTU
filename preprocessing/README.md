# Thư Mục `preprocessing/` — Tiền Xử Lý & Làm Sạch Dữ Liệu

> **Thành viên phụ trách (theo Action Plan):** Nông Minh Trí & Triệu Văn Huy  
> **Nhiệm vụ:** *"Receive the raw HDFS data from Dang Van Vinh, cleaning and structuring it to ensure high-quality inputs for the MapReduce analysis."*

---

## 1. Vai Trò & Nguyên Lý Phân Tán Trên YARN

Trong luồng Big Data thực tế, dữ liệu thô (raw logs) từ Web Server thu thập qua Flume thường xuyên xuất hiện các vấn đề:
- Dòng log bị cắt đứt do nghẽn mạng (malformed JSON).
- Trường dữ liệu bị thiếu hoặc mang giá trị `null`.
- Địa chỉ IP không hợp lệ hoặc chèn ký tự lạ.
- Kiểu dữ liệu không đồng nhất (status code, response time).

Toàn bộ quá trình tiền xử lý được thực thi phân tán trên cụm **Hadoop YARN** (Map-only Job), đọc trực tiếp từ phân vùng HDFS Raw và ghi ra HDFS Cleaned mà **không tải file trung gian về máy host**:
- **HDFS Input (Raw Zone):** `/data/raw/<LOG_DATE>/access.log`
- **HDFS Output (Clean Zone):** `/data/cleaned/<LOG_DATE>/part-*`

---

## 2. Các Tệp Tin

- **`cleaner_mapper.py`**:
  - Script Mapper nhận từng dòng log thô qua `sys.stdin` trên các container của worker node.
  - Xác thực tính toàn vẹn cú pháp JSON.
  - Kiểm tra đầy đủ các trường bắt buộc (`timestamp`, `ip`, `status_code`, `endpoint`, `response_time_ms`, `level`).
  - Xác thực định dạng địa chỉ IPv4.
  - Tăng Hadoop Counter ghi nhận chất lượng dữ liệu (`DataQuality,Corrupted_JSON`, `DataQuality,Invalid_IPv4`).
  - Ghi bản ghi sạch đã chuẩn hóa ra `sys.stdout` trực tiếp vào HDFS.

- **`run_cleaner.sh`**:
  - Script submit Map-only Job (`-numReduceTasks 0`) lên YARN ResourceManager.
  - Tham số ngày phân tích: `bash preprocessing/run_cleaner.sh [YYYY-MM-DD]`.

- **`cleaner.py`**:
  - Script chạy độc lập (standalone) hỗ trợ kiểm thử cục bộ khi chưa bật cụm Hadoop.

---

## 3. Cách Sử Dụng

### Chạy phân tán trên cụm YARN (Khuyến nghị):
```bash
bash preprocessing/run_cleaner.sh 2026-10-01
```

### Chạy kiểm thử offline (Không cần cụm Hadoop):
```bash
python3 preprocessing/cleaner.py data/fake_logs.json
```
