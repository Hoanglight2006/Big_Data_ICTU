# Thư Mục `mapreduce/` — Tính Toán Phân Tán (Hadoop Streaming)

> **Thành viên phụ trách (theo Action Plan):** Dương Đình Hoàng (Leader)  
> **Nhiệm vụ:** *"Utilizes MapReduce to analyze data provided by Nong Minh Tri and Trieu Van Huy, building the core aggregation metrics."*

---

## 1. Vai Trò & Nguyên Lý

Thực thi tính toán phân tán quy mô lớn trên cụm **Hadoop YARN** thông qua cơ chế **Hadoop Streaming**:
- Dữ liệu đầu vào: Thư mục log phân vùng theo ngày trên HDFS (`/data/logs/<LOG_DATE>/`).
- Dữ liệu đầu ra: Kết quả tổng hợp chỉ số lưu trên HDFS (`/data/output/<LOG_DATE>/`) và tự động kéo về `data/mapreduce_results.txt`.

---

## 2. Các Tệp Tin

- **`mapper.py`**:
  - Nhận từng dòng log sạch từ HDFS qua `sys.stdin`.
  - Bóc tách các trường: Giờ (`hour`), Địa chỉ IP (`ip`), Trạng thái (`status_code`), Mức log (`log_level`), Độ trễ (`response_time_ms`), Đường dẫn (`endpoint`).
  - Xuất các cặp Key-Value:
    - `hour_req:<hour>\t1`
    - `hour_err:<hour>\t1`
    - `hour_5xx:<hour>\t1`
    - `hour_resp:<hour>\t<response_time>`
    - `ip_req:<hour>:<ip>\t1`
    - `endpoint_err:<endpoint>\t1`

- **`reducer.py`**:
  - Nhận dữ liệu đã qua sắp xếp (Sort/Shuffle) theo Key từ Hadoop.
  - Gom nhóm và tính toán:
    - Đếm tổng số lượt request, lỗi ERROR, lỗi 5xx trong từng giờ.
    - Tính thời gian phản hồi trung bình (Average Response Time) cho từng giờ.
    - Đếm số lượt request của từng địa chỉ IP trong từng giờ.
  - Xuất kết quả tổng hợp ra HDFS.

- **`run_job.sh`**:
  - Shell script submit MapReduce Job lên Hadoop YARN.
  - Tự động tìm kiếm file `hadoop-streaming*.jar` linh hoạt.
  - Hỗ trợ tham số ngày phân tích: `bash mapreduce/run_job.sh 2026-10-01`.
