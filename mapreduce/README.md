# Thư Mục `mapreduce/`

Chứa mã nguồn chương trình **Hadoop Streaming MapReduce**, chịu trách nhiệm xử lý và tổng hợp dữ liệu quy mô lớn trên HDFS.

## Các tệp tin

- **`mapper.py`**:
  - Nhận luồng log từ HDFS qua `sys.stdin`.
  - Trích xuất: Giờ (`hour`), Địa chỉ IP (`ip`), Trạng thái (`status_code`), Mức log (`log_level`), Thời gian phản hồi (`response_time_ms`), Đường dẫn (`endpoint`).
  - Xuất ra stdout các cặp Key-Value:
    - `hour_req:<hour>\t1`
    - `hour_err:<hour>\t1`
    - `hour_5xx:<hour>\t1`
    - `hour_resp:<hour>\t<response_time>`
    - `ip_req:<hour>:<ip>\t1`
    - `endpoint_err:<endpoint>\t1`
- **`reducer.py`**:
  - Nhận dữ liệu đã qua sắp xếp (Sort/Shuffle) từ Hadoop.
  - Gom nhóm theo từng Key:
    - Tính tổng số lượng request, lỗi ERROR, lỗi 5xx, và request theo IP.
    - Tính giá trị trung bình cho thời gian phản hồi (`hour_resp`).
  - Xuất kết quả cuối cùng ra stdout để Hadoop lưu vào HDFS.
- **`run_job.sh`**:
  - Script tự động nộp MapReduce Job lên cụm Hadoop YARN thông qua `hadoop-streaming-3.2.1.jar`.
  - Tự động kéo kết quả từ HDFS (`/data/output/part-*`) về lưu tại `data/mapreduce_results.txt`.
