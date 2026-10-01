# Thư Mục `preprocessing/` — Tiền Xử Lý & Làm Sạch Dữ Liệu

> **Thành viên phụ trách (theo Action Plan):** Nông Minh Trí & Triệu Văn Huy  
> **Nhiệm vụ:** *"Receive the raw HDFS data from Dang Van Vinh, cleaning and structuring it to ensure high-quality inputs for the MapReduce analysis."*

---

## 1. Vai Trò & Mục Tiêu

Trong các luồng Big Data thực tế, dữ liệu thô (raw logs) từ Web Server thu thập qua Flume thường xuyên xuất hiện:
- Dòng log bị cắt đứt do nghẽn mạng (malformed JSON).
- Trường dữ liệu bị thiếu hoặc mang giá trị `null`.
- Địa chỉ IP không hợp lệ hoặc bị chèn ký tự lạ.
- Status code hoặc response time bị sai lệch kiểu dữ liệu.

Module này do **Nông Minh Trí và Triệu Văn Huy** phụ trách nhằm loại bỏ các bản ghi lỗi cú pháp hoặc thiếu trường thông tin, chuẩn hóa cấu trúc đầu vào cho bước phân tích MapReduce tiếp theo.

---

## 2. Các Tệp Tin

- **`cleaner.py`**:
  - Quét từng dòng log thô.
  - Lọc lỗi cú pháp JSON và các trường bắt buộc (`timestamp`, `ip`, `status_code`, `endpoint`, `response_time_ms`, `level`).
  - Xác thực chuẩn IPv4 hợp lệ.
  - Phân tách log rác vào `data/corrupted_logs.txt`.
  - Xuất dữ liệu sạch đạt chuẩn vào `data/cleaned_logs.json`.

---

## 3. Cách Sử Dụng

```bash
python3 preprocessing/cleaner.py data/fake_logs.json
```
