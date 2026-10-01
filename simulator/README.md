# Thư Mục `simulator/`

Đóng vai trò là **Kafka Producer**, mô phỏng lại luồng log phát ra từ Web Server theo thời gian thực.

## Các tệp tin

- **`replay.py`**:
  - Đọc file `data/fake_logs.json`.
  - Tính toán khoảng cách thời gian giữa các dòng log liên tiếp (`delta_t`) và chia cho `SCALE_FACTOR` để `sleep()`.
  - Bắn từng dòng log vào Kafka Topic `app-logs` theo đúng tiến trình thời gian thực.
  - Sử dụng trường `ip` làm message key để đảm bảo các request từ cùng một IP được phân bổ vào cùng partition.
