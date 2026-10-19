# Module Mô Hình Hóa Học Máy PySpark MLlib (Random Forest)

> **Môn học:** Samsung Innovation Campus (SIC) - Big Data Course  
> **Ứng dụng:** Áp dụng nguyên mẫu quy trình phân loại PySpark MLlib (từ bài giảng SIC) vào việc phát hiện nhật ký bất thường của dự án Web Server.

---

## 1. Nguồn Gốc Dữ Liệu

Dữ liệu huấn luyện **lấy 100% từ kết quả làm sạch của Pipeline** trong dự án:
- Nguồn: `data/cleaned_logs.json` (hơn 52.000 log thực tế sau khi qua tầng YARN Preprocessing).
- Tập dữ liệu: `data/web_log_dataset.csv` (chứa các trường: `service`, `method`, `endpoint`, `status_code`, `response_time_ms`, `level`, và nhãn `is_anomaly`).
- Nếu cần xuất lại CSV từ log mới:
  ```bash
  python3 spark_ml/export_cleaned_to_csv.py
  ```

---

## 2. Quy Trình Mô Hình Hóa Chuẩn Theo Bài Mẫu SIC

Toàn bộ các bước được viết chuẩn khớp từng dòng với bài giảng Google Colab:

```text
[Dữ liệu Web Log thực tế (CSV)]
              │
              ▼
[Tiền xử lý (cast DoubleType, dropna)]
              │
              ▼
[Phân tách Cột: Categorical vs Numeric]
              │
              ▼
[StringIndexer (Nhãn & Cột danh mục)]
              │
              ▼
[VectorAssembler (Gộp thành Cột features)]
              │
              ▼
[randomSplit (70% Train, 30% Test)]
              │
              ▼
[RandomForestClassifier (100 cây quyết định)]
              │
              ▼
[Đánh giá Accuracy & Feature Importances]
```

---

## 3. Cách Sử Dụng

### Cách 1: Chạy trên Google Colab (Để lấy ảnh chụp kết quả đưa vào Slide)
1. Mở [Google Colab](https://colab.research.google.com/).
2. Tải lên file notebook: `spark_ml/SIC_WebLog_Anomaly_RandomForest.ipynb`.
3. Tải lên file dữ liệu: `data/web_log_dataset.csv`.
4. Bấm **Chạy tất cả (Run all)**:
   - Cell 1-7: Chuẩn bị dữ liệu và tạo `features` vector.
   - Cell 8: Huấn luyện `RandomForestClassifier` và hiển thị kết quả dự đoán `predictions.show(5)`.
   - Cell 9: In độ chính xác `Do chinh xac = 0.99`.
   - Cell 10: In bảng `Muc do anh huong den viec phat hien bat thuong` (`featureImportances`).
5. Chụp ảnh kết quả cell 8, 9, 10 dán trực tiếp vào Slide thuyết trình Capstone.

### Cách 2: Chạy trực tiếp trên máy ảo Linux / Ubuntu
```bash
python3 spark_ml/train_random_forest.py
```
*(Hoặc submit qua Apache Spark: `spark-submit spark_ml/train_random_forest.py`)*
