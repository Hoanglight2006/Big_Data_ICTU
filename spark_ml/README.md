# Module Học Máy PySpark MLlib (Random Forest Classifier)

> **Môn học:** Samsung Innovation Campus (SIC) - Big Data Course  
> **Nhiệm vụ:** Xây dựng mô hình Machine Learning phân loại & phát hiện nhật ký bất thường trên nền tảng tính toán phân tán Apache Spark.

---

## 1. Cấu Trúc Thư Mục

```text
spark_ml/
├── export_dataset.py                       # Script sinh tập dữ liệu huấn luyện (15.000 mẫu)
├── train_random_forest.py                  # Script huấn luyện PySpark MLlib (chạy trên máy/cụm Spark)
├── SIC_WebLog_Anomaly_RandomForest.ipynb   # File Jupyter Notebook (dùng upload chạy trực tiếp trên Google Colab)
└── README.md                               # Hướng dẫn chi tiết
```

---

## 2. Quy Trình Mô Hình Hóa Chuẩn PySpark ML

1. **Khởi tạo:** `SparkSession.builder.appName("RandomForestClassifier").getOrCreate()`
2. **Nạp & Tiền xử lý:**
   - Đọc dữ liệu CSV bằng `spark.read.options(header=True, inferSchema=True)`
   - Ép kiểu `response_time_ms` sang `DoubleType` và loại bỏ dòng `null` (`dropna()`)
   - Bỏ cột định danh `log_id` (`drop('log_id')`)
3. **Trích xuất đặc trưng (Feature Engineering):**
   - `StringIndexer` cho cột nhãn mục tiêu `is_anomaly` $\rightarrow$ `indexedLabel`
   - `StringIndexer` cho các cột danh mục: `service`, `endpoint`, `method` $\rightarrow$ `*Index`
   - `VectorAssembler` kết hợp các cột danh mục đã mã hóa và cột số (`status_code`, `response_time_ms`, `hour`) $\rightarrow$ Cột `features`
4. **Phân chia dữ liệu:** `randomSplit([0.7, 0.3])` (70% Train, 30% Test)
5. **Huấn luyện:** `RandomForestClassifier(labelCol="indexedLabel", featuresCol="features", numTrees=100)`
6. **Đánh giá:**
   - Đo độ chính xác bằng `MulticlassClassificationEvaluator(metricName="accuracy")`
   - Xuất bảng mức độ ảnh hưởng của từng đặc trưng (`featureImportances`)

---

## 3. Cách Chạy

### Cách 1: Chạy trên Google Colab (Để lấy ảnh chụp kết quả đưa vào Slide)
1. Mở [Google Colab](https://colab.research.google.com/) và tải file `SIC_WebLog_Anomaly_RandomForest.ipynb` lên.
2. Tải file dữ liệu `data/web_log_anomaly_dataset.csv` lên thư mục làm việc của Colab.
3. Chạy từng cell từ trên xuống dưới.
4. Chụp ảnh cell kết quả:
   - Bảng dự đoán mẫu `predictions.show(5)`
   - `Do chinh xac = 0.98`
   - Bảng mức độ quan trọng `featureImportances`

### Cách 2: Chạy trực tiếp trên máy ảo Linux / Ubuntu
```bash
# 1. Sinh tập dữ liệu CSV mẫu:
python3 spark_ml/export_dataset.py

# 2. Huấn luyện bằng PySpark:
python3 spark_ml/train_random_forest.py
# Hoặc submit qua Spark:
# spark-submit spark_ml/train_random_forest.py
```
