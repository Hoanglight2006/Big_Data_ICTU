# =============================================================================
# spark_ml/train_random_forest.py — Huấn luyện mô hình RandomForestClassifier bằng PySpark MLlib
#
# Áp dụng chính xác 100% theo mẫu bài giảng Samsung Innovation Campus (SIC)
# Dữ liệu đầu vào: Dữ liệu log sạch thực tế của project (data/web_log_dataset.csv)
# =============================================================================

import os
import sys

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer, VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import col
from pyspark.sql import SparkSession

def main():
    print("==========================================================")
    print(" [INFO] KHOI TAO SPARK SESSION (RANDOM FOREST CLASSIFIER)")
    print("==========================================================")

    # Khởi tạo SparkSession
    spark = SparkSession\
        .builder\
        .appName("RandomForestClassifier")\
        .master("local[*]")\
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Đường dẫn file dữ liệu thực tế từ log của project
    csv_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data/web_log_dataset.csv")
    if not os.path.exists(csv_path):
        print(f"[WARN] Chua tim thay {csv_path}, dang tu dong chuyen doi tu cleaned_logs.json...")
        import subprocess
        subprocess.run([sys.executable, "spark_ml/export_cleaned_to_csv.py"])

    # Đọc dữ liệu (header=True, inferSchema=True)
    data = spark.read.options(header=True, inferSchema=True)\
        .csv(csv_path)
    data.show(5)

    # Tiền xử lý dữ liệu (cast DoubleType, dropna)
    data = data.withColumn("response_time_ms", data["response_time_ms"].cast(DoubleType())).dropna()
    data.show(5)

    # Danh sách cột
    label_col = "is_anomaly"
    categorical_cols = [f.name for f in data.schema.fields if f.dataType.typeName() == 'string' and f.name != label_col]
    numeric_cols = [f.name for f in data.schema.fields if f.dataType.typeName() in ['integer', 'double']]

    print(f"\nDanh sách cột danh mục (categorical): {categorical_cols}")
    print(f"Danh sách cột số (numeric)          : {numeric_cols}")

    # Fit label
    labelIndexer = StringIndexer(inputCol=label_col, outputCol="indexedLabel").fit(data)
    data_new = labelIndexer.transform(data)

    # Mã hóa các cột danh mục
    indexed_cat_cols = [c + "Index" for c in categorical_cols]
    features = StringIndexer(inputCols=categorical_cols, outputCols=indexed_cat_cols).fit(data_new)
    data_new = features.transform(data_new)

    # Ghép vector đặc trưng bằng VectorAssembler
    assemblerInput = indexed_cat_cols + numeric_cols
    assembler = VectorAssembler(inputCols=assemblerInput, outputCol="features")
    data_new = assembler.transform(data_new)

    data_new.show(5)

    # Chia tập train / test
    (trainingData, testData) = data_new.randomSplit([0.7, 0.3], seed=42)

    # Bắt đầu huấn luyện mô hình
    rf = RandomForestClassifier(labelCol="indexedLabel", featuresCol="features", numTrees=100)
    rfModel = rf.fit(trainingData)

    # Dự đoán trên tập test
    predictions = rfModel.transform(testData)

    # Transform nhãn ngược lại dạng chữ
    labelConverter = IndexToString(inputCol="prediction", outputCol="predictedLabel", labels=labelIndexer.labels)
    predictions = labelConverter.transform(predictions)

    # Hiển thị kết quả dự đoán mẫu
    predictions.select("predictedLabel", label_col, "features").show(5)

    # Đánh giá độ chính xác
    evaluator = MulticlassClassificationEvaluator(labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)
    print(f"\nDo chinh xac = {accuracy:.2f}")

    # Lấy danh sách các cột trong mô hình
    feature_names = assemblerInput

    # Mức độ quan trọng của từng cột (Feature Importances)
    importances = rfModel.featureImportances

    print("\nMuc do anh huong den viec phat hien bat thuong")
    for i in range(len(feature_names)):
        print(f"Cột {feature_names[i]:<25}: {importances[i]:.4f}")

    spark.stop()

if __name__ == "__main__":
    main()
