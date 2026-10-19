# =============================================================================
# spark_ml/train_random_forest.py — Huấn luyện mô hình RandomForestClassifier bằng PySpark MLlib
#
# Chuẩn quy trình đào tạo môn Big Data (Samsung Innovation Campus - SIC)
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
    print(" [INFO] KHOI TAO SPARK SESSION CHO RANDOM FOREST")
    print("==========================================================")

    # 1. Khởi tạo SparkSession
    spark = SparkSession\
        .builder\
        .appName("RandomForestClassifier_WebLogAnomaly")\
        .master("local[*]")\
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 2. Đọc dữ liệu CSV
    dataset_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data/web_log_anomaly_dataset.csv")
    if not os.path.exists(dataset_path):
        print(f"[ERROR] Khong tim thay file dataset tai {dataset_path}. Hay chay export_dataset.py truoc.")
        sys.exit(1)

    print(f"\n[1/6] Doc du lieu tu file: {dataset_path}")
    data = spark.read.options(header=True, inferSchema=True)\
        .csv(dataset_path)
    data.show(5)

    # 3. Tiền xử lý dữ liệu: ép kiểu và loại bỏ cột ID không cần thiết
    print("\n[2/6] Tien xu ly du lieu (cast DoubleType, dropna, drop ID)...")
    data = data.withColumn("response_time_ms", data["response_time_ms"].cast(DoubleType())).dropna()
    data = data.drop('log_id')
    data.show(5)

    # 4. Xác định danh sách cột danh mục (categorical) và cột số (numeric)
    label_col = "is_anomaly"
    categorical_cols = [f.name for f in data.schema.fields if f.dataType.typeName() == 'string' and f.name != label_col]
    numeric_cols = [f.name for f in data.schema.fields if f.dataType.typeName() in ['integer', 'double']]

    print(f"Cột nhãn (label): {label_col}")
    print(f"Cột danh mục (categorical): {categorical_cols}")
    print(f"Cột số (numeric)          : {numeric_cols}")

    # 5. Fit nhãn (label)
    labelIndexer = StringIndexer(inputCol=label_col, outputCol="indexedLabel").fit(data)
    data_new = labelIndexer.transform(data)

    # Mã hóa các cột categorical
    indexed_cat_cols = [c + "Index" for c in categorical_cols]
    features = StringIndexer(inputCols=categorical_cols, outputCols=indexed_cat_cols).fit(data_new)
    data_new = features.transform(data_new)

    # Ghép vector đặc trưng bằng VectorAssembler
    assemblerInput = indexed_cat_cols + numeric_cols
    assembler = VectorAssembler(inputCols=assemblerInput, outputCol="features")
    data_new = assembler.transform(data_new)

    print("\n[3/6] Du lieu sau khi tao Vector dac trung (features):")
    data_new.select("features", "indexedLabel").show(5)

    # 6. Chia tập Train / Test (70% Train, 30% Test)
    print("\n[4/6] Chia tap Train / Test theo ti le 70 / 30...")
    (trainingData, testData) = data_new.randomSplit([0.7, 0.3], seed=42)
    print(f"Số lượng mẫu huấn luyện (Train): {trainingData.count():,}")
    print(f"Số lượng mẫu kiểm thử (Test)   : {testData.count():,}")

    # 7. Bắt đầu huấn luyện mô hình RandomForestClassifier
    print("\n[5/6] Huan luyen mo hinh RandomForest (numTrees=100)...")
    rf = RandomForestClassifier(labelCol="indexedLabel", featuresCol="features", numTrees=100)
    rfModel = rf.fit(trainingData)

    # Dự đoán trên tập Test
    predictions = rfModel.transform(testData)

    # Chuyển ngược nhãn dự đoán về chuỗi ban đầu
    labelConverter = IndexToString(inputCol="prediction", outputCol="predictedLabel", labels=labelIndexer.labels)
    predictions = labelConverter.transform(predictions)

    print("\nKet qua du doan mau tren tap test:")
    predictions.select("predictedLabel", label_col, "features").show(10)

    # Đánh giá độ chính xác
    evaluator = MulticlassClassificationEvaluator(labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)
    print(f"\n==========================================================")
    print(f" [DANH GIA] Do chinh xac (Accuracy) = {accuracy:.4f} ({accuracy * 100:.2f}%)")
    print(f"==========================================================")

    # 8. Mức độ quan trọng của từng đặc trưng (Feature Importances)
    feature_names = assemblerInput
    importances = rfModel.featureImportances

    print("\n[6/6] Muc do anh huong / quan trong cua tung dac trung den viec phat hien bat thuong:")
    for i in range(len(feature_names)):
        print(f"Cột {feature_names[i]:<25}: {importances[i]:.4f}")

    spark.stop()
    print("\n[INFO] Hoan tat chu trinh huan luyen PySpark MLlib thanh cong!")

if __name__ == "__main__":
    main()
