# =============================================================================
# anomaly/train_kmeans_model.py — Luồng 1: Huấn luyện mô hình KMeans từ dữ liệu nhiều ngày
#
# Đọc dữ liệu lịch sử từ HDFS/local, huấn luyện mô hình PySpark KMeans và lưu lên HDFS.
# =============================================================================

import os
import sys
import glob
import subprocess
from collections import defaultdict

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

HDFS_MODEL_DIR = "/models/kmeans_anomaly_model"
LOCAL_MODEL_DIR = "data/models/kmeans_anomaly_model"

def parse_lines_to_features(lines):
    """Trích xuất vector [Traffic, Error_Rate, 5xx_Rate, Latency] theo từng giờ."""
    hour_requests = defaultdict(int)
    hour_errors = defaultdict(int)
    hour_5xx = defaultdict(int)
    hour_resp = defaultdict(float)

    for line in lines:
        line = line.strip()
        if not line or "\t" not in line:
            continue
        key, val_str = line.split("\t", 1)
        try:
            val = float(val_str)
        except ValueError:
            continue

        if key.startswith("hour_req:"):
            hour_requests[key.split(":")[1]] = int(val)
        elif key.startswith("hour_err:"):
            hour_errors[key.split(":")[1]] = int(val)
        elif key.startswith("hour_5xx:"):
            hour_5xx[key.split(":")[1]] = int(val)
        elif key.startswith("hour_resp:"):
            hour_resp[key.split(":")[1]] = val

    features_list = []
    for h in sorted(hour_requests.keys()):
        req = float(hour_requests[h])
        err = float(hour_errors.get(h, 0))
        c5xx = float(hour_5xx.get(h, 0))
        resp = float(hour_resp.get(h, 0.0))

        err_rate = (err / req * 100.0) if req > 0 else 0.0
        c5xx_rate = (c5xx / req * 100.0) if req > 0 else 0.0
        features_list.append([req, err_rate, c5xx_rate, resp])

    return features_list

def collect_historical_training_data():
    """Gom dữ liệu tổng hợp của nhiều ngày từ HDFS hoặc file local."""
    all_features = []

    # 1. Quét output trên HDFS: /data/output/*/part-*
    try:
        proc = subprocess.run(["hdfs", "dfs", "-cat", "/data/output/*/part-*"], capture_output=True, text=True)
        if proc.returncode == 0 and proc.stdout.strip():
            feats = parse_lines_to_features(proc.stdout.splitlines())
            if feats:
                all_features.extend(feats)
                print(f"[INFO] Nạp {len(feats)} mẫu từ HDFS.")
    except Exception:
        pass

    # 2. Quét file local (data/mapreduce_results*.txt)
    local_files = glob.glob("data/mapreduce_results*.txt")
    for fpath in local_files:
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                feats = parse_lines_to_features(f.readlines())
                if feats:
                    all_features.extend(feats)
                    print(f"[INFO] Nạp {len(feats)} mẫu từ: {fpath}")
        except Exception:
            continue

    return all_features

def main():
    print("==========================================================")
    print(" [LUỒNG 1] HUẤN LUYỆN MÔ HÌNH KMEANS TỪ DỮ LIỆU LỊCH SỬ")
    print("==========================================================")

    # 1. Thu thập dữ liệu
    historical_data = collect_historical_training_data()
    if not historical_data:
        print("[ERROR] Không tìm thấy dữ liệu để huấn luyện.")
        sys.exit(1)

    print(f"\n[1/5] Tổng số mẫu: {len(historical_data)} giờ.")

    # 2. Huấn luyện bằng PySpark
    try:
        from pyspark.sql import SparkSession
        from pyspark.ml.clustering import KMeans
        from pyspark.ml.evaluation import ClusteringEvaluator
        from pyspark.ml.linalg import Vectors
        from pyspark.sql import Row

        spark = SparkSession\
            .builder\
            .appName("KMeansModelTrainer")\
            .master("local[*]")\
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")

        rows = [Row(features=Vectors.dense(pt)) for pt in historical_data]
        dataset = spark.createDataFrame(rows)

        print("\n[2/5] Khởi tạo mô hình PySpark KMeans (K=2)...")
        kmeans = KMeans().setK(2).setSeed(1).setFeaturesCol("features").setPredictionCol("prediction")
        model = kmeans.fit(dataset)

        # 3. Đánh giá Silhouette
        predictions = model.transform(dataset)
        evaluator = ClusteringEvaluator(featuresCol="features", predictionCol="prediction")
        silhouette = evaluator.evaluate(predictions)
        print(f"\n[3/5] Điểm Silhouette = {silhouette:.4f}")

        # 4. Hiển thị tâm các cụm
        centers = model.clusterCenters()
        print("\n[4/5] Tọa độ tâm cụm (Cluster Centers):")
        for idx, center in enumerate(centers):
            print(f"   Cụm {idx}: Traffic={center[0]:.0f}, Lỗi={center[1]:.2f}%, 5xx={center[2]:.2f}%, Trễ={center[3]:.1f}ms")

        # 5. Lưu mô hình
        print(f"\n[5/5] Lưu mô hình:")
        os.makedirs(os.path.dirname(LOCAL_MODEL_DIR), exist_ok=True)
        model.write().overwrite().save(LOCAL_MODEL_DIR)
        print(f"   - Local: {LOCAL_MODEL_DIR}")

        try:
            hdfs_full_path = f"hdfs://localhost:9000{HDFS_MODEL_DIR}"
            model.write().overwrite().save(hdfs_full_path)
            print(f"   - HDFS : {hdfs_full_path}")
        except Exception as e:
            print(f"   (Lưu HDFS: {e})")

        spark.stop()
        print("\n[INFO] Hoàn tất huấn luyện Luồng 1.")

    except ImportError:
        print("[THÔNG BÁO] Chưa cài PySpark, dùng scikit-learn...")
        from sklearn.cluster import KMeans
        import pickle
        import numpy as np

        X = np.array(historical_data)
        km = KMeans(n_clusters=2, random_state=1, n_init=10).fit(X)

        os.makedirs("data/models", exist_ok=True)
        with open("data/models/kmeans_sklearn.pkl", "wb") as f:
            pickle.dump(km, f)

        print("[INFO] Đã lưu mô hình tại data/models/kmeans_sklearn.pkl")

if __name__ == "__main__":
    main()
