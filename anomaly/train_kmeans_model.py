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

    # Quét danh sách các thư mục ngày trên HDFS: /data/output/*/part-*
    try:
        ls_proc = subprocess.run(["hdfs", "dfs", "-ls", "-d", "/data/output/*"], capture_output=True, text=True)
        if ls_proc.returncode == 0 and ls_proc.stdout.strip():
            day_dirs = [line.split()[-1] for line in ls_proc.stdout.splitlines() if "/data/output/" in line]
            for day_dir in day_dirs:
                cat_proc = subprocess.run(["hdfs", "dfs", "-cat", f"{day_dir}/part-*"], capture_output=True, text=True)
                if cat_proc.returncode == 0 and cat_proc.stdout.strip():
                    feats = parse_lines_to_features(cat_proc.stdout.splitlines())
                    if feats:
                        all_features.extend(feats)
                        day_name = os.path.basename(day_dir)
                        print(f"[INFO] Nạp {len(feats)} khung giờ từ ngày {day_name} trên HDFS.")
    except Exception as e:
        print(f"[CẢNH BÁO] Không thể đọc dữ liệu từ HDFS: {e}")

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

        builder = SparkSession.builder.appName("KMeansModelTrainer")
        # Nếu có cấu hình SPARK_MASTER (ví dụ 'yarn') thì dùng, nếu không thì lấy từ spark-submit hoặc fallback local[*]
        spark_master = os.getenv("SPARK_MASTER")
        if spark_master:
            builder = builder.master(spark_master)

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        rows = [Row(features=Vectors.dense(pt)) for pt in historical_data]
        # Ghim tập dữ liệu trực tiếp trên RAM của các worker nodes để tối ưu tính toán lặp
        dataset = spark.createDataFrame(rows).cache()

        print("\n[2/5] Huấn luyện mô hình PySpark KMeans trên RAM (K=2)...")
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

        # 5. Lưu mô hình trực tiếp lên HDFS
        print(f"\n[5/5] Lưu mô hình lên HDFS:")
        hdfs_full_path = f"hdfs://localhost:9000{HDFS_MODEL_DIR}"
        model.write().overwrite().save(hdfs_full_path)
        print(f"   - HDFS : {hdfs_full_path}")

        spark.stop()
        print("\n[INFO] Hoàn tất huấn luyện Luồng 1 trên cụm.")

    except Exception as e:
        print(f"[LỖI] Quá trình huấn luyện PySpark thất bại: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
