import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, BooleanType
import pyspark.sql.functions as F

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC_NAME        = os.environ.get("KAFKA_TOPIC", "hcmc_bus_gps")
INPUT_PATH              = os.environ.get("INPUT_PATH", "/app/data-publisher/input_data/")

# Hệ số tua nhanh thời gian (1 = Chạy đúng 10s thực tế, 2 = Chạy nhanh gấp đôi tức 5s gửi 1 lần)
PLAYBACK_SPEED_MULTIPLIER = float(os.environ.get("PLAYBACK_SPEED_MULTIPLIER", "1.0"))

def main():
    spark = SparkSession.builder \
        .appName("BusGPS_BurstSimulator_Pro") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")
    
    # 1. Define Strict Schema
    msg_bus_way_point_schema = StructType([
        StructField("vehicle", StringType(), True),
        StructField("speed", DoubleType(), True),
        StructField("datetime", LongType(), True),
        StructField("x", DoubleType(), True),
        StructField("y", DoubleType(), True),
        StructField("ignition", BooleanType(), True),
        StructField("working", BooleanType(), True),
        StructField("door_up", BooleanType(), True),
        StructField("door_down", BooleanType(), True)
    ])
    
    root_schema = StructType([
        StructField("msgBusWayPoint", msg_bus_way_point_schema, True)
    ])
    
    # 2. Đọc và CACHE toàn bộ dữ liệu vào RAM (Cực kỳ quan trọng để lặp nhiều lần)
    print(f"[INFO] Reading and caching data from {INPUT_PATH}...")
    df = spark.read \
        .option("multiLine", "true") \
        .schema(root_schema) \
        .json(INPUT_PATH).cache()
        
    total_records = df.count()
    if total_records == 0:
        print("[ERROR] Không tìm thấy dữ liệu!")
        return
        
    # 3. Quét tìm mốc thời gian lịch sử
    time_stats = df.select(
        F.min("msgBusWayPoint.datetime").alias("abs_min_t"),
        F.max("msgBusWayPoint.datetime").alias("max_t")
    ).collect()[0]
    
    abs_min_t = time_stats["abs_min_t"]
    max_t = time_stats["max_t"]
    
    # Nhận diện đơn vị thời gian: Nếu Unix > 20 tỷ thì là mili-giây
    is_millis = max_t > 20000000000
    step = 10000 if is_millis else 10
    
    # BỎ QUA OUTLIER: Trong dữ liệu có 1 xe (0f6408d...) bắt đầu chạy từ rất sớm (cách các xe khác tới hơn 4 tiếng).
    # Nếu bắt đầu từ abs_min_t, hệ thống sẽ chạy 4 tiếng đồng hồ chỉ với 1 xe duy nhất!
    # Giải pháp: Nhảy thẳng đến khoảng thời gian đông đúc (Ví dụ: 2 tiếng trước khi kết thúc log).
    time_offset = 7200000 if is_millis else 7200 # 2 tiếng
    min_t = max(abs_min_t, max_t - time_offset)
    
    print(f"[STAT] Tổng số records: {total_records}")
    print(f"[STAT] Min Time: {min_t} | Max Time: {max_t} | Đơn vị: {'Mili-giây' if is_millis else 'Giây'}")
    print(f"\n[START] Bắt đầu giả lập Đợt sóng Burst Traffic (Nhiều xe gửi đồng loạt mỗi 10s)...")
    print(f"[START] Topic: {KAFKA_TOPIC_NAME} | Tốc độ Playback: {PLAYBACK_SPEED_MULTIPLIER}x")
    
    current_t = min_t
    window_counter = 1
    
    # 4. Vòng lặp thời gian thực (Time-Window Loop)
    while current_t <= max_t:
        loop_start_time = time.time()
        
        # Cắt ra cục dữ liệu trong đúng cửa sổ 10 giây này
        chunk_df = df.filter(
            (F.col("msgBusWayPoint.datetime") >= current_t) &
            (F.col("msgBusWayPoint.datetime") < current_t + step)
        )
        
        chunk_count = chunk_df.count()
        
        if chunk_count > 0:
            # Transform và Đẩy ĐỒNG LOẠT vào Kafka
            kafka_df = chunk_df.select(
                F.col("msgBusWayPoint.vehicle").cast("string").alias("key"),
                F.to_json(F.col("msgBusWayPoint")).alias("value")
            )
            
            kafka_df.write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
                .option("topic", KAFKA_TOPIC_NAME) \
                .save()
                
            print(f"[PUBLISHER] {time.strftime('%H:%M:%S')} | Cửa sổ {window_counter} | Đã bắn ĐỒNG LOẠT {chunk_count} tọa độ xe vào Kafka.")
            
            # Tính toán độ trễ Spark và ngủ bù để chu kỳ chính xác 10s ngoài đời
            execution_time = time.time() - loop_start_time
            target_sleep = (10.0 / PLAYBACK_SPEED_MULTIPLIER) - execution_time
            
            if target_sleep > 0:
                time.sleep(target_sleep)
        else:
            # Nếu cửa sổ 10s này không có xe nào chạy (dữ liệu rỗng), bỏ qua việc sleep để tua nhanh
            pass
            
        current_t += step
        window_counter += 1

    print("[SUCCESS] Đã hoàn thành toàn bộ kịch bản giả lập Burst Traffic!")

if __name__ == "__main__":
    main()
