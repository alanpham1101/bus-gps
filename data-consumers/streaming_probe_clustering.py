"""
streaming_probe_clustering.py
==============================
HCMC Bus GPS — Real-Time Congestion Detection via DBSCAN (Bus-as-a-Probe)
Stack : PySpark 3.5+ Structured Streaming · scikit-learn DBSCAN · Redis · HDFS

════════════════════════════════════════════════════════════════════════════════
THIẾT KẾ: Stateful Vehicle Trajectory Buffer (giải quyết GPS 10s/điểm)
════════════════════════════════════════════════════════════════════════════════

Vấn đề cốt lõi:
  · Mỗi xe gửi 1 GPS point mỗi ~10 giây.
  · Micro-batch 30s → chỉ ~3 điểm/xe → KHÔNG đủ min_samples=5 cho DBSCAN.

Giải pháp — applyInPandasWithState nhóm theo vehicle:
  · Mỗi vehicle giữ một rolling buffer (tối đa 30 điểm ≈ 5 phút lịch sử).
  · Mỗi micro-batch: nhận điểm mới → append → chạy DBSCAN trên buffer đầy đủ.
  · Chỉ emit các điểm MỚI (tránh duplicate với batch trước).
  · State timeout 30 phút → tự xóa xe không hoạt động.

Pipeline
--------
  Step 1 : Kafka → Parse → Business Filter (working & ignition)
           → Dwell-Time Filter (broadcast join vs bus stops)
  Step 2 : Grid Indexing (~100m × 100m cell)
  Step 3+4: applyInPandasWithState → Stateful DBSCAN per vehicle
  Step 5 : foreachBatch → Redis (GEOADD/HSET/ZADD) + HDFS (Parquet)

Environment Variables
---------------------
  KAFKA_BOOTSTRAP_SERVERS  kafka:9092
  KAFKA_TOPIC              hcmc_bus_gps
  REDIS_HOST               redis
  REDIS_PORT               6379
  HDFS_OUTPUT_PATH         hdfs://namenode:9000/data/analytics/congestion_behavior
  BUS_STOPS_PATH           /app/data/bus_stops.csv
  CHECKPOINT_LOCATION      /tmp/checkpoints/probe_clustering
"""

from __future__ import annotations

import json
import math
import logging
import os
from datetime import datetime, timezone
from typing import Iterator, Tuple

import numpy as np
import pandas as pd
import redis
from sklearn.cluster import DBSCAN

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ─────────────────────────────────────────────────────────────────────────────
# 0.  LOGGING
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("BusProbeClusteringPipeline")

# ─────────────────────────────────────────────────────────────────────────────
# 1.  CONFIGURATION (tất cả magic-number ở một chỗ duy nhất)
# ─────────────────────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP_SERVERS: str = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC: str             = os.environ.get("KAFKA_TOPIC", "hcmc_bus_gps")
REDIS_HOST: str              = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT: int              = int(os.environ.get("REDIS_PORT", "6379"))
HDFS_OUTPUT_PATH: str        = os.environ.get(
    "HDFS_OUTPUT_PATH",
    "hdfs://namenode:9000/data/analytics/congestion_behavior",
)
BUS_STOPS_PATH: str      = os.environ.get("BUS_STOPS_PATH", "/app/data/bus_stops.csv")
CHECKPOINT_LOCATION: str = os.environ.get(
    "CHECKPOINT_LOCATION", "hdfs://namenode:9000/checkpoints/probe_clustering_v2"
)

# Spatial
GRID_RESOLUTION: float = 0.001   # ~100m per cell
LAT_M: float           = 111_320.0
COS_LAT: float         = math.cos(math.radians(10.8))  # HCMC ~10.8°N

# Dwell-time filter
DWELL_SPEED_KMH: float = 5.0
DWELL_RADIUS_M: float  = 30.0

# DBSCAN
DBSCAN_EPS: float    = 0.001  # bán kính ~100m trong không gian GPS
DBSCAN_MIN_SAMPLES   = 5      # ≥5 điểm dày đặc = xác nhận tắc nghẽn

# Stateful buffer
TRAJECTORY_BUFFER_SIZE = 30          # 30 × 10s = 5 phút lịch sử/xe
STATE_TIMEOUT_MS       = 30 * 60_000  # 30 phút không có data → evict state

# Congestion scoring
FREE_FLOW_SPEED_KMH: float = 40.0
REDIS_TTL_SECONDS          = 600   # cluster metadata tự expire sau 10 phút
REDIS_MAX_SEVERITY_KEYS    = 200   # giữ top-200 điểm kẹt nặng nhất

# ─────────────────────────────────────────────────────────────────────────────
# 2.  SCHEMA DEFINITIONS
# ─────────────────────────────────────────────────────────────────────────────

# Schema của Kafka message value (đã flatten bởi data-publisher)
MSG_SCHEMA = StructType([
    StructField("vehicle",  StringType(),  True),
    StructField("speed",    DoubleType(),  True),
    StructField("datetime", LongType(),    True),   # Unix epoch seconds
    StructField("x",        DoubleType(),  True),   # longitude
    StructField("y",        DoubleType(),  True),   # latitude
    StructField("ignition", BooleanType(), True),
    StructField("working",  BooleanType(), True),
    StructField("door_up",  BooleanType(), True),
    StructField("door_down", BooleanType(), True),
])

# State schema: 5 JSON-encoded lists (x, y, speed, timestamp, grid_id)
# Dùng 5 StringType riêng biệt — đơn giản, an toàn với applyInPandasWithState
STATE_SCHEMA = StructType([
    StructField("x_json",   StringType(), False),
    StructField("y_json",   StringType(), False),
    StructField("spd_json", StringType(), False),
    StructField("ts_json",  StringType(), False),
    StructField("gid_json", StringType(), False),
])

# Output schema của applyInPandasWithState
OUTPUT_SCHEMA = StructType([
    StructField("vehicle",           StringType(),  False),
    StructField("x",                 DoubleType(),  False),
    StructField("y",                 DoubleType(),  False),
    StructField("speed",             DoubleType(),  False),
    StructField("event_ts",          LongType(),    False),   # Unix epoch
    StructField("grid_id",           LongType(),    False),
    StructField("cluster_id",        IntegerType(), False),
    StructField("congestion_status", StringType(),  False),
    StructField("tti",               DoubleType(),  False),
    StructField("severity",          DoubleType(),  False),
    StructField("buffer_size",       IntegerType(), False),
])

# ─────────────────────────────────────────────────────────────────────────────
# 3.  CORE CLUSTERING LOGIC
# ─────────────────────────────────────────────────────────────────────────────

def _build_feature_matrix(df: pd.DataFrame) -> np.ndarray:
    """
    Xây dựng feature matrix 4D cho DBSCAN.

    Columns: [x, y, inv_speed, norm_ts]

    · x, y     — tọa độ GPS (đơn vị °, ~0.001° ≈ 100m → khớp DBSCAN_EPS)
    · inv_speed — nghịch đảo vận tốc: xe chậm → giá trị lớn → điểm "gần" nhau
                  hơn trong feature space → DBSCAN dễ gom cụm hơn ("lực hút mật độ")
    · norm_ts  — timestamp chuẩn hóa [0,1] trong window → phân biệt điểm
                  ở cùng vị trí nhưng khác thời điểm
    """
    ts_arr = df["datetime"].values.astype(float)
    TIME_WINDOW_SEC = 300.0  # 5 phút
    t_scaled = ((ts_arr - ts_arr.min()) / TIME_WINDOW_SEC) * DBSCAN_EPS

    return np.column_stack([
        df["x"].values,
        df["y"].values,
        t_scaled
    ])


def _run_dbscan(buf_df: pd.DataFrame) -> np.ndarray:
    """
    Chạy DBSCAN trên buffer trajectory.
    Trả về mảng labels (cluster_id); -1 = noise/smooth.
    """
    if len(buf_df) < DBSCAN_MIN_SAMPLES:
        return np.full(len(buf_df), -1, dtype=int)

    X = _build_feature_matrix(buf_df)
    return DBSCAN(
        eps=DBSCAN_EPS,
        min_samples=DBSCAN_MIN_SAMPLES,
        algorithm="ball_tree",
        n_jobs=-1,
    ).fit_predict(X)


def _compute_severity(cluster_ids: pd.Series, tti: pd.Series) -> pd.Series:
    """
    Tính severity score (vectorized).

    severity = density_factor × tti_factor
    · density_factor = clamp(cluster_size / min_samples, 1, 5)
    · tti_factor     = clamp(tti / 2.0, 0, 5)

    Xe không bị kẹt (cluster_id == -1) → severity = 0.0
    """
    severity = pd.Series(0.0, index=cluster_ids.index)
    mask     = cluster_ids >= 0

    if not mask.any():
        return severity

    # Đếm số điểm mỗi cluster trong toàn buffer (để tính density)
    cluster_sizes = cluster_ids[mask].map(cluster_ids[mask].value_counts())
    density = cluster_sizes.clip(upper=DBSCAN_MIN_SAMPLES * 5) / DBSCAN_MIN_SAMPLES
    density = density.clip(lower=1.0, upper=5.0)

    tti_factor = (tti[mask] / 2.0).clip(lower=0.0, upper=5.0)
    severity[mask] = (density * tti_factor).round(2)
    return severity


# ─────────────────────────────────────────────────────────────────────────────
# 4.  STATE SERIALIZATION HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _load_state(state: GroupState) -> dict[str, list]:
    """Đọc state buffer từ Spark GroupState → Python dict of lists."""
    if not state.exists:
        return {"x": [], "y": [], "spd": [], "ts": [], "gid": []}
    s = state.get
    return {
        "x":   json.loads(s[0]),
        "y":   json.loads(s[1]),
        "spd": json.loads(s[2]),
        "ts":  json.loads(s[3]),
        "gid": json.loads(s[4]),
    }


def _save_state(state: GroupState, buf: dict[str, list]) -> None:
    """Ghi buffer về Spark GroupState và set timeout."""
    state.update((
        json.dumps(buf["x"]),
        json.dumps(buf["y"]),
        json.dumps(buf["spd"]),
        json.dumps(buf["ts"]),
        json.dumps(buf["gid"]),
    ))
    state.setTimeoutDuration(STATE_TIMEOUT_MS)


def _append_and_trim(buf: dict[str, list], new_rows: pd.DataFrame) -> dict[str, list]:
    """Append điểm mới vào buffer và rolling-trim về TRAJECTORY_BUFFER_SIZE."""
    return {
        "x":   (buf["x"]   + new_rows["x"].tolist())[-TRAJECTORY_BUFFER_SIZE:],
        "y":   (buf["y"]   + new_rows["y"].tolist())[-TRAJECTORY_BUFFER_SIZE:],
        "spd": (buf["spd"] + new_rows["speed"].fillna(0.0).tolist())[-TRAJECTORY_BUFFER_SIZE:],
        "ts":  (buf["ts"]  + new_rows["datetime"].tolist())[-TRAJECTORY_BUFFER_SIZE:],
        "gid": (buf["gid"] + new_rows["grid_id"].tolist())[-TRAJECTORY_BUFFER_SIZE:],
    }


# ─────────────────────────────────────────────────────────────────────────────
# 5.  STATEFUL FUNCTION — applyInPandasWithState
# ─────────────────────────────────────────────────────────────────────────────

def accumulate_and_cluster(
    key: Tuple,
    pdf_iter: Iterator[pd.DataFrame],
    state: GroupState,
) -> Iterator[pd.DataFrame]:
    """
    Stateful function nhóm theo vehicle_id.

    Quy trình mỗi micro-batch:
      ① Đọc buffer lịch sử của xe từ state.
      ② Gộp và sort các điểm mới theo thời gian.
      ③ Append vào buffer, trim về TRAJECTORY_BUFFER_SIZE.
      ④ Chạy DBSCAN trên toàn buffer (lịch sử + mới).
      ⑤ Emit CHỈ các điểm mới với cluster label được gán.
      ⑥ Lưu buffer mới vào state.

    Tại sao chỉ emit điểm mới?
    → Điểm cũ đã emit ở batch trước → tránh duplicate trong sink.
    → Output mode = append → không được re-emit.
    """
    vehicle_id: str = key[0]

    # ① Load state
    buf = _load_state(state)

    # ② Collect & sort new points
    new_rows = (
        pd.concat(list(pdf_iter), ignore_index=True)
        .sort_values("datetime")
        .reset_index(drop=True)
    )
    n_new = len(new_rows)

    # ③ Append & trim
    buf = _append_and_trim(buf, new_rows)

    # ④ Build full buffer DataFrame & run DBSCAN
    full_buf = pd.DataFrame({
        "x":        buf["x"],
        "y":        buf["y"],
        "speed":    buf["spd"],
        "datetime": buf["ts"],
        "grid_id":  buf["gid"],
    })
    all_labels = _run_dbscan(full_buf)

    # ⑤ Emit new points only (tail of buffer)
    emit = full_buf.tail(n_new).copy().reset_index(drop=True)
    emit["cluster_id"] = all_labels[-n_new:]
    emit["vehicle"]    = vehicle_id

    # Derived fields
    emit["congestion_status"] = np.where(emit["cluster_id"] >= 0, "Congested", "Smooth")
    emit["tti"]      = (FREE_FLOW_SPEED_KMH / emit["speed"].clip(lower=1.0)).round(3)
    emit["severity"] = _compute_severity(emit["cluster_id"], emit["tti"])
    emit["buffer_size"] = len(buf["x"])

    # Rename datetime → event_ts to avoid conflict with Python built-in
    emit = emit.rename(columns={"datetime": "event_ts"})

    output_cols = [
        "vehicle", "x", "y", "speed", "event_ts", "grid_id",
        "cluster_id", "congestion_status", "tti", "severity", "buffer_size",
    ]

    # ⑥ Save state
    _save_state(state, buf)

    yield emit[output_cols]


# ─────────────────────────────────────────────────────────────────────────────
# 6.  SINK IMPLEMENTATIONS
# ─────────────────────────────────────────────────────────────────────────────

def _write_partition_to_redis(partition, batch_id: int, redis_host: str, redis_port: int, ttl: int, max_severity_keys: int) -> None:
    """Writes a partition of aggregated congestion data directly from worker to Redis."""
    import redis
    from datetime import datetime, timezone
    
    rows = list(partition)
    if not rows:
        return
        
    try:
        r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        pipe = r.pipeline(transaction=False)

        for row in rows:
            sgid = str(int(row.grid_id))
            cx   = float(row.cx)
            cy   = float(row.cy)
            sev  = float(row.severity)

            pipe.geoadd("congestion:clusters", [cx, cy, sgid])
            pipe.hset(f"congestion:cluster:{sgid}", mapping={
                "status":       "Congested",
                "avg_speed":    f"{row.avg_speed:.2f}",
                "avg_tti":      f"{row.avg_tti:.2f}",
                "severity":     f"{sev:.2f}",
                "point_count":  str(row.point_count),
                "vehicles":     str(row.vehicles),
                "centroid_lon": f"{cx:.6f}",
                "centroid_lat": f"{cy:.6f}",
                "updated_at":   datetime.now(timezone.utc).isoformat(),
                "batch_id":     str(batch_id),
            })
            pipe.expire(f"congestion:cluster:{sgid}", ttl)
            pipe.zadd("congestion:severity", {sgid: sev})
            pipe.zremrangebyrank("congestion:severity", 0, -(max_severity_keys + 1))

        pipe.execute()
    except Exception as exc:
        print(f"Worker Redis write failed: {exc}") # Print to worker stdout/logs


def write_to_sinks(batch_df: DataFrame, batch_id: int) -> None:
    """foreachBatch handler — Redis + HDFS (Distributed)."""
    if batch_df.rdd.isEmpty():
        logger.info("Batch %d: empty — skipping.", batch_id)
        return

    # Cache to avoid recomputation if actions are triggered multiple times
    batch_df.cache()
    
    total_points = batch_df.count()

    # 1. HDFS Sink (Distributed)
    hdfs_df = batch_df.withColumn("batch_id", F.lit(batch_id)) \
                      .withColumn("processed_at", F.current_timestamp())

    try:
        hdfs_df.write.mode("append") \
               .partitionBy("congestion_status") \
               .parquet(HDFS_OUTPUT_PATH)
        logger.info("Batch %d | HDFS: %d rows written to %s", batch_id, total_points, HDFS_OUTPUT_PATH)
    except Exception as exc:
        logger.error("Batch %d | HDFS write failed: %s", batch_id, exc)

    # 2. Redis Sink (Distributed)
    congested_df = batch_df.filter(F.col("congestion_status") == "Congested")
    
    if congested_df.rdd.isEmpty():
        logger.info("Batch %d | %d total points | 0 congested", batch_id, total_points)
        batch_df.unpersist()
        return

    # Pre-aggregate directly on Spark (Distributed)
    agg_df = congested_df.groupBy("grid_id").agg(
        F.mean("x").alias("cx"),
        F.mean("y").alias("cy"),
        F.mean("speed").alias("avg_speed"),
        F.mean("tti").alias("avg_tti"),
        F.max("severity").alias("severity"),
        F.count("*").alias("point_count"),
        F.countDistinct("vehicle").alias("vehicles")
    )
    
    congested_cells_count = agg_df.count()
    logger.info(
        "Batch %d | %d total points | %d congested grid cells",
        batch_id,
        total_points,
        congested_cells_count,
    )

    # Use foreachPartition to write to Redis from worker nodes
    agg_df.foreachPartition(
        lambda partition: _write_partition_to_redis(
            partition, 
            batch_id, 
            REDIS_HOST, 
            REDIS_PORT, 
            REDIS_TTL_SECONDS, 
            REDIS_MAX_SEVERITY_KEYS
        )
    )
    
    batch_df.unpersist()


# ─────────────────────────────────────────────────────────────────────────────
# 7.  MAIN PIPELINE
# ─────────────────────────────────────────────────────────────────────────────

def _build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("HCMC_BusProbeClusteringPipeline")
        # Arrow: tăng tốc Pandas UDF / applyInPandasWithState 40-60%
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        # Tránh 200-partition default trên cluster nhỏ
        .config("spark.sql.shuffle.partitions", "8")
        # RocksDB: hiệu quả bộ nhớ hơn cho stateful streaming
        .config(
            "spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
        )
        .getOrCreate()
    )


def _build_ingestion_pipeline(spark: SparkSession) -> DataFrame:
    """
    Step 1: Kafka → Parse → Filter → Dwell-Time Filter.
    Trả về DataFrame streaming đã lọc sạch.
    """
    # 1a. Đọc danh sách trạm xe buýt và tạo SQL Expression để tránh Streaming Aggregation
    # (Đã comment lại theo yêu cầu, hiện tại chỉ dùng door_up/door_down)
    # bus_stops = spark.read.option("header", "true").option("inferSchema", "true").csv(BUS_STOPS_PATH)
    # stops = bus_stops.collect()
    # 
    # distance_conds = []
    # for row in stops:
    #     distance_conds.append(f"(sqrt(pow((y - {row.stop_y}) * {LAT_M}, 2) + pow((x - {row.stop_x}) * {LAT_M * COS_LAT}, 2)) < {DWELL_RADIUS_M})")
    # 
    # distance_expr = " OR ".join(distance_conds) if distance_conds else "false"

    # 1b. Kafka source
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # 1c. Parse JSON + null guard
    parsed = (
        raw
        .select(F.from_json(F.col("value").cast("string"), MSG_SCHEMA).alias("d"))
        .select(
            "d.vehicle",
            F.coalesce(F.col("d.speed"), F.lit(0.0)).alias("speed"),
            F.col("d.datetime"),
            "d.x", "d.y", "d.ignition", "d.working", "d.door_up", "d.door_down"
        )
        .filter(
            F.col("vehicle").isNotNull()
            & F.col("x").isNotNull()
            & F.col("y").isNotNull()
            & F.col("datetime").isNotNull()
        )
        # Business filter: chỉ xe đang lưu thông (coalesce to True if missing)
        .filter(F.coalesce(F.col("working"), F.lit(True)) & F.coalesce(F.col("ignition"), F.lit(True)))
        # Business filter: loại bỏ dữ liệu khi xe đang mở cửa đón/trả khách (coalesce to False if missing)
        .filter(~(F.coalesce(F.col("door_up"), F.lit(False)) | F.coalesce(F.col("door_down"), F.lit(False))))
        # Thêm event_time dạng Timestamp (cần cho withWatermark)
        .withColumn("event_time", F.to_timestamp(F.col("datetime")))
    )

    # 1d. Dwell-time filter: loại "dừng đón khách" gần trạm (dùng SQL Expr thay vì Join + GroupBy)
    # (Đã comment lại theo yêu cầu, hiện tại bỏ qua logic filter theo trạm)
    # clean_stream = parsed.withColumn(
    #     "is_dwell", 
    #     F.expr(f"(speed < {DWELL_SPEED_KMH}) AND ({distance_expr})")
    # ).filter(~F.col("is_dwell")).drop("is_dwell")

    return parsed


def main() -> None:
    spark = _build_spark()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession started: %s", spark.sparkContext.appName)

    # Step 1 — Ingestion & Filter
    clean_stream = _build_ingestion_pipeline(spark)

    # Step 2 — Grid Indexing
    gridded = clean_stream.withColumn(
        "grid_id",
        (
            F.floor(F.col("x") / GRID_RESOLUTION) * F.lit(1000)
            + F.floor(F.col("y") / GRID_RESOLUTION)
        ).cast("long"),
    )

    # Step 3+4 — Stateful DBSCAN per vehicle
    # withWatermark trên event_time (TimestampType) — đúng kiểu dữ liệu
    labeled = (
        gridded
        .withWatermark("event_time", "5 minutes")
        .groupBy("vehicle")
        .applyInPandasWithState(
            func=accumulate_and_cluster,
            outputStructType=OUTPUT_SCHEMA,
            stateStructType=STATE_SCHEMA,
            outputMode="append",
            timeoutConf=GroupStateTimeout.ProcessingTimeTimeout,
        )
    )

    # Step 5 — Multi-Sink
    query = (
        labeled
        .writeStream
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .foreachBatch(write_to_sinks)
        .trigger(processingTime="30 seconds")
        .start()
    )

    logger.info("Streaming query started — ID: %s", query.id)
    query.awaitTermination()


if __name__ == "__main__":
    main()
