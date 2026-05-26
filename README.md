# Real-time HCMC Bus GPS Monitoring & Congestion Detection

Hệ thống mô phỏng, xử lý và trực quan hóa dữ liệu GPS xe buýt TP.HCM theo thời gian thực. Project được thiết kế như một streaming data platform quy mô nhỏ: Kafka nhận luồng GPS, Spark Structured Streaming xử lý và phát hiện tắc nghẽn, Redis làm serving layer cho dashboard realtime, HDFS lưu dữ liệu phân tích dạng Parquet, FastAPI và Leaflet hiển thị bản đồ.

## Mục Tiêu

- Theo dõi vị trí mới nhất của từng xe buýt trên bản đồ.
- Phát hiện khu vực có dấu hiệu tắc nghẽn dựa trên hành vi di chuyển chậm, mật độ điểm GPS và lịch sử trajectory.
- Cập nhật realtime updates tới dashboard thông qua Server-Sent Events (SSE).
- Lưu kết quả phân tích vào HDFS để phục vụ batch analytics, audit hoặc báo cáo sau này.

## Thành Phần Chính

| Thành phần | File / service | Vai trò |
| --- | --- | --- |
| Kafka | `kafka`, `kafka-ui` trong `docker-compose.yml` | Message broker cho topic GPS `hcmc_bus_gps`. Kafka UI mở tại `http://localhost:8090`. |
| Spark cluster | `spark-master`, `spark-worker-*` | Chạy publisher và streaming analytics jobs. Spark UI mở tại `http://localhost:8080`; app UI thường tại `http://localhost:4040`. |
| GPS publisher | `data-publisher/spark_stream_job.py` | Đọc `data-publisher/input_data/*.json`, replay theo cửa sổ thời gian và ghi message vào Kafka. |
| GPS realtime bridge | `data-consumers/tracking_gps_data.py` | Consume Kafka, chuẩn hóa payload cho UI, ghi snapshot vào Redis hash `bus_latest` và publish channel `bus_updates`. |
| Congestion detection | `data-consumers/streaming_probe_clustering.py` | Spark Structured Streaming job, giữ rolling buffer theo xe, chạy DBSCAN, tính TTI/severity, ghi Redis và HDFS. |
| Prediction pipeline | `streaming_app.py` | Pipeline thử nghiệm dùng grid aggregation và OpenCity-Mini inference để dự báo tốc độ tương lai. |
| Serving API | `main.py` | FastAPI phục vụ dashboard, REST snapshot và SSE stream. |
| Frontend map | `static/map.html` | Bản đồ Leaflet hiển thị xe buýt và cụm tắc nghẽn realtime. |
| Storage | `redis`, `namenode`, `datanode` | Redis cho serving realtime; HDFS cho batch/analytics sink. |

## Data Contract

Kafka topic mặc định: `hcmc_bus_gps`

Mỗi Kafka message là JSON GPS waypoint đã flatten:

| Field | Type | Mô tả |
| --- | --- | --- |
| `vehicle` | string | Mã định danh xe buýt. |
| `speed` | double | Vận tốc hiện tại, đơn vị km/h. |
| `datetime` | long | Unix timestamp theo giây hoặc mili-giây tùy nguồn dữ liệu. |
| `x` | double | Kinh độ. |
| `y` | double | Vĩ độ. |
| `ignition` | boolean | Trạng thái máy xe. |
| `working` | boolean | Xe đang hoạt động hay không. |
| `door_up` | boolean | Cửa lên đang mở. |
| `door_down` | boolean | Cửa xuống đang mở. |

Một số file cũ có schema bọc ngoài `msgBusWayPoint`; publisher sẽ đọc wrapper này và ghi phần `msgBusWayPoint` vào Kafka.

## Luồng Xử Lý Realtime

### 1. Ingestion

`data-publisher/spark_stream_job.py` đọc dữ liệu mẫu từ `data-publisher/input_data/`, cache vào Spark, cắt thành các cửa sổ 10 giây theo timestamp lịch sử và publish đồng loạt vào Kafka. Biến `PLAYBACK_SPEED_MULTIPLIER` cho phép tua nhanh replay.

### 2. Tracking Layer

`data-consumers/tracking_gps_data.py` consume topic GPS và ghi:

- Redis hash `bus_latest`: snapshot mới nhất theo `bus_id`.
- Redis pub/sub `bus_updates`: mỗi update realtime cho SSE.

API liên quan:

- `GET /bus_current`: lấy snapshot xe hiện tại.
- `GET /bus_stream`: SSE stream xe buýt realtime.

### 3. Congestion Detection Layer

`data-consumers/streaming_probe_clustering.py` thực hiện:

- Parse và validate GPS message.
- Lọc xe không `working`, không `ignition`, hoặc đang mở cửa đón/trả khách.
- Tạo `event_time`, watermark 5 phút và drop duplicate theo `vehicle + event_time`.
- Grid indexing với độ phân giải `0.001` độ, xấp xỉ 100m.
- `applyInPandasWithState` theo từng `vehicle` để giữ rolling buffer trajectory.
- Chạy DBSCAN khi buffer đủ điểm, gán nhãn `Congested`, `Smooth`, `Warming Up` hoặc `Dwelling`.
- Tính TTI và severity.
- Ghi HDFS Parquet partition theo `congestion_status`.
- Cập nhật Redis:
  - `congestion:clusters`
  - `congestion:cluster:{grid_id}`
  - `congestion:severity`
  - pub/sub `congestion_detection_updates`

API liên quan:

- `GET /congestion_current`: lấy danh sách cụm tắc nghẽn mới nhất, sắp xếp theo severity.
- `GET /congestion_detection_stream`: SSE stream cụm tắc nghẽn realtime.


## Setup Và Chạy Project

Tất cả lệnh bên dưới chạy trong thư mục:

```bash
cd bus-gps
```

### 1. Build Image Spark Custom

```bash
docker compose build spark-master
```

Image `custom-spark:3.5.1` cài thêm các thư viện Python cần cho streaming job như `pandas`, `numpy`, `scikit-learn`, `redis`, `pyarrow`.

### 2. Khởi Động Infrastructure

```bash
docker compose up -d kafka kafka-ui redis namenode datanode spark-master spark-worker-1 spark-worker-2 spark-worker-3
```

Kiểm tra các UI:

- Kafka UI: `http://localhost:8090`
- Spark Master UI: `http://localhost:8080`
- HDFS NameNode UI: `http://localhost:9870`

### 3. Tạo / Replay Dữ Liệu GPS Vào Kafka

Chạy Spark publisher:

```bash
bash data-publisher/submit_stream_spark_job.sh
```

Hoặc chạy trực tiếp qua Docker Compose:

```bash
MSYS_NO_PATHCONV=1 docker compose --profile submit run --rm \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  -e HADOOP_USER_NAME=root \
  spark-submit \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --total-executor-cores 2 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /app/data-publisher/spark_stream_job.py
```

Nếu cần replay nhanh hơn:

```bash
MSYS_NO_PATHCONV=1 docker compose --profile submit run --rm \
  -e PLAYBACK_SPEED_MULTIPLIER=5 \
  spark-submit \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /app/data-publisher/spark_stream_job.py
```

### 4. Chạy GPS Tracking Bridge

```bash
docker compose --profile tracking-gps up tracking-gps-data
```

Service này cập nhật Redis `bus_latest` và publish `bus_updates` cho dashboard.

### 5. Chạy Congestion Detection Job

```bash
bash data-consumers/submit_clustering_job.sh
```

Job này ghi kết quả vào:

- Redis: serving realtime cho dashboard.
- HDFS: `hdfs://namenode:9000/data/analytics/congestion_behavior`

### 6. Chạy FastAPI Dashboard

Có thể chạy trên host:

```bash
python -m venv .venv
source .venv/Scripts/activate
pip install -r requirements_fix.txt
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

Mở dashboard:

```text
http://localhost:8000
```

Nếu chạy trên Linux/macOS, câu lệnh activate là:

```bash
source .venv/bin/activate
```

## Liên hệ
- hblong.sdh242@hcmut.edu.vn
