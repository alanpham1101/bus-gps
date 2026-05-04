# Real-time HCMC Bus Monitoring & Congestion Detection System

## 1. Project Introduction
Dự án tập trung vào việc xây dựng nền tảng dữ liệu lớn (Big Data Platform) nhằm giải quyết vấn đề ùn tắc giao thông tại TP.HCM thông qua việc giám sát hệ thống xe buýt[cite: 2, 3].

* **Problem:** Giao thông TP.HCM thường xuyên kẹt xe, gây trễ chuyến xe buýt[cite: 24]. Việc thiếu công cụ giám sát trực quan khiến điều phối viên khó phát hiện nhanh các điểm nghẽn[cite: 25].
* **Solution:** Trực quan hóa tuyến đường và chuyển động của xe buýt trên bản đồ, đồng thời phát hiện khu vực tắc nghẽn giao thông trong thời gian thực[cite: 27, 29, 30].

## 2. System Architecture & Pipeline
Hệ thống được xây dựng dựa trên kiến trúc Streaming Pipeline hiện đại[cite: 43]:

**Pipeline Flow:**
`Kafka (Ingestion) -> Spark (Analysis) -> HDFS (Persistence) -> Backend/Leaflet (Visualization)`[cite: 43].

* **Kafka:** Mô phỏng luồng dữ liệu GPS (Streaming Data) từ hàng nghìn xe buýt[cite: 33].
* **Spark:** * Consume dữ liệu từ Kafka[cite: 35].
    * Xử lý và tính toán các chỉ số (metrics) thời gian thực[cite: 36, 37].
* **HDFS:** Lưu trữ dữ liệu GPS thô và dữ liệu phân tích về các điểm tắc nghẽn[cite: 38, 39, 40].
* **Backend:** Phát triển trên nền tảng Python (Flask/FastAPI)[cite: 41].
* **Frontend:** Sử dụng thư viện Leaflet để trực quan hóa dữ liệu trên bản đồ[cite: 42].

## 3. Data Specification
Dự án sử dụng bộ dữ liệu **HCMC BUS GPS DATASET**[cite: 5].

* **Format:** JSON Message (`MsgType_BusWayPoint`)[cite: 7].
* **Velocity:** Luồng dữ liệu đẩy liên tục theo giây[cite: 8].
* **Schema:** [cite: 9, 11-20]
| Field | Type | Description |
| :--- | :--- | :--- |
| `vehicle` | String | ID định danh duy nhất của xe buýt |
| `speed` | Float | Vận tốc hiện tại của xe |
| `datetime` | Long | Thời gian ghi nhận (Epoch timestamp) |
| `x` | Double | Kinh độ (Longitude) |
| `y` | Double | Vĩ độ (Latitude) |
| `ignition` | Boolean | Trạng thái nổ máy (True/False) |
| `working` | Boolean | Trạng thái xe đang làm việc (True/False) |

## 4. Analytical Requirements (Core Logic)
Coding Agent cần tập trung triển khai các logic tính toán sau trên Spark Streaming[cite: 37]:

1. **Average Segment Speed:** Tính toán vận tốc trung bình của các xe trên từng đoạn đường cụ thể để xác định tốc độ lưu thông chung.
2. **Stop Frequency:** Tần suất dừng xe tại một khu vực nhằm phân biệt giữa việc dừng trạm thông thường và việc đứng chôn chân do kẹt xe.
3. **Congestion Detection:** Thuật toán xác định "điểm nóng" (hotspots) dựa trên vận tốc thấp kéo dài và mật độ xe cao tại một tọa độ (X, Y)[cite: 27, 30].

## 5. Development Roadmap
1. **Setup Ingestion:** Cấu hình Kafka Producer để stream dữ liệu từ file JSON mẫu[cite: 7, 33].
2. **Streaming Processor:** Viết Spark job xử lý window-based metrics (tốc độ trung bình, tần suất dừng)[cite: 37].
3. **Data Sink:** Cấu hình lưu trữ vào HDFS dưới dạng Parquet hoặc Avro để tối ưu hiệu suất[cite: 40].
4. **API & Dashboard:** Xây dựng API cung cấp tọa độ xe và vùng kẹt xe cho Dashboard Leaflet[cite: 41, 42].