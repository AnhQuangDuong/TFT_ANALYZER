# TFT_Analyzer

Dự án phân tích dữ liệu TFT với pipeline gồm crawler → streaming (speed layer and save data to hdfs) → batch layer → serving (Streamlit). Các bước dưới đây hướng dẫn chạy nhanh trên máy local với Minikube, tập trung vào Docker Compose và các deployment Kubernetes.

## 1) Chuẩn bị
- Cài Docker, Docker Compose, kubectl, Minikube, Java/Hadoop CLI.
- Kích hoạt venv nếu cần: `source venv/bin/activate`.
- Kiểm tra/điều chỉnh IP host trong [docker-compose.yml](docker-compose.yml) (ở VM hiện tại thì mặc định là 192.168.200.128 cho Kafka/Mongo). Nếu VM thay đổi IP, sửa `KAFKA_CFG_ADVERTISED_LISTENERS` cho từng broker tương ứng.

## 2) Khởi động hạ tầng nền (Kafka + Mongo + Minikube + HDFS)
- Chạy script khởi tạo: `./run_init_env.sh`.
	- Script sẽ: start Minikube, `docker compose up -d` (Kafka 3 brokers, Zookeeper, Mongo), start HDFS, chạy `admin.py`, reset các collection Mongo liên quan.
- Xác nhận dịch vụ: `docker ps` (kafka1/2/3, zookeeper, mongodb) và `minikube status`.

## 3) Triển khai các thành phần Kubernetes

### a. Crawler (lấy dữ liệu Riot → Kafka)
- Thư mục: [k8s_crawler](k8s_crawler).
- Cập nhật API key trong [k8s_crawler/deployment.yaml](k8s_crawler/deployment.yaml) biến `API_RIOT`.
- Build và load image:
	- `cd k8s_crawler`
	- `docker build -t riot-crawler:latest .`
	- `minikube image load riot-crawler:latest`
- Deploy job: `kubectl apply -f deployment.yaml`.
- Theo dõi: `kubectl logs -f job/riot-crawler-job`.

### b. Speed layer (Spark Streaming → HDFS/Mongo)
- Thư mục: [k8s_streaming_speedlayer](k8s_streaming_speedlayer).
- Kiểm tra biến môi trường trong [k8s_streaming_speedlayer/deployment.yaml](k8s_streaming_speedlayer/deployment.yaml) (Kafka, HDFS, Mongo URI).
- Build và load image:
	- `cd k8s_streaming_speedlayer`
	- `docker build -t spark-streaming:latest .`
	- `minikube image load spark-streaming:latest`
- Deploy: `kubectl apply -f deployment.yaml`.
- Logs: `kubectl logs -f deployment/spark-streaming-deployment`.

### c. Batch layer (đọc HDFS → phân tích → Mongo)
- Thư mục: [k8s_batchlayer](k8s_batchlayer).
- Kiểm tra `HDFS_NAMENODE`, `MONGODB_URI` trong [k8s_batchlayer/deployment.yaml](k8s_batchlayer/deployment.yaml).
- Build và load image:
	- `cd k8s_batchlayer`
	- `docker build -t tft-batch-layer:latest .`
	- `minikube image load tft-batch-layer:latest`
- Deploy CronJob/Job: `kubectl apply -f deployment.yaml` (CronJob mặc định mỗi 5 phút, dùng cho DEMO). Có thể tạo job thủ công: `kubectl create job --from=cronjob/tft-batch-layer tft-batch-manual-$(date +%s)`.
- Logs: `kubectl logs -l app=tft-batch-layer --tail=100`.

### d. Serving (Streamlit UI từ Mongo)
- Thư mục: [k8s_serving](k8s_serving).
- Chạy script 'deploy_and_access.sh' để load image, deploy và port-forward cổng để có thể truy cập được.

## 4) Dọn dẹp môi trường
- Chạy `./run_clear_env.sh` để xóa Kafka topic `match_history`, dừng Minikube, `docker compose down -v`, và stop HDFS.
- Nếu cần xóa resource K8s thủ công: `kubectl delete -f deployment.yaml` trong từng thư mục k8s_*.

## 5) Thứ tự chạy gợi ý
1. `./run_init_env.sh` (hạ tầng nền)
2. Deploy serving (xem UI)
3. Deploy speed layer (ghi HDFS + Mongo speed)
4. Deploy crawler (đẩy dữ liệu vào Kafka)
5. Deploy batch layer (phân tích từ HDFS vào Mongo)

## 6) Troubleshooting nhanh
- Kafka không reachable từ pod: đảm bảo dùng IP host (không phải localhost) trong env và deployment.
- Thiếu dữ liệu HDFS: kiểm tra speed layer đã chạy, đường dẫn `/tft/.../stream_output` tồn tại.
- UI không lên: kiểm tra pod serving chạy, thử `kubectl logs -l app=tft-serving-layer` và port-forward đúng IP.
