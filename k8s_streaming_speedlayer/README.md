# Spark Streaming to HDFS & MongoDB (Speed Layer)

Streaming job đọc Kafka topic `match_history`, xử lý Speed Layer (thống kê top4, phân bố hạng) và ghi từng trận vào HDFS cho Batch Layer.

## Yêu cầu
- Kubernetes (Minikube hoặc cluster)
- Docker
- Đã có Kafka, HDFS, MongoDB đang chạy (theo docker compose ở thư mục gốc)
- Mạng internet để Spark tự tải `spark-sql-kafka-0-10` package (lần chạy đầu)

## Cấu trúc thư mục
```
k8s_streaming/
├── test_spark_streaming_to_hdfs_and_speed_layer.py  # Job streaming
├── Dockerfile                                       # Build image
├── requirements.txt                                 # Python deps
├── deployment.yaml                                  # K8s Deployment
└── README.md
```

## Biến môi trường
- `KAFKA_BOOTSTRAP_SERVERS` (mặc định: `192.168.200.128:9093`)
- `KAFKA_TOPIC` (mặc định: `match_history`)
- `HDFS_NAMENODE` (mặc định: `hdfs://192.168.200.128:9000`)
- `MONGODB_URI` (mặc định: `mongodb://192.168.200.128:27017/`)
- `BATCH_TRIGGER_SECONDS` (mặc định: `30` giây)

## Build image (đã đóng gói sẵn .jar)
```bash
cd k8s_streaming
# Build
docker build -t spark-streaming:latest .
# Nếu dùng Minikube
minikube image load spark-streaming:latest
```

**Ghi chú:** Image đã tự động tải các Kafka/Hadoop jars vào `/app/jars`, nên khi chạy trong pod không cần internet và không cần mount `~/.ivy2`.

## Deploy
```bash
# Apply deployment
kubectl apply -f deployment.yaml

# Xem pod
kubectl get pods
kubectl logs -f deployment/spark-streaming-deployment
```

## Dừng / Cleanup
```bash
kubectl delete -f deployment.yaml
```

## Troubleshooting
- Lỗi kết nối Kafka: kiểm tra Kafka đang chạy và env `KAFKA_BOOTSTRAP_SERVERS`
- Lỗi kết nối HDFS: kiểm tra `HDFS_NAMENODE` và quyền truy cập
- Lỗi kết nối MongoDB: kiểm tra `MONGODB_URI`
- Lần chạy đầu Spark tải package Kafka: cần internet, đợi vài phút
- Checkpoint path: `${HDFS_NAMENODE}/tft/checkpoints`
