# Riot Data Crawler - Kubernetes Job

Ứng dụng crawl dữ liệu từ Riot Games API và gửi vào Kafka topic dưới dạng Kubernetes Job (chạy một lần).

## Yêu cầu

- Minikube hoặc Kubernetes cluster
- Docker
- Riot Games API Key (cập nhật hàng ngày)
- Kafka đang chạy và accessible từ pods

## Cấu trúc thư mục

```
k8s_testCrawl/
├── test_crawlDataRiot.py   # Script chính crawl dữ liệu
├── Dockerfile               # Docker image definition
├── requirements.txt         # Python dependencies
├── deployment.yaml          # Kubernetes Job config
└── README.md               # Tài liệu này
```

## Hướng dẫn triển khai

### 0. Khởi động Kafka bằng Docker Compose

**QUAN TRỌNG**: Trước khi deploy crawler lên Kubernetes, bạn cần khởi động Kafka cluster trên host machine:

```bash
# Di chuyển về thư mục gốc của project
cd /home/anhdq/bigdata_project

# Khởi động Kafka cluster (3 brokers), Zookeeper, và MongoDB
docker compose up -d

# Kiểm tra các service đã chạy
docker ps

# Kiểm tra logs của Kafka
docker logs kafka2
```

Kafka sẽ chạy trên host machine với các ports:
- kafka1: `192.168.200.128:9092`
- kafka2: `192.168.200.128:9093` (crawler sẽ kết nối tới broker này)
- kafka3: `192.168.200.128:9094`

### 1. Cập nhật API Key

**Lưu ý**: API key trong file [deployment.yaml](deployment.yaml) cần được cập nhật hàng ngày.

```yaml
env:
- name: API_RIOT
  value: "YOUR_RIOT_API_KEY_HERE"  # Cập nhật key mới tại đây
```

### 2. Build Docker Image

```bash
# Di chuyển vào thư mục
cd k8s_crawler

# Build image
docker build -t riot-crawler:latest .

# Nếu dùng Minikube, load image vào Minikube
minikube image load riot-crawler:latest
```

### 3. Chạy Job trên Kubernetes

```bash
# Chạy job
kubectl apply -f deployment.yaml

# Hoặc chạy trực tiếp
kubectl create -f deployment.yaml
```

### 4. Kiểm tra trạng thái

```bash
# Xem jobs
kubectl get jobs

# Xem pods của job
kubectl get pods

# Xem logs của crawler (theo dõi real-time)
kubectl logs -f job/riot-crawler-job

# Hoặc xem logs từ pod name
kubectl logs -f <pod-name>

# Kiểm tra trạng thái chi tiết của job
kubectl describe job riot-crawler-job
```

### 5. Xóa Job

```bash
# Xóa job (sẽ xóa cả pods liên quan)
kubectl delete -f deployment.yaml

# Hoặc xóa trực tiếp bằng tên
kubectl delete job riot-crawler-job

# Chạy lại job (cần xóa job cũ trước)
kubectl delete job riot-crawler-job
kubectl apply -f deployment.yaml
```

## Đặc điểm của Job

- **Chạy một lần**: Job sẽ chạy crawler một lần và kết thúc khi hoàn thành
- **Tự động retry**: Nếu pod fail, Job sẽ tự động tạo pod mới (tối đa 3 lần - backoffLimit)
- **Completions**: Job cần 1 pod hoàn thành thành công
- **RestartPolicy**: OnFailure - Pod sẽ restart nếu container bị lỗi

## Cấu hình

### Environment Variables

- `KAFKA_BOOTSTRAP_SERVERS`: Địa chỉ Kafka broker (mặc định: 192.168.200.128:9093)
- `API_RIOT`: Riot Games API key (cập nhật trực tiếp trong deployment.yaml)

### Kafka Configuration

Crawler kết nối tới Kafka cluster chạy trên **host machine** (không phải trong Kubernetes).

**Cấu hình mặc định:**
- Bootstrap server: `192.168.200.128:9093` (kafka2 broker)
- Topic: `match_history`

**Cách thay đổi Kafka broker:**

Có 2 cách:

**Cách 1**: Sửa trực tiếp trong [deployment.yaml](deployment.yaml):
```yaml
env:
- name: KAFKA_BOOTSTRAP_SERVERS
  value: "192.168.200.128:9092"  # Đổi sang kafka1
```

**Cách 2**: Sửa trong [test_crawlDataRiot.py](test_crawlDataRiot.py):
```python
kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', '192.168.200.128:9092')
```

**Lưu ý về kết nối từ Kubernetes pod tới Docker Compose:**
- Kubernetes pod chạy trong Minikube (IP: 192.168.49.2)
- Kafka chạy trên host machine (IP: 192.168.200.128)
- Pod có thể kết nối tới host qua IP: 192.168.200.128
- **Không dùng** `localhost` vì trong pod, localhost là chính pod đó!

### Crawler Configuration

Các tham số trong file `test_crawlDataRiot.py`:

- `num_crawled_players`: Số lượng players cần crawl (mặc định: 15)
- `startTime`: Thời gian bắt đầu lọc match ("2025-08-01 00:00:00")
- `count`: Số match mỗi player (mặc định: 10)

## Troubleshooting

### Pod không khởi động

```bash
# Kiểm tra chi tiết pod
kubectl describe pod <pod-name>

# Kiểm tra logs
kubectl logs <pod-name>
```

### Lỗi kết nối Kafka

**Triệu chứng:**
```
KafkaConnectionError: Unable to bootstrap from [('localhost', 9093)]
```

**Nguyên nhân và giải pháp:**

1. **Kafka chưa chạy trên host:**
   ```bash
   cd /home/anhdq/bigdata_project
   docker compose up -d
   docker ps  # Kiểm tra kafka1, kafka2, kafka3 đang chạy
   ```

2. **Sai địa chỉ Kafka trong code:**
   - Kiểm tra file [test_crawlDataRiot.py](test_crawlDataRiot.py)
   - Đảm bảo dùng IP host: `192.168.200.128:9093`
   - **KHÔNG dùng** `localhost` hoặc `127.0.0.1`

3. **Kafka chưa sẵn sàng:**
   ```bash
   # Kiểm tra logs của Kafka
   docker logs kafka2
   
   # Đợi vài giây để Kafka khởi động hoàn toàn
   ```

4. **Test kết nối từ bên trong pod:**
   ```bash
   # Lấy tên pod
   POD_NAME=$(kubectl get pods -l app=riot-crawler -o jsonpath='{.items[0].metadata.name}')
   
   # Exec vào pod
   kubectl exec -it $POD_NAME -- /bin/bash
   
   # Trong pod, test kết nối
   python3 -c "import socket; socket.create_connection(('192.168.200.128', 9093), timeout=5)"
   ```

### Lỗi API Rate Limit

Script có xử lý retry khi gặp rate limit (429). Nếu vẫn lỗi:
- Giảm `num_crawled_players` 
- Tăng thời gian chờ giữa các request
- Kiểm tra API key còn hạn sử dụng

## Dependencies

- requests==2.31.0
- beautifulsoup4==4.12.2
- python-dotenv==1.0.0
- kafka-python==2.0.2
- pytz==2023.3
