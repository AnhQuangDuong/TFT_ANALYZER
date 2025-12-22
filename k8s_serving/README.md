# TFT Serving Layer - Kubernetes Deployment

Serving layer triển khai ứng dụng Streamlit để visualize dữ liệu TFT từ MongoDB. Ứng dụng hiển thị:
1. **Batch Layer** - Phân tích lịch sử từ compositions, units, traits, items
2. **Speed Layer** - Dữ liệu real-time về player analytics

## 🚀 Quick Start - Truy cập từ Windows Host

### Cách nhanh nhất (Auto-deploy và port-forward)
```bash
cd /home/anhdq/bigdata_project/k8s_serving
chmod +x deploy_and_access.sh
./deploy_and_access.sh
```

Sau khi chạy xong, truy cập từ Windows: **http://192.168.200.128:8501**

### Cách thủ công
```bash
# 1. Build và deploy
docker build -t tft-serving-layer:latest .
minikube image load tft-serving-layer:latest
kubectl apply -f deployment.yaml

# 2. Setup port forwarding để truy cập từ Windows
kubectl port-forward --address 0.0.0.0 service/tft-serving-layer 8501:8501

# 3. Truy cập từ Windows: http://<VM_IP>:8501
```

## Cấu trúc thư mục
```
k8s_serving/
├── viz_app.py                # Streamlit visualization app
├── Dockerfile                # Build container image
├── requirements.txt          # Python dependencies
├── deployment.yaml           # K8s Deployment & Service
└── README.md
```

## Yêu cầu
- Kubernetes (Minikube hoặc cluster)
- Docker
- MongoDB đang chạy tại port 27017 với các collections:
  - `tft_db.compositions` (từ batch layer)
  - `tft_db.units` (từ batch layer)
  - `tft_db.traits` (từ batch layer)
  - `tft_db.items` (từ batch layer)
  - `tft_db.players_speed` (từ speed layer)

## Biến môi trường
- `MONGODB_URI` (mặc định: `mongodb://localhost:27017/`)

## Build Docker Image
```bash
cd k8s_serving

# Build image
docker build -t tft-serving-layer:latest .

# Nếu dùng Minikube
minikube image load tft-serving-layer:latest
```

## Deploy to Kubernetes

### Deploy ứng dụng
```bash
# Deploy Deployment và Service
kubectl apply -f deployment.yaml

# Kiểm tra deployment
kubectl get deployments
kubectl get pods -l app=tft-serving-layer

# Kiểm tra service
kubectl get services tft-serving-layer
```

### Truy cập ứng dụng

#### ✅ Option 1: Port Forwarding (Khuyến nghị cho Windows host)
```bash
# Forward port và bind trên tất cả interfaces
kubectl port-forward --address 0.0.0.0 service/tft-serving-layer 8501:8501

# Truy cập từ Windows: http://<VM_IP>:8501
# Ví dụ: http://192.168.200.128:8501
```

**Lưu ý**: Port forwarding chỉ hoạt động khi terminal đang chạy. Để chạy ở background:
```bash
nohup kubectl port-forward --address 0.0.0.0 service/tft-serving-layer 8501:8501 &
```

#### Option 2: Sử dụng NodePort (Chỉ accessible trong VM)
```bash
# Lấy URL từ Minikube (chỉ hoạt động trong VM)
minikube service tft-serving-layer --url

# Output: http://192.168.49.2:30501
# ⚠️ IP này chỉ accessible trong VM, không phải từ Windows!
```

#### Option 3: Minikube tunnel + iptables (Advanced)
```bash
# Terminal 1: Chạy minikube tunnel (cần sudo)
minikube tunnel

# Terminal 2: Setup iptables forwarding
sudo iptables -t nat -A PREROUTING -p tcp --dport 8501 -j DNAT --to-destination $(minikube ip):30501
```

## Theo dõi và Debug

### Xem trạng thái
```bash
# Xem pods
kubectl get pods -l app=tft-serving-layer

# Xem logs
kubectl logs -f -l app=tft-serving-layer

# Xem logs của pod cụ thể
kubectl logs -f <pod-name>

# Describe pod để xem events
kubectl describe pod <pod-name>
```

### Health Checks
Streamlit cung cấp health endpoint tại `/_stcore/health`. Deployment đã cấu hình:
- **Liveness Probe**: Kiểm tra sau 30s, mỗi 10s
- **Readiness Probe**: Kiểm tra sau 10s, mỗi 5s

### Restart ứng dụng
```bash
# Restart deployment
kubectl rollout restart deployment/tft-serving-layer

# Xem trạng thái rollout
kubectl rollout status deployment/tft-serving-layer
```

### Scale ứng dụng
```bash
# Scale to 3 replicas
kubectl scale deployment/tft-serving-layer --replicas=3

# Xem replicas
kubectl get deployments
```

## Cấu hình Resources

Deployment mặc định:
- **Requests**: 512Mi RAM, 250m CPU
- **Limits**: 1Gi RAM, 500m CPU

Để thay đổi, sửa trong deployment.yaml:
```yaml
resources:
  requests:
    memory: "512Mi"
    cpu: "250m"
  limits:
    memory: "1Gi"
    cpu: "500m"
```

## Troubleshooting

### Ứng dụng không kết nối được MongoDB
1. Kiểm tra biến môi trường `MONGODB_URI`
2. Đảm bảo MongoDB accessible từ pods
3. Test kết nối:
```bash
kubectl exec -it <pod-name> -- python -c "from pymongo import MongoClient; client = MongoClient('mongodb://192.168.200.128:27017/'); print(client.list_database_names())"
```

### Pod không start
1. Xem events: `kubectl describe pod <pod-name>`
2. Xem logs: `kubectl logs <pod-name>`
3. Kiểm tra image: `kubectl get pods -o jsonpath='{.items[*].spec.containers[*].image}'`

### Không có dữ liệu hiển thị
1. Kiểm tra MongoDB có collections không
2. Chạy batch layer trước: `kubectl get jobs -l app=tft-batch-layer`
3. Chạn speed layer: `kubectl get pods -l app=tft-speed-layer`
4. Refresh data trong UI

## Cập nhật ứng dụng

### Update code
```bash
# Build image mới
docker build -t tft-serving-layer:latest .
minikube image load tft-serving-layer:latest

# Restart deployment để pull image mới
kubectl rollout restart deployment/tft-serving-layer
```

### Update configuration
```bash
# Sửa deployment.yaml
vim deployment.yaml

# Apply changes
kubectl apply -f deployment.yaml
```

## Xóa deployment
```bash
# Xóa tất cả resources
kubectl delete -f deployment.yaml

# Hoặc xóa từng resource
kubectl delete deployment tft-serving-layer
kubectl delete service tft-serving-layer
```

## Tích hợp với Ingress (Optional)

Nếu muốn expose qua domain name:
```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: tft-serving-ingress
spec:
  rules:
  - host: tft.example.com
    http:
      paths:
      - path: /
        pathType: Prefix
        backend:
          service:
            name: tft-serving-layer
            port:
              number: 8501
```

Deploy:
```bash
kubectl apply -f ingress.yaml
```
