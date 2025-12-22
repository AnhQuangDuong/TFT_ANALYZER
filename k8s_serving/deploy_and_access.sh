set -e

echo "======================================"
echo "TFT Serving Layer - Deploy và Access"
echo "======================================"

# Màu sắc cho output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Lấy VM IP
VM_IP=$(ip addr show ens33 | grep "inet " | grep -v 127.0.0.1 | awk '{print $2}' | cut -d'/' -f1)
echo -e "${GREEN}VM IP:${NC} $VM_IP"

# Bước 1: Build Docker image
echo -e "\n${YELLOW}[1/5]${NC} Building Docker image..."
docker build -t tft-serving-layer:latest .

# Bước 2: Load image vào Minikube
echo -e "\n${YELLOW}[2/5]${NC} Loading image to Minikube..."
minikube image load tft-serving-layer:latest

# Bước 3: Deploy to Kubernetes
echo -e "\n${YELLOW}[3/5]${NC} Deploying to Kubernetes..."
kubectl apply -f deployment.yaml

# Chờ pod ready
echo -e "\n${YELLOW}[4/5]${NC} Waiting for pod to be ready..."
kubectl wait --for=condition=ready pod -l app=tft-serving-layer --timeout=120s

# Bước 4: Setup port forwarding
echo -e "\n${YELLOW}[5/5]${NC} Setting up port forwarding..."

# Kill port-forward cũ nếu có
pkill -f "kubectl port-forward.*tft-serving-layer" 2>/dev/null || true

# Start port-forward in background
nohup kubectl port-forward --address 0.0.0.0 service/tft-serving-layer 8501:8501 > /tmp/tft-port-forward.log 2>&1 &
PORT_FORWARD_PID=$!

# Chờ port forward ready
sleep 3

echo -e "\n${GREEN}✅ Deployment completed successfully!${NC}"
echo -e "\n======================================"
echo -e "${GREEN}Truy cập ứng dụng từ Windows host:${NC}"
echo -e "${YELLOW}http://$VM_IP:8501${NC}"
echo -e "======================================"
echo -e "\n${GREEN}Các lệnh hữu ích:${NC}"
echo -e "  - Xem logs: ${YELLOW}kubectl logs -f -l app=tft-serving-layer${NC}"
echo -e "  - Xem pods: ${YELLOW}kubectl get pods -l app=tft-serving-layer${NC}"
echo -e "  - Stop port-forward: ${YELLOW}pkill -f 'kubectl port-forward.*tft-serving-layer'${NC}"
echo -e "  - Restart deployment: ${YELLOW}kubectl rollout restart deployment/tft-serving-layer${NC}"
echo ""
echo -e "${RED}Lưu ý:${NC} Port forwarding đang chạy ở background (PID: $PORT_FORWARD_PID)"
echo -e "       Nếu reboot VM, chạy lại script này để khôi phục."
echo ""
