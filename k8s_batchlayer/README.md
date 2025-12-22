# TFT Batch Layer - Kubernetes Deployment

Batch layer xử lý dữ liệu từ HDFS và ghi kết quả phân tích vào MongoDB. Bao gồm 4 job chính:
1. **Compositions** - Phân tích đội hình (comp signature, win rate, pick rate, top 4 rate)
2. **Items** - Phân tích vật phẩm (win rate, placement, impact, popular units)
3. **Traits** - Phân tích tộc hệ/class (win rate, placement, tier distribution)
4. **Units** - Phân tích champion (win rate, placement, tier ranking, popular items)

## Cấu trúc thư mục
```
k8s_batchlayer/
├── process_comp_to_mongodb.py       # Xử lý compositions
├── process_item_to_mongodb.py       # Xử lý items
├── process_trait_to_mongodb.py      # Xử lý traits
├── process_unit_to_mongodb.py       # Xử lý units
├── run_batch_jobs.sh                # Script chạy tuần tự 4 jobs
├── Dockerfile                       # Build image với jars
├── requirements.txt                 # Python dependencies
├── deployment.yaml                  # K8s CronJob & Manual Job
└── README.md
```

## Yêu cầu
- Kubernetes (Minikube hoặc cluster)
- Docker
- HDFS đang chạy với dữ liệu tại `/tft/{ngày}/stream_output`
- MongoDB đang chạy tại port 27017

## Biến môi trường
- `HDFS_NAMENODE` (mặc định: `hdfs://192.168.200.128:9000`)
- `MONGODB_URI` (mặc định: `mongodb://192.168.200.128:27017/`)
- `HADOOP_USER_NAME` (mặc định: `anhdq`)

## Build Docker Image
```bash
cd k8s_batchlayer

# Build image
docker build -t tft-batch-layer:latest .

# Nếu dùng Minikube
minikube image load tft-batch-layer:latest
```

**Lưu ý:** Image đã tự động tải MongoDB connector và Hadoop jars vào `/app/jars`, không cần mount `~/.ivy2`.

## Deploy

### Option 1: CronJob (Chạy tự động theo lịch)
```bash
# Deploy CronJob - chạy mỗi 20 phút
kubectl apply -f deployment.yaml

# Kiểm tra CronJob
kubectl get cronjobs
kubectl get jobs

# Xem logs của job gần nhất
kubectl logs -l app=tft-batch-layer --tail=100
```

Để thay đổi lịch chạy, sửa `schedule` trong deployment.yaml:
- `0 2 * * *` - 2 giờ sáng mỗi ngày
- `0 */6 * * *` - Mỗi 6 tiếng
- `0 0 * * 0` - Chủ nhật hàng tuần

### Option 2: Manual Job (Chạy thủ công ngay lập tức)
```bash
# Chạy job thủ công
kubectl create -f deployment.yaml

# Hoặc nếu đã deploy CronJob, tạo job từ CronJob
kubectl create job --from=cronjob/tft-batch-layer tft-batch-manual-$(date +%s)

# Theo dõi logs
kubectl logs -f job/tft-batch-layer-manual
```

## Theo dõi và Debug

### Xem trạng thái
```bash
# Xem tất cả jobs
kubectl get jobs

# Xem pods của batch layer
kubectl get pods -l app=tft-batch-layer

# Xem logs chi tiết
kubectl logs -f <pod-name>

# Xem logs của container cụ thể
kubectl logs <pod-name> -c batch-processor
```

### Kiểm tra kết quả trong MongoDB
```bash
# Kết nối MongoDB
mongosh mongodb://192.168.200.128:27017/tft_db

# Kiểm tra collections
show collections

# Kiểm tra số lượng documents
db.compositions.count()
db.items.count()
db.traits.count()
db.units.count()

# Xem dữ liệu mẫu
db.compositions.find().limit(5).pretty()
```

## Cleanup

### Xóa CronJob
```bash
kubectl delete cronjob tft-batch-layer
```

### Xóa Manual Job
```bash
kubectl delete job tft-batch-layer-manual
```

### Xóa tất cả jobs cũ
```bash
kubectl delete jobs -l app=tft-batch-layer
```

## Luồng xử lý

1. **Đọc dữ liệu từ HDFS**
   - Đọc Parquet files từ `/tft/{ngày}/stream_output`
   - Filter game type = "standard" và set = "TFTSet16"
   - Loại bỏ duplicate matches

2. **Xử lý Compositions**
   - Tạo composition signature từ danh sách champions
   - Tính average placement, pick rate, top 4 rate
   - Tìm top 4 carry units phổ biến nhất
   - Ghi vào `tft_db.compositions`

3. **Xử lý Items**
   - Phân tích từng item: win rate, average placement
   - Tính impact (place change) so với baseline
   - Tìm popular units cho mỗi item
   - Ghi vào `tft_db.items`

4. **Xử lý Traits**
   - Phân tích activated traits (tier > 0)
   - Tính win rate, average placement theo trait
   - Phân tích tier distribution
   - Ghi vào `tft_db.traits`

5. **Xử lý Units**
   - Tính win rate, average placement cho mỗi champion
   - Xếp tier (S/A/B/C) dựa trên performance
   - Tìm popular items cho mỗi champion
   - Ghi vào `tft_db.units`

## Troubleshooting

### Lỗi kết nối HDFS
```
Error: Connection refused to hdfs://192.168.200.128:9000
```
**Giải pháp:**
- Kiểm tra HDFS đang chạy: `hdfs dfsadmin -report`
- Kiểm tra network: `ping 192.168.200.128`
- Kiểm tra firewall port 9000

### Lỗi kết nối MongoDB
```
Error: couldn't connect to server mongodb://192.168.200.128:27017
```
**Giải pháp:**
- Kiểm tra MongoDB: `mongosh mongodb://192.168.200.128:27017`
- Kiểm tra network và port 27017

### Job failed hoặc pod CrashLoopBackOff
```bash
# Xem logs chi tiết
kubectl describe pod <pod-name>
kubectl logs <pod-name>

# Kiểm tra resources
kubectl top pods
```

### Không có dữ liệu trong HDFS
```
Error: Path does not exist: hdfs://192.168.200.128:9000/tft/2025-12-20/stream_output
```
**Giải pháp:**
- Kiểm tra streaming layer đã chạy và ghi dữ liệu chưa
- Kiểm tra đường dẫn HDFS: `hdfs dfs -ls /tft/`
- Đợi streaming layer ghi đủ dữ liệu

## Monitoring

### Thời gian xử lý trung bình
- Compositions: ~2-5 phút
- Items: ~3-7 phút  
- Traits: ~2-5 phút
- Units: ~2-5 phút
- **Tổng cộng: ~10-22 phút** (tùy thuộc số lượng matches)

### Resource Usage
- Memory: 2-4 GB (peak ~6 GB khi xử lý items)
- CPU: 1-2 cores
- Disk: Minimal (chỉ đọc từ HDFS, ghi vào MongoDB)

## Tích hợp với pipeline

Batch layer này được thiết kế để chạy sau khi Speed Layer (streaming) đã ghi đủ dữ liệu vào HDFS:

```
Kafka → Speed Layer (Streaming) → HDFS
                                    ↓
                              Batch Layer → MongoDB → Visualization
```

Lịch chạy đề xuất:
- **Speed Layer**: Chạy liên tục, ghi micro-batches mỗi 30s
- **Batch Layer**: Chạy mỗi ngày lúc 2 giờ sáng để phân tích dữ liệu ngày hôm trước
