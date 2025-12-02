# Hướng dẫn chạy dự án TFT_ANALYZER

## 1) Yêu cầu
- Python 3.12 và Spark (PySpark đã có trong requirements).
- Nếu dùng MongoDB: đặt biến `MONGO_URI` trong `.env` (mặc định `mongodb://localhost:27017`).

## 2) Cài đặt
```bash
python -m pip install -r requirements.txt
```

## 3) Phân tích & lưu DB
Chạy Spark tính thống kê unit và ghi vào MongoDB:
```bash
$env:PYTHONIOENCODING='utf-8'
python analysis.py
```
- Kết quả ghi vào DB/collection: `tft_db.unit_stats` (URI lấy từ `.env`).
- Đọc dữ liệu JSON từ `./data_3580_matches` (recursive, infer schema, xử lý wrapper `data` nếu có).

## 4) Tạo ảnh tier list tướng
```bash
$env:PYTHONIOENCODING='utf-8'
$env:PYSPARK_SUBMIT_ARGS='--conf spark.hadoop.fs.file.impl=org.apache.hadoop.fs.RawLocalFileSystem pyspark-shell'
python unit_tier_list.py
```
- Xuất ảnh `tft_unit_tier_list.png` ở thư mục gốc dự án.
- Dùng Spark để tính tier, vẽ bảng bằng matplotlib.

## 5) Web xem kết quả (Streamlit)
Chạy app Streamlit (xem ảnh tier + đọc Mongo nếu có):
```bash
"C:\Program Files\Python313\Scripts\streamlit.EXE" run streamlit_app.py --server.port 8501
```
- Mở trình duyệt: http://localhost:8501
- Form Mongo URI mặc định `mongodb://localhost:27017` (đọc `.env` nếu có).

## 6) Notebook khám phá dữ liệu
- `tft_notebook.ipynb`: notebook Spark phân tích schema, top trận, top tướng, meta tướng (placement), meta tộc/hệ (tier_current>0).
- Mở bằng Jupyter: 
```bash
python -m jupyter notebook tft_notebook.ipynb
``` 
  (hoặc jupyter-lab nếu có exe: `"C:\Users\MSI Gaming\AppData\Roaming\Python\Python313\Scripts\jupyter-lab.exe" tft_notebook.ipynb`)
- Chạy cell tuần tự (Shift+Enter). Labels dùng ASCII để tránh lỗi font.

## 7) Chỉnh URI Mongo
- `.env`: đặt `MONGO_URI=` theo Atlas hoặc local.
- `analysis.py` và `streamlit_app.py` đọc `MONGO_URI` (fallback `mongodb://localhost:27017`).

## 8) Lưu ý
- Với dữ liệu Windows local, Spark đôi khi báo CRC: đã set `RawLocalFileSystem` trong `unit_tier_list.py` và có thể export env tương tự khi chạy `analysis.py` nếu cần.
- Nếu cần phân tích items chuyên sâu, dùng notebook riêng `tft_items_tierlist.ipynb` (độc lập, không ảnh hưởng các script chính).