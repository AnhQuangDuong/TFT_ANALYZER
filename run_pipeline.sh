#!/bin/bash
source venv/bin/activate

# reset speed layers collection
docker exec -it mongodb mongosh tft_db --eval "db.players_speed.deleteMany({})"

# run Spark Streaming first
python3 test_spark_streaming_to_hdfs_and_speed_layer.py > spark_streaming.log 2>&1 &
SPARK_PID=$!

# Wait spark init
echo "⏳ Waiting for Spark Streaming to initialize..."
sleep 5

# Run crawling data
python3 test_crawlDataRiot.py

# Wait for crawling to complete and Spark to finish processing
sleep 60

# Stop Spark Streaming
kill $SPARK_PID 2>/dev/null || true

echo "✅ Crawling and real-time processing completed."

# Continue batch processing
python3 process_comp_to_mongodb.py
python3 process_item_to_mongodb.py
python3 process_trait_to_mongodb.py
python3 process_unit_to_mongodb.py

echo "✅ Data pipeline execution completed."