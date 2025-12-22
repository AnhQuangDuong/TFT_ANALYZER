#!/bin/bash

# Navigate to the project directory
source venv/bin/activate

# Start minikube
minikube start

# Start Docker Compose in detached mode
docker compose up -d

# Start HDFS
start-dfs.sh

# Sleep for 5 seconds
sleep 5

# Execute admin.py
python3 admin.py

# reset speed layers collection
docker exec -it mongodb mongosh tft_db --eval "db.players_speed.deleteMany({})"

# reset comp collection
docker exec -it mongodb mongosh tft_db --eval "db.compositions.deleteMany({})"

# reset item collection
docker exec -it mongodb mongosh tft_db --eval "db.items.deleteMany({})"

# reset trait collection
docker exec -it mongodb mongosh tft_db --eval "db.traits.deleteMany({})"

# reset unit collection
docker exec -it mongodb mongosh tft_db --eval "db.units.deleteMany({})"