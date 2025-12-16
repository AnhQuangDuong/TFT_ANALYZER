#!/bin/bash

# Navigate to the project directory
source venv/bin/activate

# Start Docker Compose in detached mode
docker compose up -d

# Start HDFS
start-dfs.sh

# Sleep for 5 seconds
sleep 5

# Execute admin.py
python admin.py