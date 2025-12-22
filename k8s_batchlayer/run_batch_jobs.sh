#!/bin/bash

echo "========================================="
echo "Starting TFT Batch Layer Processing"
echo "========================================="

# Set JAVA_HOME if not set
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-21-openjdk-amd64}
export HADOOP_USER_NAME=${HADOOP_USER_NAME:-anhdq}

# Define jar paths
JARS_PATH="/app/jars/mongo-spark-connector_2.12-10.3.0.jar,/app/jars/mongodb-driver-sync-4.8.2.jar,/app/jars/bson-4.8.2.jar,/app/jars/mongodb-driver-core-4.8.2.jar"

echo "Processing compositions data..."
python3 process_comp_to_mongodb.py
if [ $? -eq 0 ]; then
    echo "✓ Compositions processing completed successfully"
else
    echo "✗ Compositions processing failed"
    exit 1
fi

echo ""
echo "Processing items data..."
python3 process_item_to_mongodb.py
if [ $? -eq 0 ]; then
    echo "✓ Items processing completed successfully"
else
    echo "✗ Items processing failed"
    exit 1
fi

echo ""
echo "Processing traits data..."
python3 process_trait_to_mongodb.py
if [ $? -eq 0 ]; then
    echo "✓ Traits processing completed successfully"
else
    echo "✗ Traits processing failed"
    exit 1
fi

echo ""
echo "Processing units data..."
python3 process_unit_to_mongodb.py
if [ $? -eq 0 ]; then
    echo "✓ Units processing completed successfully"
else
    echo "✗ Units processing failed"
    exit 1
fi

echo ""
echo "========================================="
echo "All batch processing completed successfully!"
echo "========================================="
