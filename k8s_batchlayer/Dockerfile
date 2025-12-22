FROM python:3.9-slim

# Install Java and curl
RUN apt-get update \ 
    && apt-get install -y --no-install-recommends openjdk-21-jre-headless curl \ 
    && apt-get clean \ 
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Download MongoDB connector and Hadoop jars
RUN mkdir -p /app/jars && \
    cd /app/jars && \
    curl -L -o mongo-spark-connector_2.12-10.3.0.jar https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/10.3.0/mongo-spark-connector_2.12-10.3.0.jar && \
    curl -L -o mongodb-driver-sync-4.8.2.jar https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-sync/4.8.2/mongodb-driver-sync-4.8.2.jar && \
    curl -L -o bson-4.8.2.jar https://repo1.maven.org/maven2/org/mongodb/bson/4.8.2/bson-4.8.2.jar && \
    curl -L -o mongodb-driver-core-4.8.2.jar https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-core/4.8.2/mongodb-driver-core-4.8.2.jar && \
    curl -L -o commons-logging-1.1.3.jar https://repo1.maven.org/maven2/commons-logging/commons-logging/1.1.3/commons-logging-1.1.3.jar && \
    curl -L -o hadoop-client-runtime-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.3.4/hadoop-client-runtime-3.3.4.jar && \
    curl -L -o hadoop-client-api-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/3.3.4/hadoop-client-api-3.3.4.jar

# Copy batch processing scripts
COPY process_comp_to_mongodb.py .
COPY process_item_to_mongodb.py .
COPY process_trait_to_mongodb.py .
COPY process_unit_to_mongodb.py .
COPY run_batch_jobs.sh .

RUN chmod +x run_batch_jobs.sh

ENV PYTHONUNBUFFERED=1 \
    JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 \
    HADOOP_USER_NAME=anhdq

CMD ["./run_batch_jobs.sh"]
