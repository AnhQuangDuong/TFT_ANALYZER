FROM python:3.9-slim

# Install Java and curl to fetch Kafka/Hadoop jars
RUN apt-get update \ 
    && apt-get install -y --no-install-recommends openjdk-21-jre-headless curl \ 
    && apt-get clean \ 
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Download Kafka & Hadoop jars locally so runtime không cần internet
RUN mkdir -p /app/jars && \
    cd /app/jars && \
    curl -L -o spark-sql-kafka-0-10_2.12-3.5.0.jar https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    curl -L -o spark-token-provider-kafka-0-10_2.12-3.5.0.jar https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar && \
    curl -L -o kafka-clients-3.4.1.jar https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar && \
    curl -L -o lz4-java-1.8.0.jar https://repo1.maven.org/maven2/org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar && \
    curl -L -o snappy-java-1.1.10.3.jar https://repo1.maven.org/maven2/org/xerial/snappy/snappy-java/1.1.10.3/snappy-java-1.1.10.3.jar && \
    curl -L -o slf4j-api-2.0.7.jar https://repo1.maven.org/maven2/org/slf4j/slf4j-api/2.0.7/slf4j-api-2.0.7.jar && \
    curl -L -o commons-pool2-2.11.1.jar https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    curl -L -o jsr305-3.0.0.jar https://repo1.maven.org/maven2/com/google/code/findbugs/jsr305/3.0.0/jsr305-3.0.0.jar && \
    curl -L -o commons-logging-1.1.3.jar https://repo1.maven.org/maven2/commons-logging/commons-logging/1.1.3/commons-logging-1.1.3.jar && \
    curl -L -o hadoop-client-runtime-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.3.4/hadoop-client-runtime-3.3.4.jar && \
    curl -L -o hadoop-client-api-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/3.3.4/hadoop-client-api-3.3.4.jar

# Copy application code
COPY test_spark_streaming_to_hdfs_and_speed_layer.py .

ENV PYTHONUNBUFFERED=1 \
    JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

CMD ["python", "test_spark_streaming_to_hdfs_and_speed_layer.py"]
