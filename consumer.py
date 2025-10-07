from kafka import KafkaConsumer
from kafka import TopicPartition
import json

KAFKA_TOPIC_NAME='match_history'
KAFKA_CONSUMER_GROUP='KAFKA_CONSUMER_GROUP'
consumer = KafkaConsumer(
    KAFKA_TOPIC_NAME,
    bootstrap_servers='192.168.200.128:9093',
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    group_id=KAFKA_CONSUMER_GROUP,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))

)

for message in consumer:
    print(message.value)
    consumer.commit()    # <--- This is what we need
    # Optionally, To check if everything went good
    print('New Kafka offset: %s' % consumer.committed(TopicPartition(KAFKA_TOPIC_NAME, message.partition)))