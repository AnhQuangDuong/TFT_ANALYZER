from kafka.admin import KafkaAdminClient, NewTopic


admin_client = KafkaAdminClient(bootstrap_servers="localhost:9094",client_id='tft-admin')

topic_list = []
topic_list.append(NewTopic(name="match_history", num_partitions=3, replication_factor=3))
admin_client.create_topics(new_topics=topic_list, validate_only=False)