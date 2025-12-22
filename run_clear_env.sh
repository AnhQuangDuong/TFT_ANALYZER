source venv/bin/activate

python3 - <<'PY'
from kafka.admin import KafkaAdminClient
from kafka.errors import UnknownTopicOrPartitionError

topic = "match_history"
client = KafkaAdminClient(bootstrap_servers="localhost:9094", client_id="tft-admin-clean")

try:
	client.delete_topics([topic], timeout_ms=10000)
	print(f"Deleted topic {topic}")
except UnknownTopicOrPartitionError:
	print(f"Topic {topic} already removed")
except Exception as exc:
	print(f"Could not delete topic {topic}: {exc}")
PY

minikube stop

docker compose down -v

stop-dfs.sh