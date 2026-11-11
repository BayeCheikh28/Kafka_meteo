import json
from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = "kafka:9092"
TOPIC_NAME = "weather_stream"

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

for record in consumer:
    print(record.value)
