import json
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = "kafka:9092"
TOPIC_NAME = "weather_stream"

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

message = {"msg": "Hello Kafka"}

producer.send(TOPIC_NAME, message)
producer.flush()

print(f"Message envoye sur '{TOPIC_NAME}': {message}")

producer.close()
