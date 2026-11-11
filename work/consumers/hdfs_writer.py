import json
from kafka import KafkaConsumer
from hdfs import InsecureClient

BOOTSTRAP_SERVERS = "kafka:9092"
TOPIC_NAME = "weather_transformed"
HDFS_URL = "http://namenode:9870"
HDFS_BASE_PATH = "/hdfs-data"

client = InsecureClient(HDFS_URL, user="root")

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

for record in consumer:
    message = record.value
    country = message["country"]
    city = message["city"]
    file_path = f"{HDFS_BASE_PATH}/{country}/{city}/alerts.json"
    line = json.dumps(message) + "\n"

    if client.status(file_path, strict=False) is None:
        client.write(file_path, data=line, encoding="utf-8")
    else:
        client.write(file_path, data=line, encoding="utf-8", append=True)

    print(f"Ecrit dans {file_path}")
