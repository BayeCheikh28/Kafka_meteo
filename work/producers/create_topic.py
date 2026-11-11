from kafka.admin import KafkaAdminClient, NewTopic

BOOTSTRAP_SERVERS = "kafka:9092"
TOPIC_NAME = "weather_stream"

admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)

existing_topics = admin.list_topics()

if TOPIC_NAME not in existing_topics:
    topic = NewTopic(name=TOPIC_NAME, num_partitions=1, replication_factor=1)
    admin.create_topics([topic])
    print(f"Topic '{TOPIC_NAME}' cree")
else:
    print(f"Topic '{TOPIC_NAME}' existe deja")

admin.close()
