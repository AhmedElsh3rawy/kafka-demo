import json
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext


def dict_to_user(obj, ctx):
    return obj


with open("user.avsc", "r") as f:
    schema_str = f.read()

schema_registry_conf = {"url": "http://localhost:8081"}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

avro_deserializer = AvroDeserializer(schema_registry_client, schema_str, dict_to_user)

consumer_conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "users-tracker",
    "auto.offset.reset": "earliest",
}

consumer = Consumer(dict(consumer_conf))

consumer.subscribe(["users"])

print("🟢 Consumer is running and subscribed to users topic")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print("❌ Error:", msg.error())
            continue

        user = avro_deserializer(
            msg.value(), SerializationContext(msg.topic(), MessageField.VALUE)
        )

        j = json.dumps(user, indent=4)
        print(j)
except KeyboardInterrupt:
    print("\n🔴 Stopping consumer")

finally:
    consumer.close()
