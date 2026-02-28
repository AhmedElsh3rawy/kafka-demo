from faker import Faker
import uuid, time
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringSerializer,
)

fake = Faker()


producer_conf = {
    "bootstrap.servers": "localhost:9092",
}

producer = Producer(dict(producer_conf))


def delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Delivered to {msg.topic()} [Partition: {msg.partition()}]")


def generate_user():
    return {
        "id": str(uuid.uuid4()),
        "name": fake.name(),
        "email": fake.email(),
        "country": fake.country(),
        "created_at": str(fake.date_time()),
    }


def user_to_dict(user, ctx):
    return {
        "id": user["id"],
        "name": user["name"],
        "email": user["email"],
        "country": user["country"],
        "created_at": user["created_at"],
    }


with open("user.avsc", "r") as f:
    schema_str = f.read()


schema_registry_conf = {"url": "http://localhost:8081"}

schema_registry_client = SchemaRegistryClient(schema_registry_conf)

avro_serializer = AvroSerializer(
    schema_registry_client,
    schema_str,
    user_to_dict,
)

string_serializer = StringSerializer("utf-8")

for i in range(5):
    user = generate_user()
    producer.produce(
        topic="users",
        key=string_serializer(str(i), SerializationContext("users", MessageField.KEY)),
        value=avro_serializer(user, SerializationContext("users", MessageField.VALUE)),
        callback=delivery_report,
    )
    producer.flush()
    time.sleep(2)
