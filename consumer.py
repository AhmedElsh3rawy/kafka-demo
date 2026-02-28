import json
import psycopg2
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

DB_CONF = {
    "host": "localhost",
    "database": "user_db",
    "user": "postgres",
    "password": "password",
}


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


def ingest_to_db(user_data):
    try:
        with psycopg2.connect(**DB_CONF) as conn:
            with conn.cursor() as cur:
                insert_query = """
                INSERT INTO users (id, name, email, country, created_at)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING;
                """
                cur.execute(
                    insert_query,
                    (
                        user_data.get("id"),
                        user_data.get("name"),
                        user_data.get("email"),
                        user_data.get("country"),
                        user_data.get("created_at"),
                    ),
                )
                conn.commit()
    except Exception as e:
        print(f"❌ DB Error: {e}")


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

        if user:
            ingest_to_db(user)
            print(f"✅ Ingested user: {user.get('name')} ({user.get('id')})")
except KeyboardInterrupt:
    print("\n🔴 Stopping consumer")

finally:
    consumer.close()
