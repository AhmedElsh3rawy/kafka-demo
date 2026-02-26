from faker import Faker
import uuid, json, time
from confluent_kafka import Producer

fake = Faker()


producer_conf = {
    "bootstrap.servers": "localhost:9092",
}

producer = Producer(producer_conf)


def delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Delivered {msg.value().decode('utf-8')}")


def generate_user():
    return {
        "id": str(uuid.uuid4()),
        "name": fake.name(),
        "email": fake.email(),
        "country": fake.country(),
        "created_at": str(fake.date_time()),
    }


for i in range(5):
    user = json.dumps(generate_user()).encode("utf-8")
    producer.produce(topic="users", value=user, callback=delivery_report)
    producer.flush()
    print("Sent:", user)
    time.sleep(2)
