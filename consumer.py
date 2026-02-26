import json

from confluent_kafka import Consumer

consumer_conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "users-tracker",
    "auto.offset.reset": "earliest",
}

consumer = Consumer(consumer_conf)

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

        value = msg.value().decode("utf-8")
        user = json.loads(value)
        print(user)
except KeyboardInterrupt:
    print("\n🔴 Stopping consumer")

finally:
    consumer.close()
