from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    event = {
        "event_id": random.randint(1000, 9999),
        "user_id": random.randint(1, 100),
        "event_type": random.choice(["click", "view", "purchase"]),
        "amount": round(random.uniform(10, 500), 2),
        "event_time": datetime.utcnow().isoformat()
    }

    producer.send("events", event)
    print("Sent:", event)
    time.sleep(2)

