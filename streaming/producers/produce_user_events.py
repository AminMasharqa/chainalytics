# produce_user_events.py
import json
import random
import time
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer

# Kafka configuration
producer = KafkaProducer(
    bootstrap_servers=['kafka:29092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: v.encode('utf-8') if v else None
)

def generate_user_event():
    # Simulate late data: event_timestamp up to 48h ago (randomly)
    delay_hours = random.choices([0, 0, 0, random.uniform(0, 48)], weights=[0.7, 0.1, 0.1, 0.1])[0]
    event_time = datetime.now(timezone.utc) - timedelta(hours=delay_hours)
    ingestion_time = datetime.now(timezone.utc)

    event = {
        "event_id": f"evt_{random.randint(10000, 99999)}",
        "user_id": str(random.randint(1, 1000)),
        "event_type": random.choice(["click", "purchase", "view"]),
        "product_id": str(random.randint(1, 1000)),
        "event_timestamp": event_time.strftime('%Y-%m-%d %H:%M:%S'),
        "ingestion_timestamp": ingestion_time.strftime('%Y-%m-%d %H:%M:%S'),
    }
    return event

def produce():
    try:
        print("🚀 Starting user events producer...")
        while True:
            event = generate_user_event()
            producer.send('user-events', key=event["event_id"], value=event)
            print(f"📤 Sent: {event['event_type']} event for user {event['user_id']}")
            time.sleep(random.uniform(0.5, 1.5))
    except KeyboardInterrupt:
        print("⏹️  Stopped producing user-events.")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    produce()