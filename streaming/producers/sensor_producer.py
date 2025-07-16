import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

# Kafka settings
KAFKA_TOPIC = "sensor-data"
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"  # internal Docker hostname

# Initialize producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def generate_sensor_data():
    return {
        "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
        "temperature": round(random.uniform(20.0, 25.0), 1),
        "humidity": round(random.uniform(40.0, 60.0), 1),
        "motion": random.choice([True, False]),
        "spaceId": random.choice(["living-room", "kitchen", "bedroom"])
    }

if __name__ == "__main__":
    while True:
        data = generate_sensor_data()
        print(f"Sending: {data}")
        producer.send(KAFKA_TOPIC, value=data)
        time.sleep(3)
