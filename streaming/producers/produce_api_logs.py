# produce_api_logs.py
import json
import random
import time
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer

# Kafka configuration
producer = KafkaProducer(
    bootstrap_servers=['kafka:29092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: v.encode('utf-8') if v else None
)

API_SOURCES = ["auth-service", "payment-gateway", "user-service", "inventory-service"]

def generate_api_log():
    now = datetime.now(timezone.utc)
    log = {
        "log_id": str(uuid.uuid4()),
        "api_source": random.choice(API_SOURCES),
        "response_time_ms": random.randint(10, 2000),
        "success_flag": random.choice([True, True, True, False]),  # mostly successes
        "call_timestamp": now.strftime('%Y-%m-%d %H:%M:%S'),
        "ingestion_timestamp": now.strftime('%Y-%m-%d %H:%M:%S'),
    }
    return log

def produce():
    try:
        print("🚀 Starting API logs producer...")
        while True:
            log_entry = generate_api_log()
            producer.send('api-logs', key=log_entry["log_id"], value=log_entry)
            status = "✅" if log_entry["success_flag"] else "❌"
            print(f"📤 Sent: {status} {log_entry['api_source']} - {log_entry['response_time_ms']}ms")
            time.sleep(random.uniform(0.2, 1.0))
    except KeyboardInterrupt:
        print("⏹️  Stopped producing api-logs.")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    produce()