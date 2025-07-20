# produce_weather_data.py
import json
import random
import time
from datetime import datetime, timezone, timedelta
from kafka import KafkaProducer

# Kafka configuration
producer = KafkaProducer(
    bootstrap_servers=['kafka:29092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: v.encode('utf-8') if v else None
)

LOCATIONS = ["NYC", "LA", "Chicago", "Miami", "Seattle"]
WEATHER_CONDITIONS = ["Sunny", "Cloudy", "Rainy", "Stormy", "Snowy", "Windy"]

def generate_weather_data():
    now = datetime.now(timezone.utc)
    data_delay = random.randint(0, 48)  # up to 48 hours late
    observation_time = now - timedelta(hours=data_delay)

    weather_record = {
        "location_id": random.choice(LOCATIONS),
        "weather_condition": random.choice(WEATHER_CONDITIONS),
        "temperature": round(random.uniform(-10, 40), 1),
        "wind_speed": round(random.uniform(0, 30), 1),
        "data_delay_hours": data_delay,
        "observation_time": observation_time.strftime('%Y-%m-%d %H:%M:%S'),
        "ingestion_timestamp": now.strftime('%Y-%m-%d %H:%M:%S'),
    }
    return weather_record

def produce():
    try:
        print("🚀 Starting weather data producer...")
        while True:
            record = generate_weather_data()
            producer.send('weather-data', key=record["location_id"], value=record)
            delay_info = f"(delayed {record['data_delay_hours']}h)" if record['data_delay_hours'] > 0 else ""
            print(f"📤 Sent: {record['location_id']} - {record['weather_condition']} {record['temperature']}°C {delay_info}")
            time.sleep(random.uniform(1, 2))
    except KeyboardInterrupt:
        print("⏹️  Stopped producing weather-data.")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    produce()