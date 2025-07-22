# Fixed produce_products.py
import json
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['kafka:29092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: v.encode('utf-8') if v else None
)

CATEGORIES = ["Electronics", "Books", "Clothing", "Home", "Sports", "Toys"]

def generate_product():
    now = datetime.now(timezone.utc)
    product = {
        "product_id": random.randint(1, 10000),
        "title": f"Product {random.randint(1, 1000)}",
        "price": round(random.uniform(10.0, 1000.0), 2),
        "category": random.choice(CATEGORIES),
        "rating_score": round(random.uniform(1.0, 5.0), 2),
        "rating_count": random.randint(0, 5000),
        "ingestion_date": now.strftime('%Y-%m-%d'),  # ✅ Added missing field
        # Note: ingestion_timestamp will be added by Spark pipeline
    }
    return product

def produce():
    try:
        print("🚀 Starting products producer...")
        while True:
            product = generate_product()
            producer.send('products', key=str(product["product_id"]), value=product)
            print(f"📤 Sent: {product['title']} - ${product['price']}")
            time.sleep(random.uniform(1, 3))
    except KeyboardInterrupt:
        print("⏹️  Stopped producing products.")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    produce()