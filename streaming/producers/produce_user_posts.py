# produce_user_posts.py
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

POST_TITLES = [
    "Exploring the new features",
    "My thoughts on the project", 
    "How to improve data pipelines",
    "An interesting blog post",
    "Daily update from user",
    "Check out this cool trick",
    "Best practices for ETL",
    "Learning Kafka step by step",
]

def generate_user_post():
    now = datetime.now(timezone.utc)
    created_time = now - timedelta(minutes=random.randint(0, 60*24))  # random up to 24h ago

    post = {
        "post_id": random.randint(1, 10000),
        "user_id": random.randint(1, 5000),
        "title": random.choice(POST_TITLES),
        "created_timestamp": created_time.strftime('%Y-%m-%d %H:%M:%S'),
        "ingestion_timestamp": now.strftime('%Y-%m-%d %H:%M:%S'),
    }
    return post

def produce():
    try:
        print("🚀 Starting user posts producer...")
        while True:
            post = generate_user_post()
            producer.send('user-posts', key=str(post["post_id"]), value=post)
            print(f"📤 Sent: Post by user {post['user_id']} - '{post['title'][:30]}...'")
            time.sleep(random.uniform(1, 3))
    except KeyboardInterrupt:
        print("⏹️  Stopped producing user-posts.")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    produce()