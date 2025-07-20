# Start each producer in the background
echo "Starting all Kafka producers..."

# Start user events producer
docker-compose exec -d kafka python3 /producers/produce_user_events.py &

# Start products producer  
docker-compose exec -d kafka python3 /producers/produce_products.py &

# Start weather data producer
docker-compose exec -d kafka python3 /producers/produce_weather_data.py &

# Start user posts producer
docker-compose exec -d kafka python3 /producers/produce_user_posts.py &

# Start API logs producer
docker-compose exec -d kafka python3 /producers/produce_api_logs.py &

echo "All producers started!"