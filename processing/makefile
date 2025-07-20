.PHONY: build build-nc build-progress down run run-scaled run-d stop submit minio-setup

build:
	docker-compose build

build-nc:
	docker-compose build --no-cache

build-progress:
	docker-compose build --no-cache --progress=plain

down:
	docker-compose down --volumes

run:
	make down && docker-compose up

run-scaled:
	make down && docker-compose up --scale spark-worker=3

run-d:
	make down && docker-compose up -d

stop:
	docker-compose stop

submit:
	docker exec chainalytics-spark-master spark-submit --master spark://spark-master:7077 --deploy-mode client ./apps/$(app)

minio-setup:
	@echo "Creating MinIO buckets..."
	docker exec chainalytics-minio mc alias set myminio http://localhost:9000 minioadmin minioadmin
	docker exec chainalytics-minio mc mb myminio/spark-data --ignore-existing
	docker exec chainalytics-minio mc mb myminio/spark-warehouse --ignore-existing
	@echo "MinIO buckets created successfully!"