#!/bin/bash

# Stop and remove all containers
echo "Stopping all containers..."
docker-compose down

# Remove all containers, networks, and volumes (optional - only if you want a complete clean)
echo "Cleaning up containers and images..."
docker-compose down --volumes --remove-orphans

# Remove the current image
echo "Removing existing Spark image..."
docker rmi chainalytics-spark-image 2>/dev/null || echo "Image not found, continuing..."

# Clear Docker build cache (optional but recommended for clean build)
echo "Pruning Docker build cache..."
docker builder prune -f

# Pull the base image explicitly (to avoid credential issues)
echo "Pulling base image..."
docker pull apache/spark:3.5.0-python3

# Build the new image with dependencies
echo "Building new image with Iceberg dependencies..."
docker-compose build --no-cache

# Start services
echo "Starting services..."
docker-compose up -d

# Wait for services to be ready
echo "Waiting for services to start..."
sleep 45

# Verify containers are running
echo "Checking container status..."
docker ps

# Test the bronze table creation script
echo "Testing bronze table creation..."
docker exec -it chainalytics-spark-master /opt/spark/bin/spark-submit /opt/spark/jobs/setup/create_bronze_tables.py

echo "Build and test complete!"