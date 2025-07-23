#!/bin/bash
# Script to submit the Kafka→Bronze ingestion job

# Ensure the Spark master container is running before invoking this

docker exec -it chainalytics-spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://chainalytics-spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /tmp/kafka_to_bronze.py
