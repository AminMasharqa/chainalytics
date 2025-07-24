#!/usr/bin/env python3
"""
Kafka to Bronze Data Pipeline - Iceberg Version
Reads data from Kafka topics and inserts into Iceberg bronze tables in MinIO warehouse
"""

import logging
import json
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create Spark session configured for Kafka and Iceberg/MinIO"""
    try:
        logger.info("Creating Spark session with Kafka and Iceberg/MinIO configuration...")
        spark = SparkSession.builder \
            .appName("Kafka-to-Iceberg-Bronze-Pipeline") \
            .config("spark.sql.catalog.warehouse", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.warehouse.type", "hadoop") \
            .config("spark.sql.catalog.warehouse.warehouse", "s3a://warehouse/") \
            .config("spark.sql.catalog.warehouse.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://chainalytics-minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.defaultCatalog", "warehouse") \
            .config("spark.sql.streaming.checkpointLocation", "s3a://warehouse/checkpoints/") \
            .getOrCreate()
        
        # Set the default database
        spark.sql("USE warehouse.chainalytics")
        
        logger.info("✓ Spark session created successfully with Iceberg support")
        return spark
    except Exception as e:
        logger.error(f"✗ Failed to create Spark session: {str(e)}")
        raise

def process_user_events(spark):
    """Process user-events topic and insert into bronze_user_events Iceberg table"""
    try:
        logger.info("Setting up user-events stream...")
        
        # Read from Kafka topic
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:29092") \
            .option("subscribe", "user-events") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Define schema for user events
        user_events_schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("event_timestamp", TimestampType(), True)
        ])
        
        # Parse JSON data and add ingestion timestamp
        parsed_df = df.select(
            from_json(col("value").cast("string"), user_events_schema).alias("data"),
            current_timestamp().alias("ingestion_timestamp")
        ).select(
            col("data.event_id"),
            col("data.user_id"),
            col("data.event_type"),
            col("data.product_id"),
            col("data.event_timestamp"),
            col("ingestion_timestamp")
        ).filter(col("event_id").isNotNull())  # Filter out null records
        
        # Write to Iceberg table using foreachBatch
        def write_to_iceberg(batch_df, batch_id):
            batch_df.writeTo("warehouse.chainalytics.bronze_user_events") \
                .append()
            logger.info(f"✓ Batch {batch_id} written to bronze_user_events")
        
        query = parsed_df.writeStream \
            .foreachBatch(write_to_iceberg) \
            .option("checkpointLocation", "s3a://warehouse/checkpoints/user-events/") \
            .trigger(processingTime='30 seconds') \
            .start()
        
        logger.info("✓ User events stream started")
        return query
        
    except Exception as e:
        logger.error(f"✗ Failed to setup user events stream: {str(e)}")
        raise

def process_weather_data(spark):
    """Process weather-data topic and insert into bronze_weather_data Iceberg table"""
    try:
        logger.info("Setting up weather-data stream...")
        
        # Read from Kafka topic
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:29092") \
            .option("subscribe", "weather-data") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Define schema for weather data
        weather_schema = StructType([
            StructField("location_id", StringType(), True),
            StructField("weather_condition", StringType(), True),
            StructField("temperature", DoubleType(), True),
            StructField("wind_speed", DoubleType(), True),
            StructField("data_delay_hours", IntegerType(), True),
            StructField("observation_time", TimestampType(), True)
        ])
        
        # Parse JSON data and add ingestion timestamp
        parsed_df = df.select(
            from_json(col("value").cast("string"), weather_schema).alias("data"),
            current_timestamp().alias("ingestion_timestamp")
        ).select(
            col("data.location_id"),
            col("data.weather_condition"),
            col("data.temperature"),
            col("data.wind_speed"),
            col("data.data_delay_hours"),
            col("data.observation_time"),
            col("ingestion_timestamp")
        ).filter(col("location_id").isNotNull())  # Filter out null records
        
        # Write to Iceberg table using foreachBatch
        def write_to_iceberg(batch_df, batch_id):
            batch_df.writeTo("warehouse.chainalytics.bronze_weather_data") \
                .append()
            logger.info(f"✓ Batch {batch_id} written to bronze_weather_data")
        
        query = parsed_df.writeStream \
            .foreachBatch(write_to_iceberg) \
            .option("checkpointLocation", "s3a://warehouse/checkpoints/weather-data/") \
            .trigger(processingTime='30 seconds') \
            .start()
        
        logger.info("✓ Weather data stream started")
        return query
        
    except Exception as e:
        logger.error(f"✗ Failed to setup weather data stream: {str(e)}")
        raise

def process_products(spark):
    """Process products topic and insert into bronze_products Iceberg table"""
    try:
        logger.info("Setting up products stream...")
        
        # Read from Kafka topic
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:29092") \
            .option("subscribe", "products") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Define schema for products
        products_schema = StructType([
            StructField("product_id", IntegerType(), True),
            StructField("title", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("category", StringType(), True),
            StructField("rating_score", DoubleType(), True),
            StructField("rating_count", IntegerType(), True),
            StructField("ingestion_date", DateType(), True)
        ])
        
        # Parse JSON data and add ingestion timestamp
        parsed_df = df.select(
            from_json(col("value").cast("string"), products_schema).alias("data"),
            current_timestamp().alias("ingestion_timestamp")
        ).select(
            col("data.product_id"),
            col("data.title"),
            col("data.price"),
            col("data.category"),
            col("data.rating_score"),
            col("data.rating_count"),
            col("data.ingestion_date"),
            col("ingestion_timestamp")
        ).filter(col("product_id").isNotNull())  # Filter out null records
        
        # Write to Iceberg table using foreachBatch
        def write_to_iceberg(batch_df, batch_id):
            batch_df.writeTo("warehouse.chainalytics.bronze_products") \
                .append()
            logger.info(f"✓ Batch {batch_id} written to bronze_products")
        
        query = parsed_df.writeStream \
            .foreachBatch(write_to_iceberg) \
            .option("checkpointLocation", "s3a://warehouse/checkpoints/products/") \
            .trigger(processingTime='30 seconds') \
            .start()
        
        logger.info("✓ Products stream started")
        return query
        
    except Exception as e:
        logger.error(f"✗ Failed to setup products stream: {str(e)}")
        raise

def process_api_logs(spark):
    """Process api-logs topic and insert into bronze_api_logs Iceberg table"""
    try:
        logger.info("Setting up api-logs stream...")
        
        # Read from Kafka topic
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:29092") \
            .option("subscribe", "api-logs") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Define schema for API logs
        api_logs_schema = StructType([
            StructField("log_id", StringType(), True),
            StructField("api_source", StringType(), True),
            StructField("response_time_ms", IntegerType(), True),
            StructField("success_flag", BooleanType(), True),
            StructField("call_timestamp", TimestampType(), True)
        ])
        
        # Parse JSON data and add ingestion timestamp
        parsed_df = df.select(
            from_json(col("value").cast("string"), api_logs_schema).alias("data"),
            current_timestamp().alias("ingestion_timestamp")
        ).select(
            col("data.log_id"),
            col("data.api_source"),
            col("data.response_time_ms"),
            col("data.success_flag"),
            col("data.call_timestamp"),
            col("ingestion_timestamp")
        ).filter(col("log_id").isNotNull())  # Filter out null records
        
        # Write to Iceberg table using foreachBatch
        def write_to_iceberg(batch_df, batch_id):
            batch_df.writeTo("warehouse.chainalytics.bronze_api_logs") \
                .append()
            logger.info(f"✓ Batch {batch_id} written to bronze_api_logs")
        
        query = parsed_df.writeStream \
            .foreachBatch(write_to_iceberg) \
            .option("checkpointLocation", "s3a://warehouse/checkpoints/api-logs/") \
            .trigger(processingTime='30 seconds') \
            .start()
        
        logger.info("✓ API logs stream started")
        return query
        
    except Exception as e:
        logger.error(f"✗ Failed to setup API logs stream: {str(e)}")
        raise

def process_user_posts(spark):
    """Process user-posts topic and insert into bronze_user_posts Iceberg table"""
    try:
        logger.info("Setting up user-posts stream...")
        
        # Read from Kafka topic
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:29092") \
            .option("subscribe", "user-posts") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Define schema for user posts
        user_posts_schema = StructType([
            StructField("post_id", IntegerType(), True),
            StructField("user_id", IntegerType(), True),
            StructField("title", StringType(), True),
            StructField("created_timestamp", TimestampType(), True)
        ])
        
        # Parse JSON data and add ingestion timestamp
        parsed_df = df.select(
            from_json(col("value").cast("string"), user_posts_schema).alias("data"),
            current_timestamp().alias("ingestion_timestamp")
        ).select(
            col("data.post_id"),
            col("data.user_id"),
            col("data.title"),
            col("data.created_timestamp"),
            col("ingestion_timestamp")
        ).filter(col("post_id").isNotNull())  # Filter out null records
        
        # Write to Iceberg table using foreachBatch
        def write_to_iceberg(batch_df, batch_id):
            batch_df.writeTo("warehouse.chainalytics.bronze_user_posts") \
                .append()
            logger.info(f"✓ Batch {batch_id} written to bronze_user_posts")
        
        query = parsed_df.writeStream \
            .foreachBatch(write_to_iceberg) \
            .option("checkpointLocation", "s3a://warehouse/checkpoints/user-posts/") \
            .trigger(processingTime='30 seconds') \
            .start()
        
        logger.info("✓ User posts stream started")
        return query
        
    except Exception as e:
        logger.error(f"✗ Failed to setup user posts stream: {str(e)}")
        raise

def verify_table_exists(spark, table_name):
    """Verify that an Iceberg table exists before starting the stream"""
    try:
        spark.sql(f"DESCRIBE TABLE warehouse.chainalytics.{table_name}")
        logger.info(f"✓ Table warehouse.chainalytics.{table_name} exists")
        return True
    except Exception as e:
        logger.error(f"✗ Table warehouse.chainalytics.{table_name} does not exist: {str(e)}")
        return False

def main():
    """Main execution function"""
    logger.info("=" * 60)
    logger.info("Starting Kafka to Iceberg Bronze Data Pipeline")
    logger.info("=" * 60)
    
    spark = None
    queries = []
    
    try:
        # Create Spark session
        spark = create_spark_session()
        
        # Verify all tables exist
        tables_to_verify = [
            "bronze_user_events",
            "bronze_weather_data", 
            "bronze_products",
            "bronze_api_logs",
            "bronze_user_posts"
        ]
        
        logger.info("Verifying Iceberg tables exist...")
        all_tables_exist = True
        for table in tables_to_verify:
            if not verify_table_exists(spark, table):
                all_tables_exist = False
        
        if not all_tables_exist:
            logger.error("✗ Not all required Iceberg tables exist. Please run the table creation script first.")
            sys.exit(1)
        
        # Start all streaming queries
        logger.info("Starting all Kafka to Iceberg streams...")
        
        queries.append(process_user_events(spark))
        queries.append(process_weather_data(spark))
        queries.append(process_products(spark))
        queries.append(process_api_logs(spark))
        queries.append(process_user_posts(spark))
        
        logger.info("=" * 60)
        logger.info("🎉 SUCCESS: All Kafka to Iceberg Bronze streams are running!")
        logger.info("Kafka Topics -> Iceberg Bronze Tables Mapping:")
        logger.info("  - user-events -> warehouse.chainalytics.bronze_user_events")
        logger.info("  - weather-data -> warehouse.chainalytics.bronze_weather_data")
        logger.info("  - products -> warehouse.chainalytics.bronze_products")
        logger.info("  - api-logs -> warehouse.chainalytics.bronze_api_logs")
        logger.info("  - user-posts -> warehouse.chainalytics.bronze_user_posts")
        logger.info("Data stored as Iceberg tables in MinIO: s3a://warehouse/chainalytics/")
        logger.info("=" * 60)
        
        # Wait for all queries to finish (runs indefinitely)
        for query in queries:
            query.awaitTermination()
        
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
    except Exception as e:
        logger.error("=" * 60)
        logger.error(f"💥 PIPELINE FAILED: {str(e)}")
        logger.error("=" * 60)
        sys.exit(1)
        
    finally:
        # Stop all queries
        logger.info("Stopping streaming queries...")
        for query in queries:
            if query and query.isActive:
                query.stop()
        
        if spark:
            logger.info("Stopping Spark session...")
            spark.stop()
            logger.info("✓ Spark session stopped")

if __name__ == "__main__":
    main()