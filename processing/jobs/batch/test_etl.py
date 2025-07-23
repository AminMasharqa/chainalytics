#!/usr/bin/env python3
"""
Bronze → Silver ETL Script: API Performance
Reads from bronze_api_logs (Parquet in MinIO), aggregates metrics, and writes to Iceberg silver_api_performance table.
"""

import logging
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, count, avg, sum, when, current_timestamp

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session():
    logger.info("Creating Spark session with Iceberg + MinIO support...")
    return SparkSession.builder \
        .appName("BronzeToSilver-API-Performance") \
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
        .getOrCreate()

def transform_api_logs(df):
    logger.info("Transforming bronze_api_logs data to compute performance metrics...")

    return df.withColumn("date", to_date("call_timestamp")) \
        .groupBy("api_source", "date") \
        .agg(
            count("*").alias("total_calls"),
            avg("response_time_ms").alias("avg_response_time_ms"),
            sum(when(col("success_flag") == False, 1).otherwise(0)).alias("error_count"),
            (sum(when(col("success_flag") == True, 1).otherwise(0)) / count("*")).alias("success_rate")
        ) \
        .withColumn(
            "performance_grade",
            when(col("success_rate") >= 0.99, "A+")
            .when(col("success_rate") >= 0.95, "A")
            .when(col("success_rate") >= 0.90, "B")
            .otherwise("C")
        ) \
        .withColumn("ingestion_timestamp", current_timestamp())

def main():
    logger.info("=" * 60)
    logger.info("🚀 Bronze → Silver ETL: API Performance")
    logger.info("=" * 60)

    spark = None
    try:
        spark = create_spark_session()

        # Read from Bronze table (Parquet)
        bronze_path = "s3a://warehouse/chainalytics.db/bronze_api_logs/"
        logger.info(f"Reading data from bronze path: {bronze_path}")
        bronze_df = spark.read.parquet(bronze_path)

        if bronze_df.rdd.isEmpty():
            logger.warning("No data found in bronze_api_logs table. Exiting.")
            return

        # Transform and compute metrics
        transformed_df = transform_api_logs(bronze_df)

        # Insert into Silver Iceberg table
        logger.info("Inserting data into silver_api_performance Iceberg table...")
        transformed_df.writeTo("warehouse.chainalytics.silver_api_performance").append()
        logger.info("✅ Insert completed successfully!")

    except Exception as e:
        logger.error(f"💥 ETL job failed: {e}")
        sys.exit(1)
    finally:
        if spark:
            spark.stop()
            logger.info("✓ Spark session stopped.")

if __name__ == "__main__":
    main()
