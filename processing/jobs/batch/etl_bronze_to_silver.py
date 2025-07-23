#!/usr/bin/env python3
"""
ETL from Bronze to Silver for chainalytics
Reads Parquet from bronze tables in MinIO, bootstraps Iceberg silver tables with explicit LOCATIONs,
applies basic aggregations, and writes into Iceberg silver tables in the `chainalytics` namespace.
"""

import logging
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, avg, max as spark_max,
    when, lit, to_date, current_timestamp, expr, upper
)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create Spark session configured for Iceberg + MinIO"""
    try:
        spark = (
            SparkSession.builder
                .appName("ETL-Bronze-to-Silver")
                .config("spark.sql.catalog.warehouse", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.warehouse.type", "hadoop")
                .config("spark.sql.catalog.warehouse.warehouse", "s3a://warehouse/")
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.hadoop.fs.s3a.endpoint", "http://chainalytics-minio:9000")
                .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
                .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                .getOrCreate()
        )
        logger.info("✓ Spark session created.")
        return spark
    except Exception as e:
        logger.error(f"✗ Spark session creation failed: {e}")
        raise

def bootstrap_silver_tables(spark):
    """Create each silver table with an explicit LOCATION to avoid malformed URI."""
    logger.info("Bootstrapping Iceberg silver tables with explicit LOCATIONs…")

    spark.sql("""
      CREATE TABLE IF NOT EXISTS `warehouse`.`chainalytics`.`silver_user_behavior` (
        user_id STRING,
        total_events BIGINT,
        unique_products_viewed BIGINT,
        last_activity_date DATE,
        avg_session_duration DOUBLE,
        user_segment STRING,
        ingestion_timestamp TIMESTAMP
      )
      USING ICEBERG
      LOCATION 's3a://warehouse/chainalytics/silver_user_behavior/'
    """)

    spark.sql("""
      CREATE TABLE IF NOT EXISTS `warehouse`.`chainalytics`.`silver_product_analytics` (
        product_id INT,
        product_name STRING,
        category STRING,
        current_price DOUBLE,
        avg_rating DOUBLE,
        total_views INT,
        total_purchases INT,
        conversion_rate DOUBLE,
        ingestion_timestamp TIMESTAMP
      )
      USING ICEBERG
      LOCATION 's3a://warehouse/chainalytics/silver_product_analytics/'
    """)

    spark.sql("""
      CREATE TABLE IF NOT EXISTS `warehouse`.`chainalytics`.`silver_weather_impact` (
        location_id STRING,
        date DATE,
        avg_temperature DOUBLE,
        weather_category STRING,
        delivery_delay_hours INT,
        impact_score DOUBLE,
        ingestion_timestamp TIMESTAMP
      )
      USING ICEBERG
      LOCATION 's3a://warehouse/chainalytics/silver_weather_impact/'
    """)

    spark.sql("""
      CREATE TABLE IF NOT EXISTS `warehouse`.`chainalytics`.`silver_api_performance` (
        api_source STRING,
        date DATE,
        total_calls BIGINT,
        avg_response_time_ms DOUBLE,
        success_rate DOUBLE,
        error_count BIGINT,
        performance_grade STRING,
        ingestion_timestamp TIMESTAMP
      )
      USING ICEBERG
      LOCATION 's3a://warehouse/chainalytics/silver_api_performance/'
    """)

    spark.sql("""
      CREATE TABLE IF NOT EXISTS `warehouse`.`chainalytics`.`silver_product_performance` (
        product_key STRING,
        product_name STRING,
        price_tier STRING,
        category_std STRING,
        popularity_score DOUBLE,
        quality_rating STRING,
        last_updated DATE,
        ingestion_timestamp TIMESTAMP
      )
      USING ICEBERG
      LOCATION 's3a://warehouse/chainalytics/silver_product_performance/'
    """)

    logger.info("✓ Bootstrap complete.")

def etl_user_behavior(spark):
    logger.info("Processing user behavior…")
    df = spark.read.parquet("s3a://warehouse/chainalytics/bronze_user_events/")
    agg = (
        df.groupBy("user_id")
          .agg(
            count("*").alias("total_events"),
            countDistinct("product_id").alias("unique_products_viewed"),
            spark_max(to_date("event_timestamp")).alias("last_activity_date")
          )
          .withColumn("avg_session_duration", lit(0.0))
          .withColumn("user_segment",
                      when(col("total_events") < 10, lit("new")).otherwise(lit("engaged")))
          .withColumn("ingestion_timestamp", current_timestamp())
    )
    agg.writeTo("warehouse.chainalytics.silver_user_behavior").append()
    logger.info(f"✓ silver_user_behavior loaded ({agg.count()} rows)")

def etl_product_analytics(spark):
    logger.info("Processing product analytics…")
    prod   = spark.read.parquet("s3a://warehouse/chainalytics/bronze_products/")
    events = spark.read.parquet("s3a://warehouse/chainalytics/bronze_user_events/")
    vagg = events.filter(col("event_type")=="view") \
                 .groupBy("product_id").count().withColumnRenamed("count","total_views")
    bagg = events.filter(col("event_type")=="purchase") \
                 .groupBy("product_id").count().withColumnRenamed("count","total_purchases")

    agg = (
        prod.alias("p")
            .join(vagg, "product_id", "left")
            .join(bagg, "product_id", "left")
            .na.fill({"total_views": 0, "total_purchases": 0})
            .select(
              col("product_id"),
              col("title").alias("product_name"),
              col("category"),
              col("price").alias("current_price"),
              col("rating_score").alias("avg_rating"),
              col("total_views"),
              col("total_purchases")
            )
            .withColumn("conversion_rate",
                        when(col("total_views") > 0,
                             col("total_purchases") / col("total_views"))
                        .otherwise(lit(0.0)))
            .withColumn("ingestion_timestamp", current_timestamp())
    )

    agg.writeTo("warehouse.chainalytics.silver_product_analytics").append()
    logger.info(f"✓ silver_product_analytics loaded ({agg.count()} rows)")

def etl_weather_impact(spark):
    logger.info("Processing weather impact…")
    df = spark.read.parquet("s3a://warehouse/chainalytics/bronze_weather_data/")
    agg = (
        df.withColumn("date", to_date("observation_time"))
          .groupBy("location_id","date")
          .agg(
            avg("temperature").alias("avg_temperature"),
            avg("data_delay_hours").cast("int").alias("delivery_delay_hours")
          )
          .withColumn("weather_category",
                      when(col("avg_temperature") < 10, lit("cold"))
                      .when(col("avg_temperature") < 25, lit("moderate"))
                      .otherwise(lit("hot")))
          .withColumn("impact_score", expr("delivery_delay_hours/(avg_temperature+1)*100"))
          .withColumn("ingestion_timestamp", current_timestamp())
    )
    agg.writeTo("warehouse.chainalytics.silver_weather_impact").append()
    logger.info(f"✓ silver_weather_impact loaded ({agg.count()} rows)")

def etl_api_performance(spark):
    logger.info("Processing API performance…")
    df = spark.read.parquet("s3a://warehouse/chainalytics/bronze_api_logs/")
    agg = (
        df.withColumn("date", to_date("call_timestamp"))
          .groupBy("api_source","date")
          .agg(
            count("*").alias("total_calls"),
            avg("response_time_ms").alias("avg_response_time_ms"),
            avg(col("success_flag").cast("int")).alias("success_rate"),
            count(when(col("success_flag")==False, True)).alias("error_count")
          )
          .withColumn("performance_grade",
                      when(col("success_rate") >= 0.9, lit("Good"))
                      .otherwise(lit("Poor")))
          .withColumn("ingestion_timestamp", current_timestamp())
    )
    agg.writeTo("warehouse.chainalytics.silver_api_performance").append()
    logger.info(f"✓ silver_api_performance loaded ({agg.count()} rows)")

def etl_product_performance(spark):
    logger.info("Processing product performance…")
    df = spark.read.parquet("s3a://warehouse/chainalytics/bronze_products/")
    agg = (
        df.groupBy("product_id","title","category","price")
          .agg(avg("rating_score").alias("avg_rating"))
          .select(
            col("product_id").cast("string").alias("product_key"),
            col("title").alias("product_name"),
            when(col("price") < 50, lit("Low"))
              .when(col("price") < 200, lit("Medium"))
              .otherwise(lit("High")).alias("price_tier"),
            upper(col("category")).alias("category_std"),
            col("avg_rating").alias("popularity_score"),
            when(col("avg_rating") >= 4.5, lit("Excellent"))
             .when(col("avg_rating") >= 3.5, lit("Good"))
             .otherwise(lit("Average")).alias("quality_rating"),
            spark_max(to_date("ingestion_timestamp")).alias("last_updated"),
            current_timestamp().alias("ingestion_timestamp")
          )
    )
    agg.writeTo("warehouse.chainalytics.silver_product_performance").append()
    logger.info(f"✓ silver_product_performance loaded ({agg.count()} rows)")

def main():
    logger.info("="*60)
    logger.info("▶️  ETL: Bronze → Silver")
    logger.info("="*60)
    try:
        spark = create_spark_session()
        bootstrap_silver_tables(spark)
        etl_user_behavior(spark)
        etl_product_analytics(spark)
        etl_weather_impact(spark)
        etl_api_performance(spark)
        etl_product_performance(spark)
        logger.info("🎉 ETL complete: all silver tables updated.")
    except Exception as e:
        logger.error(f"💥 ETL failed: {e}")
        sys.exit(1)
    finally:
        if 'spark' in locals():
            spark.stop()
            logger.info("✓ Spark session stopped.")

if __name__ == "__main__":
    main()
