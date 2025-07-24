#!/usr/bin/env python3
"""
ETL from Bronze to Silver for chainalytics
Reads Parquet from bronze tables in MinIO, bootstraps Iceberg silver tables,
applies aggregations, and writes into Iceberg silver tables.
"""
import logging
import sys
import traceback
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg, col, count, countDistinct, current_timestamp, expr, lit, 
    max as spark_max, to_date, upper, when
)

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_spark_session() -> SparkSession:
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
        logger.info("✓ Spark session created")
        return spark
    except Exception as e:
        logger.error(f"✗ Spark session creation failed: {e}")
        raise


def bootstrap_silver_tables(spark: SparkSession) -> None:
    """Create silver tables with explicit locations"""
    logger.info("Bootstrapping Iceberg silver tables...")
    
    tables = [
        {
            "name": "silver_user_behavior",
            "schema": """(
                user_id STRING,
                total_events BIGINT,
                unique_products_viewed BIGINT,
                last_activity_date DATE,
                avg_session_duration DOUBLE,
                user_segment STRING,
                ingestion_timestamp TIMESTAMP
            )"""
        },
        {
            "name": "silver_product_analytics",
            "schema": """(
                product_id INT,
                product_name STRING,
                category STRING,
                current_price DOUBLE,
                avg_rating DOUBLE,
                total_views INT,
                total_purchases INT,
                conversion_rate DOUBLE,
                ingestion_timestamp TIMESTAMP
            )"""
        },
        {
            "name": "silver_weather_impact",
            "schema": """(
                location_id STRING,
                date DATE,
                avg_temperature DOUBLE,
                weather_category STRING,
                delivery_delay_hours INT,
                impact_score DOUBLE,
                ingestion_timestamp TIMESTAMP
            )"""
        },
        {
            "name": "silver_api_performance",
            "schema": """(
                api_source STRING,
                date DATE,
                total_calls BIGINT,
                avg_response_time_ms DOUBLE,
                success_rate DOUBLE,
                error_count BIGINT,
                performance_grade STRING,
                ingestion_timestamp TIMESTAMP
            )"""
        },
        {
            "name": "silver_product_performance",
            "schema": """(
                product_key STRING,
                product_name STRING,
                price_tier STRING,
                category_std STRING,
                popularity_score DOUBLE,
                quality_rating STRING,
                last_updated DATE,
                ingestion_timestamp TIMESTAMP
            )"""
        }
    ]
    
    for table in tables:
        try:
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS `warehouse`.`chainalytics`.`{table['name']}` 
                {table['schema']}
                USING ICEBERG
                LOCATION 's3a://warehouse/chainalytics/{table['name']}/'
            """)
            logger.info(f"✓ Table {table['name']} created/verified")
        except Exception as e:
            logger.error(f"✗ Failed to create table {table['name']}: {e}")
            raise
    
    logger.info("✓ Bootstrap complete")


def etl_user_behavior(spark: SparkSession) -> None:
    """Process user behavior data"""
    logger.info("Processing user behavior...")
    try:
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
        row_count = agg.count()
        logger.info(f"✓ silver_user_behavior loaded ({row_count} rows)")
        
    except Exception as e:
        logger.error(f"✗ Failed to process user behavior: {e}")
        raise


def etl_product_analytics(spark: SparkSession) -> None:
    """Process product analytics data"""
    logger.info("Processing product analytics...")
    try:
        prod = spark.read.parquet("s3a://warehouse/chainalytics/bronze_products/")
        events = spark.read.parquet("s3a://warehouse/chainalytics/bronze_user_events/")
        
        # Aggregate views and purchases
        views_agg = (
            events.filter(col("event_type") == "view")
            .groupBy("product_id")
            .count()
            .withColumnRenamed("count", "total_views")
        )
        
        purchases_agg = (
            events.filter(col("event_type") == "purchase")
            .groupBy("product_id")
            .count()
            .withColumnRenamed("count", "total_purchases")
        )
        
        # Join and calculate metrics
        agg = (
            prod.join(views_agg, "product_id", "left")
            .join(purchases_agg, "product_id", "left")
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
        row_count = agg.count()
        logger.info(f"✓ silver_product_analytics loaded ({row_count} rows)")
        
    except Exception as e:
        logger.error(f"✗ Failed to process product analytics: {e}")
        raise


def etl_weather_impact(spark: SparkSession) -> None:
    """Process weather impact data"""
    logger.info("Processing weather impact...")
    try:
        df = spark.read.parquet("s3a://warehouse/chainalytics/bronze_weather_data/")
        
        agg = (
            df.withColumn("date", to_date("observation_time"))
            .groupBy("location_id", "date")
            .agg(
                avg("temperature").alias("avg_temperature"),
                avg("data_delay_hours").alias("avg_delay_hours")
            )
            .withColumn("delivery_delay_hours", col("avg_delay_hours").cast("int"))
            .withColumn("weather_category",
                       when(col("avg_temperature") < 10, lit("cold"))
                       .when(col("avg_temperature") < 25, lit("moderate"))
                       .otherwise(lit("hot")))
            .withColumn("impact_score", 
                       expr("delivery_delay_hours / (avg_temperature + 1) * 100"))
            .withColumn("ingestion_timestamp", current_timestamp())
            .select("location_id", "date", "avg_temperature", "weather_category",
                   "delivery_delay_hours", "impact_score", "ingestion_timestamp")
        )
        
        agg.writeTo("warehouse.chainalytics.silver_weather_impact").append()
        row_count = agg.count()
        logger.info(f"✓ silver_weather_impact loaded ({row_count} rows)")
        
    except Exception as e:
        logger.error(f"✗ Failed to process weather impact: {e}")
        raise


def etl_api_performance(spark: SparkSession) -> None:
    """Process API performance data"""
    logger.info("Processing API performance...")
    try:
        df = spark.read.parquet("s3a://warehouse/chainalytics/bronze_api_logs/")
        
        agg = (
            df.withColumn("date", to_date("call_timestamp"))
            .groupBy("api_source", "date")
            .agg(
                count("*").alias("total_calls"),
                avg("response_time_ms").alias("avg_response_time_ms"),
                avg(col("success_flag").cast("int")).alias("success_rate"),
                count(when(col("success_flag") == False, True)).alias("error_count")
            )
            .withColumn("performance_grade",
                       when(col("success_rate") >= 0.9, lit("Good"))
                       .otherwise(lit("Poor")))
            .withColumn("ingestion_timestamp", current_timestamp())
        )
        
        agg.writeTo("warehouse.chainalytics.silver_api_performance").append()
        row_count = agg.count()
        logger.info(f"✓ silver_api_performance loaded ({row_count} rows)")
        
    except Exception as e:
        logger.error(f"✗ Failed to process API performance: {e}")
        raise


def etl_product_performance(spark: SparkSession) -> None:
    """Process product performance data"""
    logger.info("Processing product performance...")
    try:
        df = spark.read.parquet("s3a://warehouse/chainalytics/bronze_products/")
        
        agg = (
            df.groupBy("product_id", "title", "category", "price")
            .agg(
                avg("rating_score").alias("popularity_score"),
                spark_max("ingestion_date").alias("last_updated")
            )
            .withColumn("product_key", col("product_id").cast("string"))
            .withColumn("product_name", col("title"))
            .withColumn("price_tier",
                       when(col("price") < 50, lit("Low"))
                       .when(col("price") < 200, lit("Medium"))
                       .otherwise(lit("High")))
            .withColumn("category_std", upper(col("category")))
            .withColumn("quality_rating",
                       when(col("popularity_score") >= 4.5, lit("Excellent"))
                       .when(col("popularity_score") >= 3.5, lit("Good"))
                       .otherwise(lit("Average")))
            .withColumn("ingestion_timestamp", current_timestamp())
            .select(
                "product_key", "product_name", "price_tier", "category_std",
                "popularity_score", "quality_rating", "last_updated", "ingestion_timestamp"
            )
        )
        
        agg.writeTo("warehouse.chainalytics.silver_product_performance").append()
        row_count = agg.count()
        logger.info(f"✓ silver_product_performance loaded ({row_count} rows)")
        
    except Exception as e:
        logger.error(f"✗ Failed to process product performance: {e}")
        raise


def main() -> None:
    """Main ETL execution function"""
    logger.info("=" * 60)
    logger.info("▶️  ETL: Bronze → Silver")
    logger.info("=" * 60)
    
    spark: Optional[SparkSession] = None
    
    try:
        spark = create_spark_session()
        bootstrap_silver_tables(spark)
        
        # Execute ETL functions
        etl_user_behavior(spark)
        etl_product_analytics(spark)
        etl_weather_impact(spark)
        etl_api_performance(spark)
        etl_product_performance(spark)
        
        logger.info("🎉 ETL complete: all silver tables updated")
        
    except Exception as e:
        logger.error(f"💥 ETL failed: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)
        
    finally:
        if spark:
            spark.stop()
            logger.info("✓ Spark session stopped")


if __name__ == "__main__":
    main()