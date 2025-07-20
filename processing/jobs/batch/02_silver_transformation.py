#!/usr/bin/env python3
"""
Silver Layer Transformation - Clean and standardize bronze data
Simple transformations with business logic
"""

import sys
sys.path.append('/opt/spark/config')
sys.path.append('/opt/spark/utils')

from spark_config import spark_config
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def silver_transformation():
    """Transform bronze data into clean silver tables"""
    
    logger.info("Starting silver layer transformation")
    
    try:
        spark = spark_config.create_spark_session()
        
        # 1. Transform Product Performance Data
        df_bronze_products = spark.table("iceberg_catalog.chainalytics.bronze_product_performance")
        
        df_silver_products = df_bronze_products \
            .filter(col("price") > 0) \
            .filter(col("rating") >= 1.0) \
            .withColumn("product_key", col("product_id")) \
            .withColumn("category_std", upper(col("category"))) \
            .withColumn("price_tier", 
                when(col("price") < 100, "Low")
                .when(col("price") < 500, "Medium")
                .otherwise("High")) \
            .withColumn("popularity_score", 
                col("rating") * col("stock_quantity") / 100.0) \
            .withColumn("quality_rating",
                when(col("rating") >= 4.5, "Excellent")
                .when(col("rating") >= 4.0, "Good")
                .when(col("rating") >= 3.0, "Average")
                .otherwise("Poor")) \
            .withColumn("last_updated", current_date()) \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .select("product_key", "product_name", "price_tier", "category_std", 
                   "popularity_score", "quality_rating", "last_updated", "ingestion_timestamp")
        
        df_silver_products.writeTo("iceberg_catalog.chainalytics.silver_product_performance") \
                          .createOrReplace()
        
        logger.info(f"✅ Transformed {df_silver_products.count()} products to silver")
        
        # 2. Transform User Sessions Data
        df_bronze_sessions = spark.table("iceberg_catalog.chainalytics.bronze_user_sessions")
        
        df_silver_sessions = df_bronze_sessions \
            .filter(col("duration_minutes") > 0) \
            .withColumn("session_duration_min", col("duration_minutes")) \
            .withColumn("total_events", col("page_views")) \
            .withColumn("conversion_flag", col("converted")) \
            .withColumn("session_start", to_timestamp(col("session_start"))) \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .select("session_id", "user_id", "session_duration_min", "total_events",
                   "conversion_flag", "session_start", "ingestion_timestamp")
        
        df_silver_sessions.writeTo("iceberg_catalog.chainalytics.silver_user_sessions") \
                          .createOrReplace()
        
        logger.info(f"✅ Transformed {df_silver_sessions.count()} sessions to silver")
        
        # 3. Transform Customer Behavior Data
        df_bronze_customers = spark.table("iceberg_catalog.chainalytics.bronze_customer_behavior")
        
        df_silver_customers = df_bronze_customers \
            .withColumn("customer_id", col("user_id")) \
            .withColumn("engagement_level", initcap(col("engagement_level"))) \
            .withColumn("avg_session_min", col("avg_session_minutes")) \
            .withColumn("purchase_frequency", col("monthly_purchases")) \
            .withColumn("first_activity", to_date(col("first_activity"))) \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .select("customer_id", "engagement_level", "avg_session_min", "purchase_frequency",
                   "preferred_category", "first_activity", "ingestion_timestamp")
        
        df_silver_customers.writeTo("iceberg_catalog.chainalytics.silver_customer_behavior") \
                           .createOrReplace()
        
        logger.info(f"✅ Transformed {df_silver_customers.count()} customers to silver")
        
        # Add sample system health data
        system_health_data = [
            ("health_001", 99.5, 120, "2025-01-15 10:00:00"),
            ("health_002", 98.8, 150, "2025-01-15 11:00:00"),
            ("health_003", 99.9, 100, "2025-01-15 12:00:00")
        ]
        
        system_schema = StructType([
            StructField("health_id", StringType(), True),
            StructField("api_availability", DoubleType(), True),
            StructField("avg_response_time", IntegerType(), True),
            StructField("measurement_time", StringType(), True)
        ])
        
        df_system = spark.createDataFrame(system_health_data, system_schema) \
            .withColumn("measurement_time", to_timestamp(col("measurement_time"))) \
            .withColumn("ingestion_timestamp", current_timestamp())
        
        df_system.writeTo("iceberg_catalog.chainalytics.silver_system_health") \
                 .createOrReplace()
        
        logger.info("Silver transformation completed successfully")
        
    except Exception as e:
        logger.error(f"Error in silver transformation: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    silver_transformation()