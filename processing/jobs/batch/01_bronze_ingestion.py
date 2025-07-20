#!/usr/bin/env python3
"""
Bronze Layer Ingestion - Load sample CSV data into Iceberg bronze tables
Simple approach: Load sample data and add ingestion metadata
"""

import sys
sys.path.append('/opt/spark/config')
sys.path.append('/opt/spark/utils')

from spark_config import spark_config
from pyspark.sql.functions import current_timestamp, lit, monotonically_increasing_id
from pyspark.sql.types import *
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_sample_data(spark):
    """Create sample data to simulate real data sources"""
    
    # Sample Product Performance Data
    product_data = [
        ("PROD001", "Laptop Pro", "Electronics", 4.5, 1299.99, 150),
        ("PROD002", "Coffee Maker", "Home", 4.2, 89.99, 300),
        ("PROD003", "Running Shoes", "Sports", 4.7, 129.99, 200),
        ("PROD004", "Smartphone", "Electronics", 4.6, 899.99, 100),
        ("PROD005", "Desk Chair", "Furniture", 4.1, 249.99, 80)
    ]
    
    product_schema = StructType([
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("rating", DoubleType(), True),
        StructField("price", DoubleType(), True),
        StructField("stock_quantity", IntegerType(), True)
    ])
    
    # Sample User Sessions Data
    session_data = [
        ("sess_001", "user_123", 25, 12, True, "2025-01-15 10:30:00"),
        ("sess_002", "user_456", 8, 3, False, "2025-01-15 11:45:00"),
        ("sess_003", "user_789", 45, 18, True, "2025-01-15 14:20:00"),
        ("sess_004", "user_123", 15, 6, False, "2025-01-15 16:10:00"),
        ("sess_005", "user_321", 35, 14, True, "2025-01-15 18:30:00")
    ]
    
    session_schema = StructType([
        StructField("session_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("duration_minutes", IntegerType(), True),
        StructField("page_views", IntegerType(), True),
        StructField("converted", BooleanType(), True),
        StructField("session_start", StringType(), True)
    ])
    
    # Sample Customer Behavior Data
    customer_data = [
        ("user_123", "high", 30, 5, "Electronics", "2025-01-01"),
        ("user_456", "medium", 15, 2, "Home", "2025-01-10"),
        ("user_789", "high", 40, 8, "Sports", "2024-12-15"),
        ("user_321", "low", 10, 1, "Furniture", "2025-01-12"),
        ("user_654", "medium", 20, 3, "Electronics", "2025-01-05")
    ]
    
    customer_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("engagement_level", StringType(), True),
        StructField("avg_session_minutes", IntegerType(), True),
        StructField("monthly_purchases", IntegerType(), True),
        StructField("preferred_category", StringType(), True),
        StructField("first_activity", StringType(), True)
    ])
    
    return {
        'products': spark.createDataFrame(product_data, product_schema),
        'sessions': spark.createDataFrame(session_data, session_schema),
        'customers': spark.createDataFrame(customer_data, customer_schema)
    }

def bronze_ingestion():
    """Load sample data into bronze tables"""
    
    logger.info("Starting bronze data ingestion")
    
    try:
        spark = spark_config.create_spark_session()
        
        # Create sample data
        sample_data = create_sample_data(spark)
        
        # Load Product Performance into Bronze
        df_products = sample_data['products'] \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("source_system", lit("sample_data")) \
            .withColumn("data_quality_score", lit(95.0))
        
        df_products.writeTo("iceberg_catalog.chainalytics.bronze_product_performance") \
                   .createOrReplace()
        
        logger.info(f"✅ Loaded {df_products.count()} product records to bronze")
        
        # Load User Sessions into Bronze  
        df_sessions = sample_data['sessions'] \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("source_system", lit("web_analytics")) \
            .withColumn("session_quality", lit("validated"))
        
        df_sessions.writeTo("iceberg_catalog.chainalytics.bronze_user_sessions") \
                   .createOrReplace()
        
        logger.info(f"✅ Loaded {df_sessions.count()} session records to bronze")
        
        # Load Customer Behavior into Bronze
        df_customers = sample_data['customers'] \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("source_system", lit("crm_system")) \
            .withColumn("profile_completeness", lit(85.0))
        
        df_customers.writeTo("iceberg_catalog.chainalytics.bronze_customer_behavior") \
                    .createOrReplace()
        
        logger.info(f"✅ Loaded {df_customers.count()} customer records to bronze")
        
        # Verify data was loaded
        for table in ["bronze_product_performance", "bronze_user_sessions", "bronze_customer_behavior"]:
            count = spark.table(f"iceberg_catalog.chainalytics.{table}").count()
            logger.info(f"📊 {table}: {count} records")
        
        logger.info("Bronze ingestion completed successfully")
        
    except Exception as e:
        logger.error(f"Error in bronze ingestion: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    bronze_ingestion()