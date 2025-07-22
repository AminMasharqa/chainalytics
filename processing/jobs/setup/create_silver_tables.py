#!/usr/bin/env python3
"""
Simple Silver Tables Creation for ChainAnalytics
Creates silver tables in 'warehouse' catalog
Location: jobs/setup/create_silver_tables_simple.py
"""

from pyspark.sql import SparkSession
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create Spark session with Iceberg configuration"""
    spark = SparkSession.builder \
        .appName("SilverTablesCreation") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
        .config("spark.sql.catalog.spark_catalog.warehouse", "s3a://warehouse/") \
        .getOrCreate()
    
    # Configure S3/MinIO settings
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "minioadmin")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "minioadmin")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio:9000")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.attempts.maximum", "1")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.connection.establish.timeout", "5000")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.connection.timeout", "10000")
    
    return spark

def create_silver_tables():
    """Create all silver layer tables"""
    
    logger.info("🚀 Starting silver tables creation")
    
    spark = create_spark_session()
    
    try:
        # Create database in the default catalog
        # spark.sql("CREATE DATABASE IF NOT EXISTS chainalytics")
        spark.sql("USE chainalytics")
        logger.info("✅ Using database: chainalytics")
        
        # 1. Silver User Behavior
        spark.sql("""
            CREATE TABLE IF NOT EXISTS silver_user_behavior (
                user_id STRING,
                total_events INTEGER,
                unique_products_viewed INTEGER,
                avg_session_duration DOUBLE,
                last_activity_date DATE,
                user_segment STRING,
                ingestion_timestamp TIMESTAMP
            )
            USING ICEBERG
            PARTITIONED BY (user_segment)
            TBLPROPERTIES (
                'write.format.default' = 'parquet'
            )
        """)
        logger.info("✅ Created table: chainalytics.silver_user_behavior")
        
        # 2. Silver Product Analytics
        spark.sql("""
            CREATE TABLE IF NOT EXISTS silver_product_analytics (
                product_id INTEGER,
                product_name STRING,
                category STRING,
                current_price DOUBLE,
                avg_rating DOUBLE,
                total_views INTEGER,
                total_purchases INTEGER,
                conversion_rate DOUBLE,
                ingestion_timestamp TIMESTAMP
            )
            USING ICEBERG
            PARTITIONED BY (category)
            TBLPROPERTIES (
                'write.format.default' = 'parquet'
            )
        """)
        logger.info("✅ Created table: chainalytics.silver_product_analytics")
        
        # 3. Silver Weather Impact
        spark.sql("""
            CREATE TABLE IF NOT EXISTS silver_weather_impact (
                location_id STRING,
                date DATE,
                avg_temperature DOUBLE,
                weather_category STRING,
                delivery_delay_hours INTEGER,
                impact_score DOUBLE,
                ingestion_timestamp TIMESTAMP
            )
            USING ICEBERG
            PARTITIONED BY (weather_category)
            TBLPROPERTIES (
                'write.format.default' = 'parquet'
            )
        """)
        logger.info("✅ Created table: chainalytics.silver_weather_impact")
        
        # 4. Silver API Performance
        spark.sql("""
            CREATE TABLE IF NOT EXISTS silver_api_performance (
                api_source STRING,
                date DATE,
                total_calls INTEGER,
                avg_response_time_ms DOUBLE,
                success_rate DOUBLE,
                error_count INTEGER,
                performance_grade STRING,
                ingestion_timestamp TIMESTAMP
            )
            USING ICEBERG
            PARTITIONED BY (api_source)
            TBLPROPERTIES (
                'write.format.default' = 'parquet'
            )
        """)
        logger.info("✅ Created table: chainalytics.silver_api_performance")
        
        # Show all tables created
        logger.info("📊 All Silver tables created:")
        spark.sql("SHOW TABLES").show()
        
        logger.info("✅ Silver tables creation completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Error creating silver tables: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    create_silver_tables()