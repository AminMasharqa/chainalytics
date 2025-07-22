#!/usr/bin/env python3
"""
Simplified Silver Table Creation Script
Creates tables directly in warehouse.chainalytics database
"""

import logging
import sys
from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create Spark session configured for MinIO storage"""
    try:
        logger.info("Creating Spark session with MinIO configuration...")
        spark = SparkSession.builder \
            .appName("Silver-Tables-MinIO") \
            .config("spark.sql.warehouse.dir", "s3a://warehouse/") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://chainalytics-minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .getOrCreate()
        
        logger.info("✓ Spark session created with MinIO configuration")
        return spark
    except Exception as e:
        logger.error(f"✗ Failed to create Spark session: {str(e)}")
        raise

def verify_catalog_and_database(spark):
    """Verify that spark_catalog and chainalytics database exist"""
    try:
        logger.info("Checking if spark_catalog exists...")
        catalogs = [row.catalog for row in spark.sql("SHOW CATALOGS").collect()]
        if 'spark_catalog' not in catalogs:
            error_msg = "Catalog 'spark_catalog' does not exist"
            logger.error(f"✗ {error_msg}")
            logger.error(f"Available catalogs: {catalogs}")
            raise Exception(error_msg)
        
        logger.info("✓ Spark catalog found")
        
        logger.info("Creating chainalytics database if it doesn't exist...")
        spark.sql("CREATE DATABASE IF NOT EXISTS chainalytics")
        logger.info("✓ Chainalytics database created/verified")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ Catalog/Database verification failed: {str(e)}")
        raise

def create_silver_tables(spark):
    """Create silver layer tables in chainalytics database"""
    try:
        logger.info("Starting silver table creation...")
        
        # Create silver_user_behavior table
        logger.info("Creating silver_user_behavior table...")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS chainalytics.silver_user_behavior (
                user_id STRING,
                total_events INTEGER,
                unique_products_viewed INTEGER,
                avg_session_duration DOUBLE,
                last_activity_date DATE,
                user_segment STRING,
                ingestion_timestamp TIMESTAMP
            ) USING PARQUET
        """)
        logger.info("✓ silver_user_behavior table created successfully")
        
        # Create silver_product_analytics table
        logger.info("Creating silver_product_analytics table...")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS chainalytics.silver_product_analytics (
                product_id INTEGER,
                product_name STRING,
                category STRING,
                current_price DOUBLE,
                avg_rating DOUBLE,
                total_views INTEGER,
                total_purchases INTEGER,
                conversion_rate DOUBLE,
                ingestion_timestamp TIMESTAMP
            ) USING PARQUET
        """)
        logger.info("✓ silver_product_analytics table created successfully")
        
        # Create silver_weather_impact table
        logger.info("Creating silver_weather_impact table...")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS chainalytics.silver_weather_impact (
                location_id STRING,
                date DATE,
                avg_temperature DOUBLE,
                weather_category STRING,
                delivery_delay_hours INTEGER,
                impact_score DOUBLE,
                ingestion_timestamp TIMESTAMP
            ) USING PARQUET
        """)
        logger.info("✓ silver_weather_impact table created successfully")
        
        # Create silver_api_performance table
        logger.info("Creating silver_api_performance table...")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS chainalytics.silver_api_performance (
                api_source STRING,
                date DATE,
                total_calls INTEGER,
                avg_response_time_ms DOUBLE,
                success_rate DOUBLE,
                error_count INTEGER,
                performance_grade STRING,
                ingestion_timestamp TIMESTAMP
            ) USING PARQUET
        """)
        logger.info("✓ silver_api_performance table created successfully")

        # Create silver_product_performance table
        logger.info("Creating silver_product_performance table...")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS chainalytics.silver_product_performance (
                product_key STRING,
                product_name STRING,
                price_tier STRING,
                category_std STRING,
                popularity_score DOUBLE,
                quality_rating STRING,
                last_updated DATE,
                ingestion_timestamp TIMESTAMP
            ) USING PARQUET
        """)
        logger.info("✓ silver_product_performance table created successfully")

        
        
        # Verify tables were created
        logger.info("Verifying created tables...")
        tables = spark.sql("SHOW TABLES IN chainalytics").collect()
        table_names = [row.tableName for row in tables]
        
        expected_tables = ['silver_user_behavior', 'silver_product_analytics', 'silver_weather_impact', 'silver_api_performance']
        created_tables = [table for table in expected_tables if table in table_names]
        
        if len(created_tables) == len(expected_tables):
            logger.info("✓ All silver tables verified in chainalytics:")
            for table in created_tables:
                logger.info(f"  - {table}")
            return True
        else:
            missing_tables = [table for table in expected_tables if table not in created_tables]
            logger.error(f"✗ Table verification failed. Missing tables: {missing_tables}")
            return False
        
    except Exception as e:
        logger.error(f"✗ Failed to create silver tables: {str(e)}")
        raise

def insert_test_data(spark):
    """Insert sample test data"""
    try:
        logger.info("Inserting test data...")
        
        # Insert test user behavior data
        logger.info("Inserting test user behavior data...")
        spark.sql("""
            INSERT INTO chainalytics.silver_user_behavior VALUES 
            ('user_001', 45, 12, 8.5, current_date(), 'premium', current_timestamp()),
            ('user_002', 23, 8, 5.2, current_date(), 'standard', current_timestamp())
        """)
        logger.info("✓ Test user behavior data inserted")
        
        # Insert test product analytics data
        logger.info("Inserting test product analytics data...")
        spark.sql("""
            INSERT INTO chainalytics.silver_product_analytics VALUES 
            (101, 'Premium Widget', 'electronics', 299.99, 4.5, 1500, 45, 0.03, current_timestamp()),
            (102, 'Standard Tool', 'tools', 49.99, 4.2, 800, 32, 0.04, current_timestamp())
        """)
        logger.info("✓ Test product analytics data inserted")
        
        # Insert test weather impact data
        logger.info("Inserting test weather impact data...")
        spark.sql("""
            INSERT INTO chainalytics.silver_weather_impact VALUES 
            ('NYC_001', current_date(), 22.5, 'sunny', 0, 0.95, current_timestamp()),
            ('LA_002', current_date(), 28.0, 'cloudy', 1, 0.85, current_timestamp())
        """)
        logger.info("✓ Test weather impact data inserted")
        
        # Insert test API performance data
        logger.info("Inserting test API performance data...")
        spark.sql("""
            INSERT INTO chainalytics.silver_api_performance VALUES 
            ('payment_api', current_date(), 5000, 120.5, 99.8, 10, 'A', current_timestamp()),
            ('inventory_api', current_date(), 3200, 85.2, 99.5, 16, 'A', current_timestamp())
        """)
        logger.info("✓ Test API performance data inserted")
        
        # Verify data insertion
        user_count = spark.sql("SELECT COUNT(*) as count FROM chainalytics.silver_user_behavior").collect()[0]['count']
        product_count = spark.sql("SELECT COUNT(*) as count FROM chainalytics.silver_product_analytics").collect()[0]['count']
        weather_count = spark.sql("SELECT COUNT(*) as count FROM chainalytics.silver_weather_impact").collect()[0]['count']
        api_count = spark.sql("SELECT COUNT(*) as count FROM chainalytics.silver_api_performance").collect()[0]['count']
        
        logger.info("✓ Data verification:")
        logger.info(f"  - User Behavior: {user_count} records")
        logger.info(f"  - Product Analytics: {product_count} records")
        logger.info(f"  - Weather Impact: {weather_count} records")
        logger.info(f"  - API Performance: {api_count} records")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ Failed to insert test data: {str(e)}")
        raise

def read_and_display_data(spark):
    """Read and display sample data from all silver tables"""
    try:
        logger.info("Reading data from silver tables in chainalytics database...")

        silver_tables = [
            'silver_user_behavior',
            'silver_product_analytics',
            'silver_weather_impact',
            'silver_api_performance'
        ]

        for table in silver_tables:
            logger.info(f"📖 Reading from table: {table}")
            df = spark.sql(f"SELECT * FROM chainalytics.{table}")
            count = df.count()
            logger.info(f"✓ {table} contains {count} records")

            if count > 0:
                logger.info(f"🧾 Showing top records from {table}:")
                df.show(truncate=False)
                logger.info(f"📋 Schema for {table}:")
                df.printSchema()
            else:
                logger.warning(f"⚠️ No data found in table: {table}")
            logger.info("-" * 80)

        return True

    except Exception as e:
        logger.error(f"✗ Failed to read/display data from silver tables: {str(e)}")
        raise

def main():
    """Main execution function"""
    logger.info("=" * 60)
    logger.info("Starting Silver Table Creation Script")
    logger.info("=" * 60)
    
    spark = None
    try:
        # Create Spark session
        spark = create_spark_session()
        
        # Verify catalog and database exist
        verify_catalog_and_database(spark)
        
        # Create tables
        create_silver_tables(spark)
        
        # Insert test data
        insert_test_data(spark)
        
        # Read and display data from tables
        read_and_display_data(spark)
        
        logger.info("=" * 60)
        logger.info("🎉 SUCCESS: Silver tables created in chainalytics database!")
        logger.info("Tables stored in MinIO bucket: s3a://warehouse/chainalytics/")
        logger.info("Tables: silver_user_behavior, silver_product_analytics, silver_weather_impact, silver_api_performance")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error("=" * 60)
        logger.error(f"💥 SCRIPT FAILED: {str(e)}")
        logger.error("=" * 60)
        sys.exit(1)
        
    finally:
        if spark:
            logger.info("Stopping Spark session...")
            spark.stop()
            logger.info("✓ Spark session stopped")

if __name__ == "__main__":
    main()