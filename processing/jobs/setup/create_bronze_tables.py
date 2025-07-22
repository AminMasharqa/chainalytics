#!/usr/bin/env python3
"""
Simplified Bronze Table Creation Script
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
            .appName("Bronze-Tables-MinIO") \
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

def create_bronze_tables(spark):
    """Create bronze layer tables in chainalytics database"""
    try:
        logger.info("Starting bronze table creation...")
        
        # Create bronze_user_events table
        logger.info("Creating bronze_user_events table...")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS chainalytics.bronze_user_events (
                event_id STRING,
                user_id STRING,
                event_type STRING,
                product_id STRING,
                event_timestamp TIMESTAMP,
                ingestion_timestamp TIMESTAMP
            ) USING PARQUET
        """)
        logger.info("✓ bronze_user_events table created successfully")
        
        # Create bronze_weather_data table
        logger.info("Creating bronze_weather_data table...")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS chainalytics.bronze_weather_data (
                location_id STRING,
                weather_condition STRING,
                temperature DOUBLE,
                wind_speed DOUBLE,
                data_delay_hours INTEGER,
                observation_time TIMESTAMP,
                ingestion_timestamp TIMESTAMP
            ) USING PARQUET
        """)
        logger.info("✓ bronze_weather_data table created successfully")
        
        # Create bronze_products table
        logger.info("Creating bronze_products table...")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS chainalytics.bronze_products (
                product_id INTEGER,
                title STRING,
                price DOUBLE,
                category STRING,
                rating_score DOUBLE,
                rating_count INTEGER,
                ingestion_date DATE,
                ingestion_timestamp TIMESTAMP
            ) USING PARQUET
        """)
        logger.info("✓ bronze_products table created successfully")
        
        # Create bronze_api_logs table
        logger.info("Creating bronze_api_logs table...")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS chainalytics.bronze_api_logs (
                log_id STRING,
                api_source STRING,
                response_time_ms INTEGER,
                success_flag BOOLEAN,
                call_timestamp TIMESTAMP,
                ingestion_timestamp TIMESTAMP
            ) USING PARQUET
        """)
        logger.info("✓ bronze_api_logs table created successfully")
        
        # Create bronze_user_posts table
        logger.info("Creating bronze_user_posts table...")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS chainalytics.bronze_user_posts (
                post_id INTEGER,
                user_id INTEGER,
                title STRING,
                created_timestamp TIMESTAMP,
                ingestion_timestamp TIMESTAMP
            ) USING PARQUET
        """)
        logger.info("✓ bronze_user_posts table created successfully")
        
        # Verify tables were created
        logger.info("Verifying created tables...")
        tables = spark.sql("SHOW TABLES IN chainalytics").collect()
        table_names = [row.tableName for row in tables]
        
        expected_tables = ['bronze_user_events', 'bronze_weather_data', 'bronze_products', 'bronze_api_logs', 'bronze_user_posts']
        created_tables = [table for table in expected_tables if table in table_names]
        
        if len(created_tables) == len(expected_tables):
            logger.info("✓ All bronze tables verified in chainalytics:")
            for table in created_tables:
                logger.info(f"  - {table}")
            return True
        else:
            missing_tables = [table for table in expected_tables if table not in created_tables]
            logger.error(f"✗ Table verification failed. Missing tables: {missing_tables}")
            return False
        
    except Exception as e:
        logger.error(f"✗ Failed to create bronze tables: {str(e)}")
        raise

def insert_test_data(spark):
    """Insert sample test data"""
    try:
        logger.info("Inserting test data...")
        
        # Insert test user events
        logger.info("Inserting test user events...")
        spark.sql("""
            INSERT INTO chainalytics.bronze_user_events VALUES 
            ('evt_001', 'user_123', 'view', 'prod_456', current_timestamp(), current_timestamp()),
            ('evt_002', 'user_124', 'purchase', 'prod_789', current_timestamp(), current_timestamp())
        """)
        logger.info("✓ Test user events inserted")
        
        # Insert test weather data
        logger.info("Inserting test weather data...")
        spark.sql("""
            INSERT INTO chainalytics.bronze_weather_data VALUES 
            ('NYC_001', 'sunny', 22.5, 15.2, 0, current_timestamp(), current_timestamp()),
            ('LA_002', 'cloudy', 18.0, 12.8, 1, current_timestamp(), current_timestamp())
        """)
        logger.info("✓ Test weather data inserted")
        
        # Insert test products
        logger.info("Inserting test products...")
        spark.sql("""
            INSERT INTO chainalytics.bronze_products VALUES 
            (101, 'Premium Widget', 299.99, 'electronics', 4.5, 150, current_date(), current_timestamp()),
            (102, 'Standard Tool', 49.99, 'tools', 4.2, 85, current_date(), current_timestamp())
        """)
        logger.info("✓ Test products inserted")
        
        # Insert test API logs
        logger.info("Inserting test API logs...")
        spark.sql("""
            INSERT INTO chainalytics.bronze_api_logs VALUES 
            ('log_001', 'payment_api', 120, true, current_timestamp(), current_timestamp()),
            ('log_002', 'inventory_api', 85, true, current_timestamp(), current_timestamp())
        """)
        logger.info("✓ Test API logs inserted")
        
        # Insert test user posts
        logger.info("Inserting test user posts...")
        spark.sql("""
            INSERT INTO chainalytics.bronze_user_posts VALUES 
            (1, 123, 'Great product review', current_timestamp(), current_timestamp()),
            (2, 124, 'Product feedback', current_timestamp(), current_timestamp())
        """)
        logger.info("✓ Test user posts inserted")
        
        # Verify data insertion
        events_count = spark.sql("SELECT COUNT(*) as count FROM chainalytics.bronze_user_events").collect()[0]['count']
        weather_count = spark.sql("SELECT COUNT(*) as count FROM chainalytics.bronze_weather_data").collect()[0]['count']
        products_count = spark.sql("SELECT COUNT(*) as count FROM chainalytics.bronze_products").collect()[0]['count']
        api_count = spark.sql("SELECT COUNT(*) as count FROM chainalytics.bronze_api_logs").collect()[0]['count']
        posts_count = spark.sql("SELECT COUNT(*) as count FROM chainalytics.bronze_user_posts").collect()[0]['count']
        
        logger.info("✓ Data verification:")
        logger.info(f"  - User Events: {events_count} records")
        logger.info(f"  - Weather Data: {weather_count} records")
        logger.info(f"  - Products: {products_count} records")
        logger.info(f"  - API Logs: {api_count} records")
        logger.info(f"  - User Posts: {posts_count} records")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ Failed to insert test data: {str(e)}")
        raise

def read_and_display_data(spark):
    """Read and display sample data from all bronze tables"""
    try:
        logger.info("Reading data from bronze tables in chainalytics database...")

        bronze_tables = [
            'bronze_user_events',
            'bronze_weather_data',
            'bronze_products',
            'bronze_api_logs',
            'bronze_user_posts'
        ]

        for table in bronze_tables:
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
        logger.error(f"✗ Failed to read/display data from bronze tables: {str(e)}")
        raise


def main():
    """Main execution function"""
    logger.info("=" * 60)
    logger.info("Starting Bronze Table Creation Script")
    logger.info("=" * 60)
    
    spark = None
    try:
        # Create Spark session
        spark = create_spark_session()
        
        # Verify catalog and database exist
        verify_catalog_and_database(spark)
        
        # Create tables
        create_bronze_tables(spark)
        
        # Insert test data
        insert_test_data(spark)
        
        # Read and display data from tables
        read_and_display_data(spark)
        
        logger.info("=" * 60)
        logger.info("🎉 SUCCESS: Bronze tables created in chainalytics database!")
        logger.info("Tables stored in MinIO bucket: s3a://warehouse/chainalytics/")
        logger.info("Tables: bronze_user_events, bronze_weather_data, bronze_products, bronze_api_logs, bronze_user_posts")
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