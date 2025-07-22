#!/usr/bin/env python3
"""
Simplified Silver Tables Creation for ChainAnalytics
Creates silver tables directly in warehouse.chainalytics database
"""

import logging
import sys
from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create basic Spark session"""
    try:
        logger.info("Creating Spark session...")
        spark = SparkSession.builder \
            .appName("Silver-Tables-Simple") \
            .getOrCreate()
        
        logger.info("✓ Spark session created successfully")
        return spark
    except Exception as e:
        logger.error(f"✗ Failed to create Spark session: {str(e)}")
        raise

def verify_catalog_and_database(spark):
    """Verify that warehouse catalog and chainalytics database exist"""
    try:
        logger.info("Checking if warehouse catalog exists...")
        catalogs = [row.catalog for row in spark.sql("SHOW CATALOGS").collect()]
        if 'warehouse' not in catalogs:
            error_msg = "Catalog 'warehouse' does not exist"
            logger.error(f"✗ {error_msg}")
            logger.error(f"Available catalogs: {catalogs}")
            raise Exception(error_msg)
        
        logger.info("✓ Warehouse catalog found")
        
        logger.info("Checking if chainalytics database exists...")
        databases = [row.namespace for row in spark.sql("SHOW DATABASES IN warehouse").collect()]
        if 'chainalytics' not in databases:
            error_msg = "Database 'chainalytics' does not exist in warehouse catalog"
            logger.error(f"✗ {error_msg}")
            logger.error(f"Available databases in warehouse: {databases}")
            raise Exception(error_msg)
        
        logger.info("✓ Chainalytics database found in warehouse catalog")
        return True
        
    except Exception as e:
        logger.error(f"✗ Catalog/Database verification failed: {str(e)}")
        raise

def create_silver_tables(spark):
    """Create silver layer tables in warehouse.chainalytics"""
    try:
        logger.info("Starting silver table creation...")
        
        # Create silver_user_behavior table
        logger.info("Creating silver_user_behavior table...")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS warehouse.chainalytics.silver_user_behavior (
                user_id STRING,
                total_events INTEGER,
                unique_products_viewed INTEGER,
                avg_session_duration DOUBLE,
                last_activity_date DATE,
                user_segment STRING,
                ingestion_timestamp TIMESTAMP
            ) USING DELTA
            PARTITIONED BY (user_segment)
        """)
        logger.info("✓ silver_user_behavior table created successfully")
        
        # Create silver_product_analytics table
        logger.info("Creating silver_product_analytics table...")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS warehouse.chainalytics.silver_product_analytics (
                product_id INTEGER,
                product_name STRING,
                category STRING,
                current_price DOUBLE,
                avg_rating DOUBLE,
                total_views INTEGER,
                total_purchases INTEGER,
                conversion_rate DOUBLE,
                ingestion_timestamp TIMESTAMP
            ) USING DELTA
            PARTITIONED BY (category)
        """)
        logger.info("✓ silver_product_analytics table created successfully")
        
        # Create silver_weather_impact table
        logger.info("Creating silver_weather_impact table...")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS warehouse.chainalytics.silver_weather_impact (
                location_id STRING,
                date DATE,
                avg_temperature DOUBLE,
                weather_category STRING,
                delivery_delay_hours INTEGER,
                impact_score DOUBLE,
                ingestion_timestamp TIMESTAMP
            ) USING DELTA
            PARTITIONED BY (weather_category)
        """)
        logger.info("✓ silver_weather_impact table created successfully")
        
        # Create silver_api_performance table
        logger.info("Creating silver_api_performance table...")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS warehouse.chainalytics.silver_api_performance (
                api_source STRING,
                date DATE,
                total_calls INTEGER,
                avg_response_time_ms DOUBLE,
                success_rate DOUBLE,
                error_count INTEGER,
                performance_grade STRING,
                ingestion_timestamp TIMESTAMP
            ) USING DELTA
            PARTITIONED BY (api_source)
        """)
        logger.info("✓ silver_api_performance table created successfully")
        
        # Verify tables were created
        logger.info("Verifying created silver tables...")
        tables = spark.sql("SHOW TABLES IN warehouse.chainalytics").collect()
        silver_tables = [row.tableName for row in tables if row.tableName.startswith('silver_')]
        
        expected_tables = ['silver_user_behavior', 'silver_product_analytics', 'silver_weather_impact', 'silver_api_performance']
        created_tables = [table for table in expected_tables if table in silver_tables]
        
        if len(created_tables) == len(expected_tables):
            logger.info("✓ All silver tables verified in warehouse.chainalytics:")
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
    """Insert sample test data into silver tables"""
    try:
        logger.info("Inserting test data into silver tables...")
        
        # Insert test user behavior data
        logger.info("Inserting test user behavior data...")
        spark.sql("""
            INSERT INTO warehouse.chainalytics.silver_user_behavior VALUES 
            ('user_001', 45, 12, 8.5, current_date(), 'premium', current_timestamp()),
            ('user_002', 23, 8, 5.2, current_date(), 'standard', current_timestamp())
        """)
        logger.info("✓ Test user behavior data inserted")
        
        # Insert test product analytics data
        logger.info("Inserting test product analytics data...")
        spark.sql("""
            INSERT INTO warehouse.chainalytics.silver_product_analytics VALUES 
            (101, 'Premium Widget', 'electronics', 299.99, 4.5, 1500, 45, 0.03, current_timestamp()),
            (102, 'Standard Tool', 'tools', 49.99, 4.2, 800, 32, 0.04, current_timestamp())
        """)
        logger.info("✓ Test product analytics data inserted")
        
        # Insert test weather impact data
        logger.info("Inserting test weather impact data...")
        spark.sql("""
            INSERT INTO warehouse.chainalytics.silver_weather_impact VALUES 
            ('NYC_001', current_date(), 22.5, 'sunny', 0, 0.95, current_timestamp()),
            ('LA_002', current_date(), 28.0, 'cloudy', 1, 0.85, current_timestamp())
        """)
        logger.info("✓ Test weather impact data inserted")
        
        # Insert test API performance data
        logger.info("Inserting test API performance data...")
        spark.sql("""
            INSERT INTO warehouse.chainalytics.silver_api_performance VALUES 
            ('payment_api', current_date(), 5000, 120.5, 99.8, 10, 'A', current_timestamp()),
            ('inventory_api', current_date(), 3200, 85.2, 99.5, 16, 'A', current_timestamp())
        """)
        logger.info("✓ Test API performance data inserted")
        
        # Verify data insertion
        user_count = spark.sql("SELECT COUNT(*) as count FROM warehouse.chainalytics.silver_user_behavior").collect()[0]['count']
        product_count = spark.sql("SELECT COUNT(*) as count FROM warehouse.chainalytics.silver_product_analytics").collect()[0]['count']
        weather_count = spark.sql("SELECT COUNT(*) as count FROM warehouse.chainalytics.silver_weather_impact").collect()[0]['count']
        api_count = spark.sql("SELECT COUNT(*) as count FROM warehouse.chainalytics.silver_api_performance").collect()[0]['count']
        
        logger.info("✓ Data verification:")
        logger.info(f"  - User Behavior: {user_count} records")
        logger.info(f"  - Product Analytics: {product_count} records")
        logger.info(f"  - Weather Impact: {weather_count} records")
        logger.info(f"  - API Performance: {api_count} records")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ Failed to insert test data: {str(e)}")
        raise

def main():
    """Main execution function"""
    logger.info("=" * 60)
    logger.info("Starting Silver Tables Creation Script")
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
        
        logger.info("=" * 60)
        logger.info("🎉 SUCCESS: Silver tables created in warehouse.chainalytics!")
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