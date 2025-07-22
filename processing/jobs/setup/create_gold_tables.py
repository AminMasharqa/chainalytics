#!/usr/bin/env python3
"""
Simplified Gold Table Creation Script
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
            .appName("Gold-Tables-MinIO") \
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

def create_gold_tables(spark):
    """Create gold layer tables in chainalytics database"""
    try:
        logger.info("Starting gold table creation...")
        
        # Create gold_customer_analytics table
        logger.info("Creating gold_customer_analytics table...")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS chainalytics.gold_customer_analytics (
                customer_id STRING,
                customer_name STRING,
                total_orders INTEGER,
                total_spent DOUBLE,
                avg_order_value DOUBLE,
                favorite_category STRING,
                last_order_date DATE,
                customer_tier STRING,
                lifetime_value DOUBLE,
                churn_risk_score DOUBLE,
                ingestion_timestamp TIMESTAMP
            ) USING PARQUET
        """)
        logger.info("✓ gold_customer_analytics table created successfully")
        
        # Create gold_daily_summary table
        logger.info("Creating gold_daily_summary table...")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS chainalytics.gold_daily_summary (
                business_date DATE,
                total_revenue DOUBLE,
                total_orders INTEGER,
                active_customers INTEGER,
                new_customers INTEGER,
                avg_order_value DOUBLE,
                top_selling_category STRING,
                weather_impact_score DOUBLE,
                api_performance_score DOUBLE,
                overall_health_score DOUBLE,
                ingestion_timestamp TIMESTAMP
            ) USING PARQUET
        """)
        logger.info("✓ gold_daily_summary table created successfully")
        
        # Create gold_product_performance table
        logger.info("Creating gold_product_performance table...")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS chainalytics.gold_product_performance (
                product_id INTEGER,
                product_name STRING,
                category STRING,
                revenue_rank INTEGER,
                conversion_rank INTEGER,
                customer_satisfaction DOUBLE,
                inventory_status STRING,
                price_optimization_score DOUBLE,
                recommended_action STRING,
                ingestion_timestamp TIMESTAMP
            ) USING PARQUET
        """)
        logger.info("✓ gold_product_performance table created successfully")
        
        # Create gold_executive_dashboard table
        logger.info("Creating gold_executive_dashboard table...")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS chainalytics.gold_executive_dashboard (
                kpi_date DATE,
                revenue_growth_rate DOUBLE,
                customer_acquisition_rate DOUBLE,
                customer_retention_rate DOUBLE,
                average_customer_lifetime_value DOUBLE,
                profit_margin DOUBLE,
                operational_efficiency_score DOUBLE,
                customer_satisfaction_index DOUBLE,
                market_share_estimate DOUBLE,
                ingestion_timestamp TIMESTAMP
            ) USING PARQUET
        """)
        logger.info("✓ gold_executive_dashboard table created successfully")
        
        # Create gold_weather_correlation table
        logger.info("Creating gold_weather_correlation table...")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS chainalytics.gold_weather_correlation (
                correlation_id STRING,
                location_id STRING,
                weather_condition STRING,
                avg_temperature DOUBLE,
                sales_impact_percentage DOUBLE,
                optimal_weather_score DOUBLE,
                business_date DATE,
                ingestion_timestamp TIMESTAMP
            ) USING PARQUET
        """)
        logger.info("✓ gold_weather_correlation table created successfully")
        
        # Verify tables were created
        logger.info("Verifying created tables...")
        tables = spark.sql("SHOW TABLES IN chainalytics").collect()
        table_names = [row.tableName for row in tables]
        
        expected_tables = ['gold_customer_analytics', 'gold_daily_summary', 'gold_product_performance', 'gold_executive_dashboard', 'gold_weather_correlation']
        created_tables = [table for table in expected_tables if table in table_names]
        
        if len(created_tables) == len(expected_tables):
            logger.info("✓ All gold tables verified in chainalytics:")
            for table in created_tables:
                logger.info(f"  - {table}")
            return True
        else:
            missing_tables = [table for table in expected_tables if table not in created_tables]
            logger.error(f"✗ Table verification failed. Missing tables: {missing_tables}")
            return False
        
    except Exception as e:
        logger.error(f"✗ Failed to create gold tables: {str(e)}")
        raise

def insert_test_data(spark):
    """Insert sample test data"""
    try:
        logger.info("Inserting test data...")
        
        # Insert test customer analytics
        logger.info("Inserting test customer analytics...")
        spark.sql("""
            INSERT INTO chainalytics.gold_customer_analytics VALUES 
            ('cust_001', 'John Smith', 25, 2500.00, 100.00, 'electronics', current_date(), 'premium', 5000.00, 0.2, current_timestamp()),
            ('cust_002', 'Jane Doe', 12, 850.00, 70.83, 'clothing', current_date(), 'standard', 1500.00, 0.4, current_timestamp())
        """)
        logger.info("✓ Test customer analytics inserted")
        
        # Insert test daily summary
        logger.info("Inserting test daily summary...")
        spark.sql("""
            INSERT INTO chainalytics.gold_daily_summary VALUES 
            (current_date(), 15000.00, 85, 120, 15, 176.47, 'electronics', 0.92, 0.98, 0.95, current_timestamp()),
            (current_date() - interval '1' day, 14200.00, 78, 115, 12, 182.05, 'clothing', 0.88, 0.96, 0.92, current_timestamp())
        """)
        logger.info("✓ Test daily summary inserted")
        
        # Insert test product performance
        logger.info("Inserting test product performance...")
        spark.sql("""
            INSERT INTO chainalytics.gold_product_performance VALUES 
            (101, 'Premium Widget', 'electronics', 1, 3, 4.5, 'in_stock', 0.85, 'increase_marketing', current_timestamp()),
            (102, 'Standard Tool', 'tools', 5, 2, 4.2, 'low_stock', 0.75, 'reorder_inventory', current_timestamp())
        """)
        logger.info("✓ Test product performance inserted")
        
        # Insert test executive dashboard
        logger.info("Inserting test executive dashboard...")
        spark.sql("""
            INSERT INTO chainalytics.gold_executive_dashboard VALUES 
            (current_date(), 0.15, 0.08, 0.85, 2500.00, 0.32, 0.88, 4.2, 0.12, current_timestamp()),
            (current_date() - interval '1' day, 0.12, 0.06, 0.82, 2450.00, 0.31, 0.86, 4.1, 0.11, current_timestamp())
        """)
        logger.info("✓ Test executive dashboard inserted")
        
        # Insert test weather correlation
        logger.info("Inserting test weather correlation...")
        spark.sql("""
            INSERT INTO chainalytics.gold_weather_correlation VALUES 
            ('corr_001', 'NYC_001', 'sunny', 22.5, 15.2, 0.85, current_date(), current_timestamp()),
            ('corr_002', 'LA_002', 'cloudy', 18.0, -5.8, 0.45, current_date(), current_timestamp())
        """)
        logger.info("✓ Test weather correlation inserted")
        
        # Verify data insertion
        customer_count = spark.sql("SELECT COUNT(*) as count FROM chainalytics.gold_customer_analytics").collect()[0]['count']
        daily_count = spark.sql("SELECT COUNT(*) as count FROM chainalytics.gold_daily_summary").collect()[0]['count']
        product_count = spark.sql("SELECT COUNT(*) as count FROM chainalytics.gold_product_performance").collect()[0]['count']
        executive_count = spark.sql("SELECT COUNT(*) as count FROM chainalytics.gold_executive_dashboard").collect()[0]['count']
        weather_count = spark.sql("SELECT COUNT(*) as count FROM chainalytics.gold_weather_correlation").collect()[0]['count']
        
        logger.info("✓ Data verification:")
        logger.info(f"  - Customer Analytics: {customer_count} records")
        logger.info(f"  - Daily Summary: {daily_count} records")
        logger.info(f"  - Product Performance: {product_count} records")
        logger.info(f"  - Executive Dashboard: {executive_count} records")
        logger.info(f"  - Weather Correlation: {weather_count} records")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ Failed to insert test data: {str(e)}")
        raise

def read_and_display_data(spark):
    """Read and display sample data from all gold tables"""
    try:
        logger.info("Reading data from gold tables in chainalytics database...")

        gold_tables = [
            'gold_customer_analytics',
            'gold_daily_summary',
            'gold_product_performance',
            'gold_executive_dashboard',
            'gold_weather_correlation'
        ]

        for table in gold_tables:
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
        logger.error(f"✗ Failed to read/display data from gold tables: {str(e)}")
        raise


def main():
    """Main execution function"""
    logger.info("=" * 60)
    logger.info("Starting Gold Table Creation Script")
    logger.info("=" * 60)
    
    spark = None
    try:
        # Create Spark session
        spark = create_spark_session()
        
        # Verify catalog and database exist
        verify_catalog_and_database(spark)
        
        # Create tables
        create_gold_tables(spark)
        
        # Insert test data
        insert_test_data(spark)
        
        # Read and display data from tables
        read_and_display_data(spark)
        
        logger.info("=" * 60)
        logger.info("🎉 SUCCESS: Gold tables created in chainalytics database!")
        logger.info("Tables stored in MinIO bucket: s3a://warehouse/chainalytics/")
        logger.info("Tables: gold_customer_analytics, gold_daily_summary, gold_product_performance, gold_executive_dashboard, gold_weather_correlation")
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