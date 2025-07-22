#!/usr/bin/env python3
"""
Simplified Gold Tables Creation for ChainAnalytics
Creates gold tables directly in warehouse.chainalytics database
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
            .appName("Gold-Tables-Simple") \
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

def create_gold_tables(spark):
    """Create gold layer tables in warehouse.chainalytics"""
    try:
        logger.info("Starting gold table creation...")
        
        # Create gold_customer_dashboard table
        logger.info("Creating gold_customer_dashboard table...")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS warehouse.chainalytics.gold_customer_dashboard (
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
            ) USING DELTA
            PARTITIONED BY (customer_tier)
        """)
        logger.info("✓ gold_customer_dashboard table created successfully")
        
        # Create gold_daily_business_summary table
        logger.info("Creating gold_daily_business_summary table...")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS warehouse.chainalytics.gold_daily_business_summary (
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
            ) USING DELTA
            PARTITIONED BY (business_date)
        """)
        logger.info("✓ gold_daily_business_summary table created successfully")
        
        # Create gold_product_insights table
        logger.info("Creating gold_product_insights table...")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS warehouse.chainalytics.gold_product_insights (
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
            ) USING DELTA
            PARTITIONED BY (category)
        """)
        logger.info("✓ gold_product_insights table created successfully")
        
        # Create gold_executive_kpis table
        logger.info("Creating gold_executive_kpis table...")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS warehouse.chainalytics.gold_executive_kpis (
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
            ) USING DELTA
            PARTITIONED BY (kpi_date)
        """)
        logger.info("✓ gold_executive_kpis table created successfully")
        
        # Verify tables were created
        logger.info("Verifying created gold tables...")
        tables = spark.sql("SHOW TABLES IN warehouse.chainalytics").collect()
        gold_tables = [row.tableName for row in tables if row.tableName.startswith('gold_')]
        
        expected_tables = ['gold_customer_dashboard', 'gold_daily_business_summary', 'gold_product_insights', 'gold_executive_kpis']
        created_tables = [table for table in expected_tables if table in gold_tables]
        
        if len(created_tables) == len(expected_tables):
            logger.info("✓ All gold tables verified in warehouse.chainalytics:")
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
    """Insert sample test data into gold tables"""
    try:
        logger.info("Inserting test data into gold tables...")
        
        # Insert test customer dashboard data
        logger.info("Inserting test customer dashboard data...")
        spark.sql("""
            INSERT INTO warehouse.chainalytics.gold_customer_dashboard VALUES 
            ('cust_001', 'John Smith', 25, 2500.00, 100.00, 'electronics', current_date(), 'premium', 5000.00, 0.2, current_timestamp()),
            ('cust_002', 'Jane Doe', 12, 850.00, 70.83, 'clothing', current_date(), 'standard', 1500.00, 0.4, current_timestamp())
        """)
        logger.info("✓ Test customer dashboard data inserted")
        
        # Insert test daily business summary data
        logger.info("Inserting test daily business summary data...")
        spark.sql("""
            INSERT INTO warehouse.chainalytics.gold_daily_business_summary VALUES 
            (current_date(), 15000.00, 85, 120, 15, 176.47, 'electronics', 0.92, 0.98, 0.95, current_timestamp()),
            (current_date() - interval '1' day, 14200.00, 78, 115, 12, 182.05, 'clothing', 0.88, 0.96, 0.92, current_timestamp())
        """)
        logger.info("✓ Test daily business summary data inserted")
        
        # Insert test product insights data
        logger.info("Inserting test product insights data...")
        spark.sql("""
            INSERT INTO warehouse.chainalytics.gold_product_insights VALUES 
            (101, 'Premium Widget', 'electronics', 1, 3, 4.5, 'in_stock', 0.85, 'increase_marketing', current_timestamp()),
            (102, 'Standard Tool', 'tools', 5, 2, 4.2, 'low_stock', 0.75, 'reorder_inventory', current_timestamp())
        """)
        logger.info("✓ Test product insights data inserted")
        
        # Insert test executive KPIs data
        logger.info("Inserting test executive KPIs data...")
        spark.sql("""
            INSERT INTO warehouse.chainalytics.gold_executive_kpis VALUES 
            (current_date(), 0.15, 0.08, 0.85, 2500.00, 0.32, 0.88, 4.2, 0.12, current_timestamp()),
            (current_date() - interval '1' day, 0.12, 0.06, 0.82, 2450.00, 0.31, 0.86, 4.1, 0.11, current_timestamp())
        """)
        logger.info("✓ Test executive KPIs data inserted")
        
        # Verify data insertion
        customer_count = spark.sql("SELECT COUNT(*) as count FROM warehouse.chainalytics.gold_customer_dashboard").collect()[0]['count']
        business_count = spark.sql("SELECT COUNT(*) as count FROM warehouse.chainalytics.gold_daily_business_summary").collect()[0]['count']
        product_count = spark.sql("SELECT COUNT(*) as count FROM warehouse.chainalytics.gold_product_insights").collect()[0]['count']
        kpi_count = spark.sql("SELECT COUNT(*) as count FROM warehouse.chainalytics.gold_executive_kpis").collect()[0]['count']
        
        logger.info("✓ Data verification:")
        logger.info(f"  - Customer Dashboard: {customer_count} records")
        logger.info(f"  - Daily Business Summary: {business_count} records")
        logger.info(f"  - Product Insights: {product_count} records")
        logger.info(f"  - Executive KPIs: {kpi_count} records")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ Failed to insert test data: {str(e)}")
        raise

def main():
    """Main execution function"""
    logger.info("=" * 60)
    logger.info("Starting Gold Tables Creation Script")
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
        
        logger.info("=" * 60)
        logger.info("🎉 SUCCESS: Gold tables created in warehouse.chainalytics!")
        logger.info("Tables: gold_customer_dashboard, gold_daily_business_summary, gold_product_insights, gold_executive_kpis")
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