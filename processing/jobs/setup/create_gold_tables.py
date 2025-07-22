#!/usr/bin/env python3
"""
Simple Gold Tables Creation for ChainAnalytics
Creates gold tables in 'warehouse' catalog
Location: jobs/setup/create_gold_tables_simple.py
"""

from pyspark.sql import SparkSession
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create Spark session with Iceberg configuration"""
    return SparkSession.builder \
        .appName("GoldTablesCreation") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
        .config("spark.sql.catalog.spark_catalog.warehouse", "s3a://warehouse/spark-warehouse/") \
        .config("spark.sql.catalog.spark_catalog.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.sql.catalog.spark_catalog.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.sql.catalog.spark_catalog.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.sql.catalog.spark_catalog.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.sql.catalog.spark_catalog.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def create_gold_tables():
    """Create all gold layer tables"""
    
    logger.info("🚀 Starting gold tables creation")
    
    spark = create_spark_session()
    
    try:
        # Create database in the default catalog
        # spark.sql("CREATE DATABASE IF NOT EXISTS chainalytics")
        spark.sql("USE chainalytics")
        logger.info("✅ Using database: chainalytics")
        
        # 1. Gold Customer Dashboard
        spark.sql("""
            CREATE TABLE IF NOT EXISTS gold_customer_dashboard (
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
            )
            USING ICEBERG
            PARTITIONED BY (customer_tier)
            TBLPROPERTIES (
                'write.format.default' = 'parquet'
            )
        """)
        logger.info("✅ Created table: chainalytics.gold_customer_dashboard")
        
        # 2. Gold Daily Business Summary
        spark.sql("""
            CREATE TABLE IF NOT EXISTS gold_daily_business_summary (
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
            )
            USING ICEBERG
            PARTITIONED BY (business_date)
            TBLPROPERTIES (
                'write.format.default' = 'parquet'
            )
        """)
        logger.info("✅ Created table: chainalytics.gold_daily_business_summary")
        
        # 3. Gold Product Insights
        spark.sql("""
            CREATE TABLE IF NOT EXISTS gold_product_insights (
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
            )
            USING ICEBERG
            PARTITIONED BY (category)
            TBLPROPERTIES (
                'write.format.default' = 'parquet'
            )
        """)
        logger.info("✅ Created table: chainalytics.gold_product_insights")
        
        # 4. Gold Executive KPIs
        spark.sql("""
            CREATE TABLE IF NOT EXISTS gold_executive_kpis (
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
            )
            USING ICEBERG
            PARTITIONED BY (kpi_date)
            TBLPROPERTIES (
                'write.format.default' = 'parquet'
            )
        """)
        logger.info("✅ Created table: chainalytics.gold_executive_kpis")
        
        # Show all tables created
        logger.info("📊 All Gold tables created:")
        spark.sql("SHOW TABLES").show()
        
        logger.info("✅ Gold tables creation completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Error creating gold tables: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    create_gold_tables()