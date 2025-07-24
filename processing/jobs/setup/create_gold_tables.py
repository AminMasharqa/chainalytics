#!/usr/bin/env python3
"""
Create Iceberg Gold Tables in MinIO: warehouse.catalog -> "chainalytics" database
"""

import logging
import sys
from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session():
    try:
        logger.info("Starting Spark session with Iceberg + MinIO...")
        spark = SparkSession.builder \
            .appName("Iceberg-Gold-Tables") \
            .config("spark.sql.catalog.warehouse", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.warehouse.type", "hadoop") \
            .config("spark.sql.catalog.warehouse.warehouse", "s3a://warehouse/") \
            .config("spark.sql.catalog.warehouse.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://chainalytics-minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .getOrCreate()
        logger.info("✓ Spark session created.")
        return spark
    except Exception as e:
        logger.error(f"✗ Spark session creation failed: {e}")
        raise


def create_iceberg_tables(spark):
    """Create gold tables with explicit locations using silver table technique"""
    try:
        logger.info("Creating Iceberg gold tables...")

        tables = [
            {
                "name": "gold_customer_analytics",
                "schema": """(
                    customer_id STRING,
                    customer_name STRING,
                    total_orders INT,
                    total_spent DOUBLE,
                    avg_order_value DOUBLE,
                    favorite_category STRING,
                    last_order_date DATE,
                    customer_tier STRING,
                    lifetime_value DOUBLE,
                    churn_risk_score DOUBLE,
                    ingestion_timestamp TIMESTAMP
                )"""
            },
            {
                "name": "gold_daily_summary",
                "schema": """(
                    business_date DATE,
                    total_revenue DOUBLE,
                    total_orders INT,
                    active_customers INT,
                    new_customers INT,
                    avg_order_value DOUBLE,
                    top_selling_category STRING,
                    weather_impact_score DOUBLE,
                    api_performance_score DOUBLE,
                    overall_health_score DOUBLE,
                    ingestion_timestamp TIMESTAMP
                )"""
            },
            {
                "name": "gold_product_performance",
                "schema": """(
                    product_id INT,
                    product_name STRING,
                    category STRING,
                    revenue_rank INT,
                    conversion_rank INT,
                    customer_satisfaction DOUBLE,
                    inventory_status STRING,
                    price_optimization_score DOUBLE,
                    recommended_action STRING,
                    ingestion_timestamp TIMESTAMP
                )"""
            },
            {
                "name": "gold_executive_dashboard",
                "schema": """(
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
                )"""
            },
            {
                "name": "gold_weather_correlation",
                "schema": """(
                    correlation_id STRING,
                    location_id STRING,
                    weather_condition STRING,
                    avg_temperature DOUBLE,
                    sales_impact_percentage DOUBLE,
                    optimal_weather_score DOUBLE,
                    business_date DATE,
                    ingestion_timestamp TIMESTAMP
                )"""
            }
        ]

        for table in tables:
            try:
                logger.info(f"Creating table: {table['name']}")
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
                
        logger.info("✓ All gold tables created successfully")
    except Exception as e:
        logger.error(f"✗ Failed to create gold tables: {e}")
        raise



def main():
    logger.info("=" * 60)
    logger.info("🧊 Iceberg Gold Table Creation Script")
    logger.info("=" * 60)

    spark = None
    try:
        spark = create_spark_session()
        create_iceberg_tables(spark)
        logger.info("✅ All gold tables created successfully in warehouse.catalog → `chainalytics`")
    except Exception as e:
        logger.error(f"💥 Script failed: {e}")
        sys.exit(1)
    finally:
        if spark:
            logger.info("Stopping Spark session...")
            spark.stop()
            logger.info("✓ Spark session stopped.")

if __name__ == "__main__":
    main()