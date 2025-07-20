#!/usr/bin/env python3
"""
Create Silver layer tables for ChainAnalytics project
Silver layer: Cleaned, validated, and standardized data
Location: jobs/setup/create_silver_tables.py
"""

import sys
sys.path.append('/opt/spark/jobs/utils')

from spark_config import spark_config
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_silver_tables():
    """Create all silver layer tables with ingestion timestamps"""
    
    logger.info("Starting silver tables creation")
    
    try:
        spark = spark_config.create_spark_session("SilverTableSetup")
        
        # Silver Product Performance
        spark.sql("""
            CREATE TABLE IF NOT EXISTS iceberg_catalog.chainalytics.silver_product_performance (
                product_key STRING,
                product_name STRING,
                price_tier STRING,
                category_std STRING,
                popularity_score DOUBLE,
                quality_rating STRING,
                last_updated DATE,
                ingestion_timestamp TIMESTAMP
            )
            USING ICEBERG
            PARTITIONED BY (category_std, price_tier)
            TBLPROPERTIES (
                'write.format.default' = 'parquet',
                'write.upsert.enabled' = 'true'
            )
        """)
        logger.info("✅ Created silver_product_performance")
        
        # Silver User Sessions
        spark.sql("""
            CREATE TABLE IF NOT EXISTS iceberg_catalog.chainalytics.silver_user_sessions (
                session_id STRING,
                user_id STRING,
                session_duration_min INTEGER,
                total_events INTEGER,
                conversion_flag BOOLEAN,
                session_start TIMESTAMP,
                ingestion_timestamp TIMESTAMP
            )
            USING ICEBERG
            PARTITIONED BY (date(session_start), conversion_flag)
            TBLPROPERTIES (
                'write.format.default' = 'parquet',
                'write.upsert.enabled' = 'true'
            )
        """)
        logger.info("✅ Created silver_user_sessions")
        
        # Silver Logistics Impact
        spark.sql("""
            CREATE TABLE IF NOT EXISTS iceberg_catalog.chainalytics.silver_logistics_impact (
                impact_id STRING,
                location STRING,
                weather_risk_score INTEGER,
                delay_hours INTEGER,
                impact_timestamp TIMESTAMP,
                ingestion_timestamp TIMESTAMP
            )
            USING ICEBERG
            PARTITIONED BY (location, weather_risk_score)
            TBLPROPERTIES (
                'write.format.default' = 'parquet',
                'write.upsert.enabled' = 'true'
            )
        """)
        logger.info("✅ Created silver_logistics_impact")
        
        # Silver Customer Behavior
        spark.sql("""
            CREATE TABLE IF NOT EXISTS iceberg_catalog.chainalytics.silver_customer_behavior (
                customer_id STRING,
                engagement_level STRING,
                avg_session_min INTEGER,
                purchase_frequency INTEGER,
                preferred_category STRING,
                first_activity DATE,
                ingestion_timestamp TIMESTAMP
            )
            USING ICEBERG
            PARTITIONED BY (engagement_level, preferred_category)
            TBLPROPERTIES (
                'write.format.default' = 'parquet',
                'write.upsert.enabled' = 'true'
            )
        """)
        logger.info("✅ Created silver_customer_behavior")
        
        # Silver System Health
        spark.sql("""
            CREATE TABLE IF NOT EXISTS iceberg_catalog.chainalytics.silver_system_health (
                health_id STRING,
                api_availability DOUBLE,
                avg_response_time INTEGER,
                measurement_time TIMESTAMP,
                ingestion_timestamp TIMESTAMP
            )
            USING ICEBERG
            PARTITIONED BY (date(measurement_time))
            TBLPROPERTIES (
                'write.format.default' = 'parquet',
                'write.upsert.enabled' = 'true'
            )
        """)
        logger.info("✅ Created silver_system_health")
        
        # Show all tables
        logger.info("📊 All Silver tables:")
        spark.sql("SHOW TABLES IN iceberg_catalog.chainalytics").filter("tableName LIKE 'silver%'").show()
        
        logger.info("Silver layer tables created successfully with ingestion timestamps!")
        
    except Exception as e:
        logger.error(f"Error creating silver tables: {str(e)}")
        raise
    finally:
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    create_silver_tables()