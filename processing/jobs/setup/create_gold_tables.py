#!/usr/bin/env python3
"""
Create Gold layer tables for ChainAnalytics project
Gold layer: Star Schema & Dimensional Models - Executive Insights
Location: jobs/setup/create_gold_tables.py
"""

import sys
sys.path.append('/opt/spark/jobs/utils')

from spark_config import spark_config
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_gold_tables():
    """Create all gold layer tables with ingestion timestamps"""
    
    logger.info("Starting gold tables creation")
    
    try:
        spark = spark_config.create_spark_session("GoldTableSetup")
        
        # Date Dimension
        spark.sql("""
            CREATE TABLE IF NOT EXISTS iceberg_catalog.chainalytics.dim_date (
                date_key INTEGER,
                full_date DATE,
                day_of_week STRING,
                month_name STRING,
                quarter INTEGER,
                year INTEGER,
                is_weekend BOOLEAN,
                ingestion_timestamp TIMESTAMP
            )
            USING ICEBERG
            PARTITIONED BY (year, quarter)
            TBLPROPERTIES (
                'write.format.default' = 'parquet'
            )
        """)
        logger.info("✅ Created dim_date")
        
        # Product Dimension
        spark.sql("""
            CREATE TABLE IF NOT EXISTS iceberg_catalog.chainalytics.dim_product (
                product_key INTEGER,
                product_id STRING,
                product_name STRING,
                category STRING,
                price_tier STRING,
                ingestion_timestamp TIMESTAMP
            )
            USING ICEBERG
            PARTITIONED BY (category, price_tier)
            TBLPROPERTIES (
                'write.format.default' = 'parquet'
            )
        """)
        logger.info("✅ Created dim_product")
        
        # Customer Dimension (Type 2 SCD)
        spark.sql("""
            CREATE TABLE IF NOT EXISTS iceberg_catalog.chainalytics.dim_customer (
                customer_key INTEGER,
                customer_id STRING,
                customer_name STRING,
                customer_segment STRING,
                purchase_behavior STRING,
                geographic_region STRING,
                lifecycle_stage STRING,
                valid_from TIMESTAMP,
                valid_to TIMESTAMP,
                ingestion_timestamp TIMESTAMP
            )
            USING ICEBERG
            PARTITIONED BY (customer_segment, geographic_region)
            TBLPROPERTIES (
                'write.format.default' = 'parquet',
                'write.upsert.enabled' = 'true'
            )
        """)
        logger.info("✅ Created dim_customer (SCD Type 2)")
        
        # Customer Activity Fact Table
        spark.sql("""
            CREATE TABLE IF NOT EXISTS iceberg_catalog.chainalytics.fact_customer_activity (
                activity_key INTEGER,
                customer_key INTEGER,
                product_key INTEGER,
                date_key INTEGER,
                session_count INTEGER,
                total_revenue DOUBLE,
                ingestion_timestamp TIMESTAMP
            )
            USING ICEBERG
            PARTITIONED BY (date_key)
            TBLPROPERTIES (
                'write.format.default' = 'parquet'
            )
        """)
        logger.info("✅ Created fact_customer_activity")
        
        # Operational Metrics Fact Table
        spark.sql("""
            CREATE TABLE IF NOT EXISTS iceberg_catalog.chainalytics.fact_operational_metrics (
                metric_key INTEGER,
                date_key INTEGER,
                delivery_delay_avg INTEGER,
                system_uptime_pct DOUBLE,
                customer_satisfaction DOUBLE,
                ingestion_timestamp TIMESTAMP
            )
            USING ICEBERG
            PARTITIONED BY (date_key)
            TBLPROPERTIES (
                'write.format.default' = 'parquet'
            )
        """)
        logger.info("✅ Created fact_operational_metrics")
        
        # Customer Conversion Features (Advanced Analytics)
        spark.sql("""
            CREATE TABLE IF NOT EXISTS iceberg_catalog.chainalytics.gold_customer_conversion_features (
                customer_key INTEGER,
                engagement_score DOUBLE,
                conversion_probability DOUBLE,
                conversion_risk_level STRING,
                avg_session_duration DOUBLE,
                preferred_category STRING,
                purchase_frequency INTEGER,
                feature_importance_top STRING,
                last_activity_date DATE,
                customer_segment STRING,
                ingestion_timestamp TIMESTAMP
            )
            USING ICEBERG
            PARTITIONED BY (customer_segment, conversion_risk_level)
            TBLPROPERTIES (
                'write.format.default' = 'parquet',
                'write.upsert.enabled' = 'true'
            )
        """)
        logger.info("✅ Created gold_customer_conversion_features")
        
        # Daily Business Summary Table - Executive Dashboard
        spark.sql("""
            CREATE TABLE IF NOT EXISTS iceberg_catalog.chainalytics.gold_daily_business_summary (
                -- Date Information
                business_date DATE,
                date_key INTEGER,
                day_of_week STRING,
                is_weekend BOOLEAN,
                
                -- Customer Metrics
                total_active_customers INTEGER,
                new_customers INTEGER,
                returning_customers INTEGER,
                high_value_customers INTEGER,
                customer_churn_risk INTEGER,
                
                -- Sales & Revenue Metrics
                total_revenue DOUBLE,
                total_transactions INTEGER,
                avg_transaction_value DOUBLE,
                conversion_rate DOUBLE,
                revenue_growth_rate DOUBLE,
                
                -- Product Metrics
                products_viewed INTEGER,
                products_purchased INTEGER,
                top_category STRING,
                category_performance_score DOUBLE,
                inventory_turnover DOUBLE,
                
                -- Engagement Metrics
                total_sessions INTEGER,
                avg_session_duration_min DOUBLE,
                total_page_views INTEGER,
                bounce_rate DOUBLE,
                user_retention_rate DOUBLE,
                
                -- Operational Metrics
                system_uptime_pct DOUBLE,
                avg_api_response_time_ms INTEGER,
                delivery_delay_avg INTEGER,
                customer_satisfaction_score DOUBLE,
                operational_alerts INTEGER,
                
                -- Business Health Indicators
                overall_business_score DOUBLE,
                performance_vs_target STRING,
                critical_issues STRING,
                recommendations STRING,
                
                -- Metadata
                ingestion_timestamp TIMESTAMP,
                last_updated_timestamp TIMESTAMP
            )
            USING ICEBERG
            PARTITIONED BY (business_date)
            TBLPROPERTIES (
                'write.format.default' = 'parquet',
                'write.upsert.enabled' = 'true'
            )
        """)
        logger.info("✅ Created gold_daily_business_summary - Executive Dashboard")
        
        # Show all tables
        logger.info("📊 All Gold tables:")
        spark.sql("SHOW TABLES IN iceberg_catalog.chainalytics").filter("tableName LIKE 'gold%' OR tableName LIKE 'dim_%' OR tableName LIKE 'fact_%'").show()
        
        logger.info("✅ Gold layer tables created successfully with ingestion timestamps!")
        logger.info("🚀 Added Daily Business Summary table for executive insights!")
        
    except Exception as e:
        logger.error(f"Error creating gold tables: {str(e)}")
        raise
    finally:
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    create_gold_tables()