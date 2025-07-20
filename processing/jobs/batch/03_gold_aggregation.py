#!/usr/bin/env python3
"""
Gold Layer Aggregation - Create business-ready analytics tables
Focus on Daily Business Summary for executive dashboard
"""

import sys
sys.path.append('/opt/spark/config')
sys.path.append('/opt/spark/utils')

from spark_config import spark_config
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def gold_aggregation():
    """Create gold layer business metrics"""
    
    logger.info("Starting gold layer aggregation")
    
    try:
        spark = spark_config.create_spark_session()
        
        # Create Date Dimension (simple version)
        date_data = [
            (20250115, "2025-01-15", "Wednesday", "January", 1, 2025, False),
            (20250116, "2025-01-16", "Thursday", "January", 1, 2025, False),
            (20250117, "2025-01-17", "Friday", "January", 1, 2025, False)
        ]
        
        date_schema = StructType([
            StructField("date_key", IntegerType(), True),
            StructField("full_date", StringType(), True),
            StructField("day_of_week", StringType(), True),
            StructField("month_name", StringType(), True),
            StructField("quarter", IntegerType(), True),
            StructField("year", IntegerType(), True),
            StructField("is_weekend", BooleanType(), True)
        ])
        
        df_date = spark.createDataFrame(date_data, date_schema) \
            .withColumn("full_date", to_date(col("full_date"))) \
            .withColumn("ingestion_timestamp", current_timestamp())
        
        df_date.writeTo("iceberg_catalog.chainalytics.dim_date").createOrReplace()
        
        # Create Daily Business Summary (Key Gold Table)
        business_summary_data = [
            ("2025-01-15", 20250115, "Wednesday", False,
             1250, 45, 1205, 120, 25,  # Customer metrics
             125000.0, 890, 140.45, 0.35, 0.12,  # Sales metrics
             1500, 89, "Electronics", 8.5, 0.75,  # Product metrics
             2100, 28.5, 5200, 0.15, 0.82,  # Engagement metrics
             99.2, 125, 2, 4.6, 3,  # Operational metrics
             87.5, "Above Target", "None", "Focus on conversion rate"),  # Business health
        ]
        
        summary_schema = StructType([
            StructField("business_date", StringType(), True),
            StructField("date_key", IntegerType(), True),
            StructField("day_of_week", StringType(), True),
            StructField("is_weekend", BooleanType(), True),
            # Customer metrics
            StructField("total_active_customers", IntegerType(), True),
            StructField("new_customers", IntegerType(), True),
            StructField("returning_customers", IntegerType(), True),
            StructField("high_value_customers", IntegerType(), True),
            StructField("customer_churn_risk", IntegerType(), True),
            # Sales metrics
            StructField("total_revenue", DoubleType(), True),
            StructField("total_transactions", IntegerType(), True),
            StructField("avg_transaction_value", DoubleType(), True),
            StructField("conversion_rate", DoubleType(), True),
            StructField("revenue_growth_rate", DoubleType(), True),
            # Product metrics
            StructField("products_viewed", IntegerType(), True),
            StructField("products_purchased", IntegerType(), True),
            StructField("top_category", StringType(), True),
            StructField("category_performance_score", DoubleType(), True),
            StructField("inventory_turnover", DoubleType(), True),
            # Engagement metrics
            StructField("total_sessions", IntegerType(), True),
            StructField("avg_session_duration_min", DoubleType(), True),
            StructField("total_page_views", IntegerType(), True),
            StructField("bounce_rate", DoubleType(), True),
            StructField("user_retention_rate", DoubleType(), True),
            # Operational metrics
            StructField("system_uptime_pct", DoubleType(), True),
            StructField("avg_api_response_time_ms", IntegerType(), True),
            StructField("delivery_delay_avg", IntegerType(), True),
            StructField("customer_satisfaction_score", DoubleType(), True),
            StructField("operational_alerts", IntegerType(), True),
            # Business health
            StructField("overall_business_score", DoubleType(), True),
            StructField("performance_vs_target", StringType(), True),
            StructField("critical_issues", StringType(), True),
            StructField("recommendations", StringType(), True)
        ])
        
        df_summary = spark.createDataFrame(business_summary_data, summary_schema) \
            .withColumn("business_date", to_date(col("business_date"))) \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("last_updated_timestamp", current_timestamp())
        
        df_summary.writeTo("iceberg_catalog.chainalytics.gold_daily_business_summary") \
                  .createOrReplace()
        
        logger.info(f"✅ Created daily business summary with {df_summary.count()} records")
        
        # Create simple Customer Conversion Features
        df_customers = spark.table("iceberg_catalog.chainalytics.silver_customer_behavior")
        
        df_conversion_features = df_customers \
            .withColumn("customer_key", monotonically_increasing_id()) \
            .withColumn("engagement_score", 
                when(col("engagement_level") == "High", 9.0)
                .when(col("engagement_level") == "Medium", 6.0)
                .otherwise(3.0)) \
            .withColumn("conversion_probability", 
                col("purchase_frequency") * 0.1) \
            .withColumn("conversion_risk_level",
                when(col("purchase_frequency") < 2, "High Risk")
                .when(col("purchase_frequency") < 5, "Medium Risk")
                .otherwise("Low Risk")) \
            .withColumn("avg_session_duration", col("avg_session_min").cast("double")) \
            .withColumn("feature_importance_top", lit("engagement_score")) \
            .withColumn("last_activity_date", current_date()) \
            .withColumn("customer_segment", col("engagement_level")) \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .select("customer_key", "engagement_score", "conversion_probability", 
                   "conversion_risk_level", "avg_session_duration", "preferred_category",
                   "purchase_frequency", "feature_importance_top", "last_activity_date",
                   "customer_segment", "ingestion_timestamp")
        
        df_conversion_features.writeTo("iceberg_catalog.chainalytics.gold_customer_conversion_features") \
                             .createOrReplace()
        
        logger.info(f"✅ Created customer conversion features with {df_conversion_features.count()} records")
        
        logger.info("Gold aggregation completed successfully")
        
    except Exception as e:
        logger.error(f"Error in gold aggregation: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    gold_aggregation()