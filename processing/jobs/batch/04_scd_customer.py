#!/usr/bin/env python3
"""
SCD Type 2 Implementation for Customer Dimension
Simple approach: Track customer changes over time
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

def scd_customer():
    """Implement SCD Type 2 for customer dimension"""
    
    logger.info("Starting SCD Type 2 customer processing")
    
    try:
        spark = spark_config.create_spark_session()
        
        # Simulate customer changes over time
        current_customers = [
            ("user_123", "John Smith", "Premium", "High Spender", "North America", "Active"),
            ("user_456", "Jane Doe", "Standard", "Medium Spender", "Europe", "Active"),
            ("user_789", "Bob Johnson", "Premium", "High Spender", "Asia", "Active"),
            ("user_321", "Alice Brown", "Basic", "Low Spender", "North America", "Churned"),
            ("user_654", "Charlie Wilson", "Standard", "Medium Spender", "Europe", "Active")
        ]
        
        customer_schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("customer_name", StringType(), True),
            StructField("customer_segment", StringType(), True),
            StructField("purchase_behavior", StringType(), True),
            StructField("geographic_region", StringType(), True),
            StructField("lifecycle_stage", StringType(), True)
        ])
        
        df_current = spark.createDataFrame(current_customers, customer_schema)
        
        # Check if customer dimension exists
        try:
            df_existing = spark.table("iceberg_catalog.chainalytics.dim_customer")
            existing_count = df_existing.count()
            logger.info(f"Found existing customer dimension with {existing_count} records")
        except:
            logger.info("Customer dimension doesn't exist, creating initial load")
            df_existing = None
        
        if df_existing is None:
            # Initial load - create first version of all customers
            df_initial = df_current \
                .withColumn("customer_key", monotonically_increasing_id()) \
                .withColumn("valid_from", current_timestamp()) \
                .withColumn("valid_to", lit(None).cast("timestamp")) \
                .withColumn("ingestion_timestamp", current_timestamp())
            
            df_initial.writeTo("iceberg_catalog.chainalytics.dim_customer").createOrReplace()
            logger.info(f"✅ Created initial customer dimension with {df_initial.count()} records")
        
        else:
            # SCD Type 2 logic - detect changes and create new versions
            df_changes = df_current \
                .withColumn("customer_key", monotonically_increasing_id() + 1000) \
                .withColumn("valid_from", current_timestamp()) \
                .withColumn("valid_to", lit(None).cast("timestamp")) \
                .withColumn("ingestion_timestamp", current_timestamp())
            
            # Close previous versions (simplified - in real scenario you'd do proper change detection)
            df_updated_existing = df_existing \
                .withColumn("valid_to", 
                    when(col("valid_to").isNull(), 
                         current_timestamp() - expr("INTERVAL 1 SECOND"))
                    .otherwise(col("valid_to")))
            
            # Append new versions
            df_final = df_updated_existing.union(df_changes)
            
            df_final.writeTo("iceberg_catalog.chainalytics.dim_customer").createOrReplace()
            logger.info(f"✅ Updated customer dimension with SCD Type 2 logic")
        
        # Verify the result
        final_count = spark.table("iceberg_catalog.chainalytics.dim_customer").count()
        logger.info(f"📊 Customer dimension now has {final_count} total records")
        
        logger.info("SCD Type 2 processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error in SCD processing: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    scd_customer()