#!/usr/bin/env python3
"""
Create Bronze layer tables for ChainAnalytics project
Initialize all bronze tables with proper Iceberg configuration
Location: jobs/setup/create_bronze_tables.py
"""

import sys
import os

# Add paths for imports
sys.path.append('/opt/spark/jobs/utils')

from spark_config import spark_config
from iceberg_utils import IcebergTableManager
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_all_bronze_tables():
    """Create all bronze layer tables"""
    
    logger.info("Starting bronze tables creation")
    
    try:
        spark = spark_config.create_spark_session("BronzeTableSetup")
        iceberg_manager = IcebergTableManager(spark)
        
        # Create database first
        iceberg_manager.create_database()
        logger.info("Created/verified database: iceberg_catalog.chainalytics")
        
        # Create all bronze tables
        tables = iceberg_manager.create_all_bronze_tables()
        
        # Verify tables were created
        for table_type, table_name in tables.items():
            try:
                df = spark.table(table_name)
                logger.info(f"✅ {table_type}: {table_name} - Schema: {len(df.columns)} columns")
            except Exception as e:
                logger.error(f"❌ {table_type}: {table_name} - Error: {str(e)}")
        
        logger.info("Bronze tables creation completed successfully")
        
        # Show database tables
        spark.sql("SHOW TABLES IN iceberg_catalog.chainalytics").show()
        
    except Exception as e:
        logger.error(f"Error creating bronze tables: {str(e)}")
        raise
    finally:
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    create_all_bronze_tables()