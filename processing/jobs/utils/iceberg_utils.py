#!/usr/bin/env python3
"""
Iceberg Table Management Utilities
Location: jobs/utils/iceberg_utils.py
"""

from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp
import logging

logger = logging.getLogger(__name__)

class IcebergTableManager:
    """Utility class for managing Iceberg tables"""
    
    def __init__(self, spark):
        self.spark = spark
        self.catalog = "iceberg_catalog"
        self.database = "chainalytics"
    
    def create_database(self):
        """Create the main database if it doesn't exist"""
        try:
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.catalog}.{self.database}")
            logger.info(f"Created/verified database: {self.catalog}.{self.database}")
        except Exception as e:
            logger.error(f"Error creating database: {str(e)}")
            raise
    
    def create_all_bronze_tables(self):
        """Create all bronze layer tables"""
        
        tables = {}
        
        # Bronze Product Performance
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.catalog}.{self.database}.bronze_product_performance (
                product_id STRING,
                product_name STRING,
                category STRING,
                rating DOUBLE,
                price DOUBLE,
                stock_quantity INTEGER,
                ingestion_timestamp TIMESTAMP,
                source_system STRING,
                data_quality_score DOUBLE
            )
            USING ICEBERG
            PARTITIONED BY (category)
            TBLPROPERTIES (
                'write.format.default' = 'parquet'
            )
        """)
        tables['bronze_product_performance'] = f"{self.catalog}.{self.database}.bronze_product_performance"
        
        # Bronze User Sessions - Fix the partitioning syntax
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.catalog}.{self.database}.bronze_user_sessions (
                session_id STRING,
                user_id STRING,
                duration_minutes INTEGER,
                page_views INTEGER,
                converted BOOLEAN,
                session_start TIMESTAMP,
                ingestion_timestamp TIMESTAMP,
                source_system STRING,
                session_quality STRING
            )
            USING ICEBERG
            PARTITIONED BY (days(session_start))
            TBLPROPERTIES (
                'write.format.default' = 'parquet'
            )
        """)
        tables['bronze_user_sessions'] = f"{self.catalog}.{self.database}.bronze_user_sessions"
        
        # Bronze Customer Behavior
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.catalog}.{self.database}.bronze_customer_behavior (
                user_id STRING,
                engagement_level STRING,
                avg_session_minutes INTEGER,
                monthly_purchases INTEGER,
                preferred_category STRING,
                first_activity TIMESTAMP,
                ingestion_timestamp TIMESTAMP,
                source_system STRING,
                profile_completeness DOUBLE
            )
            USING ICEBERG
            PARTITIONED BY (engagement_level)
            TBLPROPERTIES (
                'write.format.default' = 'parquet'
            )
        """)
        tables['bronze_customer_behavior'] = f"{self.catalog}.{self.database}.bronze_customer_behavior"
        
        # Bronze Streaming Events (for real-time data)
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.catalog}.{self.database}.bronze_streaming_events (
                event_id STRING,
                user_id STRING,
                event_type STRING,
                product_id STRING,
                session_id STRING,
                event_timestamp TIMESTAMP,
                value DOUBLE,
                processing_timestamp TIMESTAMP,
                ingestion_timestamp TIMESTAMP,
                source_system STRING
            )
            USING ICEBERG
            PARTITIONED BY (days(event_timestamp))
            TBLPROPERTIES (
                'write.format.default' = 'parquet'
            )
        """)
        tables['bronze_streaming_events'] = f"{self.catalog}.{self.database}.bronze_streaming_events"
        
        # Bronze Streaming Events Late (for late data)
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.catalog}.{self.database}.bronze_streaming_events_late (
                event_id STRING,
                user_id STRING,
                event_type STRING,
                product_id STRING,
                session_id STRING,
                event_timestamp TIMESTAMP,
                value DOUBLE,
                processing_timestamp TIMESTAMP,
                ingestion_timestamp TIMESTAMP,
                arrival_delay_hours DOUBLE,
                late_data_category STRING,
                requires_reprocessing BOOLEAN
            )
            USING ICEBERG
            PARTITIONED BY (days(event_timestamp))
            TBLPROPERTIES (
                'write.format.default' = 'parquet'
            )
        """)
        tables['bronze_streaming_events_late'] = f"{self.catalog}.{self.database}.bronze_streaming_events_late"
        
        logger.info(f"Created {len(tables)} bronze tables")
        return tables
    
    def table_exists(self, table_name):
        """Check if a table exists"""
        try:
            self.spark.table(f"{self.catalog}.{self.database}.{table_name}")
            return True
        except:
            return False
    
    def get_table_count(self, table_name):
        """Get row count for a table"""
        try:
            return self.spark.table(f"{self.catalog}.{self.database}.{table_name}").count()
        except:
            return -1