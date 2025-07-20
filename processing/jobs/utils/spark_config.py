#!/usr/bin/env python3
"""
Spark Configuration Utility
Centralized Spark session configuration for ChainAnalytics
Location: jobs/utils/spark_config.py
"""

from pyspark.sql import SparkSession
import os

class SparkConfig:
    """Centralized Spark configuration management"""
    
    def __init__(self):
        self.app_name = "ChainAnalytics"
        # Fixed to match Docker container names
        self.master = os.getenv('SPARK_MASTER', 'spark://chainalytics-spark-master:7077')
        self.minio_endpoint = os.getenv('MINIO_ENDPOINT', 'http://chainalytics-minio:9000')
        self.minio_access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
        self.minio_secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
    
    def create_spark_session(self, app_name=None):
        """Create Spark session with Iceberg and MinIO configuration"""
        
        if app_name:
            session_name = f"{self.app_name}_{app_name}"
        else:
            session_name = self.app_name
            
        spark = SparkSession.builder \
            .appName(session_name) \
            .master(self.master) \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.iceberg_catalog.type", "hadoop") \
            .config("spark.sql.catalog.iceberg_catalog.warehouse", "s3a://chainalytics/warehouse/") \
            .config("spark.hadoop.fs.s3a.endpoint", self.minio_endpoint) \
            .config("spark.hadoop.fs.s3a.access.key", self.minio_access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", self.minio_secret_key) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
            
        # Set log level to reduce noise
        spark.sparkContext.setLogLevel("WARN")
        
        return spark

# Global instance for easy import
spark_config = SparkConfig()