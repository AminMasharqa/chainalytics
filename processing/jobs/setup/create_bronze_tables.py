#!/usr/bin/env python3
"""
Bronze Table Creation Script - MinIO Compatible
This script creates tables that will be visible in MinIO UI browser
"""

import logging
import sys
from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session_minio():
    """Create Spark session configured for MinIO storage"""
    try:
        spark = SparkSession.builder \
            .appName("Bronze-Tables-MinIO") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.iceberg_catalog.type", "hadoop") \
            .config("spark.sql.catalog.iceberg_catalog.warehouse", "s3a://warehouse/") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://chainalytics-minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .getOrCreate()
        
        logger.info("Created Spark session with MinIO configuration")
        return spark
    except Exception as e:
        logger.error(f"Failed to create Spark session: {str(e)}")
        raise

def test_minio_connection(spark):
    """Test MinIO connectivity"""
    try:
        # Try to list buckets (this will test S3 connection)
        spark.sql("SHOW NAMESPACES IN iceberg_catalog").show()
        logger.info("MinIO connection test successful")
        return True
    except Exception as e:
        logger.warning(f"MinIO connection test failed: {str(e)}")
        return False

def create_bronze_tables(spark):
    """Create bronze layer tables that will be stored in MinIO"""
    try:
        # Create bronze namespace
        spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg_catalog.bronze")
        logger.info("Created namespace: iceberg_catalog.bronze")
        
        # Create transactions table
        spark.sql("""
            CREATE TABLE IF NOT EXISTS iceberg_catalog.bronze.transactions (
                id BIGINT,
                hash STRING,
                from_address STRING,
                to_address STRING,
                value STRING,
                block_number BIGINT,
                gas_used BIGINT,
                gas_price BIGINT,
                created_date DATE,
                ingestion_timestamp TIMESTAMP
            ) USING iceberg
            PARTITIONED BY (created_date)
            TBLPROPERTIES (
                'write.format.default'='parquet',
                'write.parquet.compression-codec'='snappy'
            )
        """)
        logger.info("Created table: iceberg_catalog.bronze.transactions")
        
        # Create blocks table
        spark.sql("""
            CREATE TABLE IF NOT EXISTS iceberg_catalog.bronze.blocks (
                block_number BIGINT,
                block_hash STRING,
                parent_hash STRING,
                timestamp TIMESTAMP,
                transaction_count BIGINT,
                created_date DATE,
                ingestion_timestamp TIMESTAMP
            ) USING iceberg
            PARTITIONED BY (created_date)
            TBLPROPERTIES (
                'write.format.default'='parquet',
                'write.parquet.compression-codec'='snappy'
            )
        """)
        logger.info("Created table: iceberg_catalog.bronze.blocks")
        
        # Verify tables
        tables = spark.sql("SHOW TABLES IN iceberg_catalog.bronze").collect()
        logger.info("Tables created in iceberg_catalog.bronze:")
        for table in tables:
            logger.info(f"  - {table.tableName}")
        
        # Insert some test data to make files visible in MinIO
        insert_test_data(spark)
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to create bronze tables: {str(e)}")
        return False

def insert_test_data(spark):
    """Insert test data to create actual files in MinIO"""
    try:
        # Insert test transaction
        spark.sql("""
            INSERT INTO iceberg_catalog.bronze.transactions VALUES 
            (1, 'test_hash_001', '0xabc123', '0xdef456', '1000000000000000000', 12345678, 21000, 20000000000, 
             current_date(), current_timestamp())
        """)
        
        # Insert test block
        spark.sql("""
            INSERT INTO iceberg_catalog.bronze.blocks VALUES 
            (12345678, 'block_hash_001', 'parent_hash_001', current_timestamp(), 150, 
             current_date(), current_timestamp())
        """)
        
        logger.info("Inserted test data - files should now be visible in MinIO")
        return True
        
    except Exception as e:
        logger.error(f"Failed to insert test data: {str(e)}")
        return False

def main():
    """Main execution function"""
    logger.info("Starting bronze table creation for MinIO storage...")
    
    try:
        # Create Spark session
        spark = create_spark_session_minio()
        
        # Test MinIO connection
        if not test_minio_connection(spark):
            logger.warning("MinIO connection test failed, but continuing...")
        
        # Create tables
        if create_bronze_tables(spark):
            logger.info("SUCCESS: Bronze tables created and stored in MinIO!")
            logger.info("Check MinIO UI at: http://localhost:9001")
            logger.info("Look for bucket 'warehouse' -> bronze folder")
        else:
            logger.error("Failed to create bronze tables")
            sys.exit(1)
            
        spark.stop()
        
    except Exception as e:
        logger.error(f"Script execution failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()