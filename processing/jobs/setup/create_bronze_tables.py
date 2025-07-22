#!/usr/bin/env python3
"""
Simplified Bronze Table Creation Script
Creates tables directly in warehouse.chainalytics database
"""

import logging
import sys
from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create Spark session configured for MinIO storage"""
    try:
        logger.info("Creating Spark session with MinIO configuration...")
        spark = SparkSession.builder \
            .appName("Bronze-Tables-MinIO") \
            .config("spark.sql.warehouse.dir", "s3a://warehouse/") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://chainalytics-minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .getOrCreate()
        
        logger.info("✓ Spark session created with MinIO configuration")
        return spark
    except Exception as e:
        logger.error(f"✗ Failed to create Spark session: {str(e)}")
        raise

def verify_catalog_and_database(spark):
    """Verify that spark_catalog and chainalytics database exist"""
    try:
        logger.info("Checking if spark_catalog exists...")
        catalogs = [row.catalog for row in spark.sql("SHOW CATALOGS").collect()]
        if 'spark_catalog' not in catalogs:
            error_msg = "Catalog 'spark_catalog' does not exist"
            logger.error(f"✗ {error_msg}")
            logger.error(f"Available catalogs: {catalogs}")
            raise Exception(error_msg)
        
        logger.info("✓ Spark catalog found")
        
        logger.info("Creating chainalytics database if it doesn't exist...")
        spark.sql("CREATE DATABASE IF NOT EXISTS chainalytics")
        logger.info("✓ Chainalytics database created/verified")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ Catalog/Database verification failed: {str(e)}")
        raise

def create_bronze_tables(spark):
    """Create bronze layer tables in chainalytics database"""
    try:
        logger.info("Starting bronze table creation...")
        
        # Create transactions table
        logger.info("Creating transactions table...")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS chainalytics.transactions (
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
            ) USING PARQUET
            PARTITIONED BY (created_date)
        """)
        logger.info("✓ Transactions table created successfully")
        
        # Create blocks table
        logger.info("Creating blocks table...")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS chainalytics.blocks (
                block_number BIGINT,
                block_hash STRING,
                parent_hash STRING,
                timestamp TIMESTAMP,
                transaction_count BIGINT,
                created_date DATE,
                ingestion_timestamp TIMESTAMP
            ) USING PARQUET
            PARTITIONED BY (created_date)
        """)
        logger.info("✓ Blocks table created successfully")
        
        # Verify tables were created
        logger.info("Verifying created tables...")
        tables = spark.sql("SHOW TABLES IN chainalytics").collect()
        table_names = [row.tableName for row in tables]
        
        if 'transactions' in table_names and 'blocks' in table_names:
            logger.info("✓ Both tables verified in chainalytics:")
            for table in tables:
                logger.info(f"  - {table.tableName}")
            return True
        else:
            logger.error(f"✗ Table verification failed. Found tables: {table_names}")
            return False
        
    except Exception as e:
        logger.error(f"✗ Failed to create bronze tables: {str(e)}")
        raise

def insert_test_data(spark):
    """Insert sample test data"""
    try:
        logger.info("Inserting test data...")
        
        # Insert test transaction
        logger.info("Inserting test transaction...")
        spark.sql("""
            INSERT INTO chainalytics.transactions VALUES 
            (1, 'test_hash_001', '0xabc123', '0xdef456', '1000000000000000000', 12345678, 21000, 20000000000, 
             current_date(), current_timestamp())
        """)
        logger.info("✓ Test transaction inserted")
        
        # Insert test block
        logger.info("Inserting test block...")
        spark.sql("""
            INSERT INTO chainalytics.blocks VALUES 
            (12345678, 'block_hash_001', 'parent_hash_001', current_timestamp(), 150, 
             current_date(), current_timestamp())
        """)
        logger.info("✓ Test block inserted")
        
        # Verify data insertion
        tx_count = spark.sql("SELECT COUNT(*) as count FROM chainalytics.transactions").collect()[0]['count']
        block_count = spark.sql("SELECT COUNT(*) as count FROM chainalytics.blocks").collect()[0]['count']
        
        logger.info(f"✓ Data verification - Transactions: {tx_count}, Blocks: {block_count}")
        return True
        
    except Exception as e:
        logger.error(f"✗ Failed to insert test data: {str(e)}")
        raise

def read_and_display_data(spark):
    """Read and display sample data from tables"""
    try:
        logger.info("Reading data from tables...")
        
        # Read transactions data
        logger.info("📖 Reading transactions table...")
        transactions_df = spark.sql("SELECT * FROM chainalytics.transactions LIMIT 5")
        transactions_data = transactions_df.collect()
        
        if transactions_data:
            logger.info("✓ Transactions table data:")
            logger.info("-" * 80)
            for i, row in enumerate(transactions_data, 1):
                logger.info(f"  Transaction {i}:")
                logger.info(f"    ID: {row.id}")
                logger.info(f"    Hash: {row.hash}")
                logger.info(f"    From: {row.from_address}")
                logger.info(f"    To: {row.to_address}")
                logger.info(f"    Value: {row.value}")
                logger.info(f"    Block Number: {row.block_number}")
                logger.info(f"    Gas Used: {row.gas_used}")
                logger.info(f"    Gas Price: {row.gas_price}")
                logger.info(f"    Created Date: {row.created_date}")
                logger.info(f"    Ingestion Time: {row.ingestion_timestamp}")
                logger.info("-" * 40)
        else:
            logger.info("⚠️ No data found in transactions table")
        
        # Read blocks data
        logger.info("📖 Reading blocks table...")
        blocks_df = spark.sql("SELECT * FROM chainalytics.blocks LIMIT 5")
        blocks_data = blocks_df.collect()
        
        if blocks_data:
            logger.info("✓ Blocks table data:")
            logger.info("-" * 80)
            for i, row in enumerate(blocks_data, 1):
                logger.info(f"  Block {i}:")
                logger.info(f"    Block Number: {row.block_number}")
                logger.info(f"    Block Hash: {row.block_hash}")
                logger.info(f"    Parent Hash: {row.parent_hash}")
                logger.info(f"    Timestamp: {row.timestamp}")
                logger.info(f"    Transaction Count: {row.transaction_count}")
                logger.info(f"    Created Date: {row.created_date}")
                logger.info(f"    Ingestion Time: {row.ingestion_timestamp}")
                logger.info("-" * 40)
        else:
            logger.info("⚠️ No data found in blocks table")
        
        # Show table schemas
        logger.info("📋 Table Schemas:")
        logger.info("Transactions table schema:")
        transactions_df.printSchema()
        
        logger.info("Blocks table schema:")
        blocks_df.printSchema()
        
        # Show record counts
        tx_total = spark.sql("SELECT COUNT(*) as count FROM chainalytics.transactions").collect()[0]['count']
        block_total = spark.sql("SELECT COUNT(*) as count FROM chainalytics.blocks").collect()[0]['count']
        
        logger.info("📊 Final Record Counts:")
        logger.info(f"  - Transactions: {tx_total} records")
        logger.info(f"  - Blocks: {block_total} records")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ Failed to read data from tables: {str(e)}")
        # raise COUNT(*) as count FROM warehouse.chainalytics.transactions").collect()[0]['count']
        block_total = spark.sql("SELECT COUNT(*) as count FROM warehouse.chainalytics.blocks").collect()[0]['count']
        
        logger.info("📊 Final Record Counts:")
        logger.info(f"  - Transactions: {tx_total} records")
        logger.info(f"  - Blocks: {block_total} records")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ Failed to read data from tables: {str(e)}")
        raise

def main():
    """Main execution function"""
    logger.info("=" * 60)
    logger.info("Starting Bronze Table Creation Script")
    logger.info("=" * 60)
    
    spark = None
    try:
        # Create Spark session
        spark = create_spark_session()
        
        # Verify catalog and database exist
        verify_catalog_and_database(spark)
        
        # Create tables
        create_bronze_tables(spark)
        
        # Insert test data
        insert_test_data(spark)
        
        # Read and display data from tables
        read_and_display_data(spark)
        
        logger.info("=" * 60)
        logger.info("🎉 SUCCESS: Bronze tables created in chainalytics database!")
        logger.info("Tables stored in MinIO bucket: s3a://warehouse/chainalytics/")
        logger.info("Tables: transactions, blocks")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error("=" * 60)
        logger.error(f"💥 SCRIPT FAILED: {str(e)}")
        logger.error("=" * 60)
        sys.exit(1)
        
    finally:
        if spark:
            logger.info("Stopping Spark session...")
            spark.stop()
            logger.info("✓ Spark session stopped")

if __name__ == "__main__":
    main()