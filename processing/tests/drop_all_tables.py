#!/usr/bin/env python3
"""
Drop All Tables Script
Drops all tables in chainalytics.db database
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
            .appName("Drop-All-Tables-MinIO") \
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
    """Verify that spark_catalog and chainalytics.db database exist"""
    try:
        logger.info("Checking if spark_catalog exists...")
        catalogs = [row.catalog for row in spark.sql("SHOW CATALOGS").collect()]
        if 'spark_catalog' not in catalogs:
            error_msg = "Catalog 'spark_catalog' does not exist"
            logger.error(f"✗ {error_msg}")
            logger.error(f"Available catalogs: {catalogs}")
            raise Exception(error_msg)
        
        logger.info("✓ Spark catalog found")
        
        logger.info("Checking if chainalytics.db database exists...")
        databases = [row.namespace for row in spark.sql("SHOW DATABASES").collect()]
        if 'chainalytics.db' not in databases:
            logger.warning("⚠️ Database 'chainalytics.db' does not exist - nothing to drop")
            return False
        
        logger.info("✓ chainalytics.db database found")
        return True
        
    except Exception as e:
        logger.error(f"✗ Catalog/Database verification failed: {str(e)}")
        raise

def drop_all_tables(spark):
    """Drop all tables in chainalytics.db database"""
    try:
        logger.info("Starting to drop all tables in chainalytics.db database...")
        
        # Get list of all tables in chainalytics.db database
        logger.info("Getting list of tables in chainalytics.db database...")
        tables = spark.sql("SHOW TABLES IN chainalytics.db").collect()
        table_names = [row.tableName for row in tables]
        
        if not table_names:
            logger.info("✓ No tables found in chainalytics.db database - nothing to drop")
            return True
        
        logger.info(f"Found {len(table_names)} tables to drop:")
        for table in table_names:
            logger.info(f"  - {table}")
        
        # Drop each table
        dropped_tables = []
        failed_tables = []
        
        for table in table_names:
            try:
                logger.info(f"Dropping table: {table}")
                spark.sql(f"DROP TABLE IF EXISTS chainalytics.db.{table}")
                dropped_tables.append(table)
                logger.info(f"✓ Successfully dropped table: {table}")
            except Exception as e:
                failed_tables.append(table)
                logger.error(f"✗ Failed to drop table {table}: {str(e)}")
        
        # Verify tables were dropped
        logger.info("Verifying tables were dropped...")
        remaining_tables = spark.sql("SHOW TABLES IN chainalytics.db").collect()
        remaining_table_names = [row.tableName for row in remaining_tables]
        
        logger.info("=" * 60)
        logger.info("DROP OPERATION SUMMARY:")
        logger.info("=" * 60)
        logger.info(f"✓ Successfully dropped {len(dropped_tables)} tables:")
        for table in dropped_tables:
            logger.info(f"  - {table}")
        
        if failed_tables:
            logger.error(f"✗ Failed to drop {len(failed_tables)} tables:")
            for table in failed_tables:
                logger.error(f"  - {table}")
        
        if remaining_table_names:
            logger.warning(f"⚠️ {len(remaining_table_names)} tables still remain:")
            for table in remaining_table_names:
                logger.warning(f"  - {table}")
        else:
            logger.info("✓ All tables successfully dropped from chainalytics.db database")
        
        return len(failed_tables) == 0
        
    except Exception as e:
        logger.error(f"✗ Failed to drop tables: {str(e)}")
        raise

def drop_database(spark):
    """Drop the entire chainalytics.db database"""
    try:
        logger.info("Attempting to drop chainalytics.db database...")
        
        # First ensure all tables are dropped
        tables = spark.sql("SHOW TABLES IN chainalytics.db").collect()
        if tables:
            logger.warning("⚠️ Tables still exist in database. Cannot drop database with existing tables.")
            return False
        
        # Drop the database
        spark.sql("DROP DATABASE IF EXISTS chainalytics.db")
        logger.info("✓ Successfully dropped chainalytics.db database")
        
        # Verify database was dropped
        databases = [row.namespace for row in spark.sql("SHOW DATABASES").collect()]
        if 'chainalytics.db' not in databases:
            logger.info("✓ Verified chainalytics.db database no longer exists")
            return True
        else:
            logger.error("✗ Failed to drop chainalytics.db database - still exists")
            return False
        
    except Exception as e:
        logger.error(f"✗ Failed to drop database: {str(e)}")
        raise

def main():
    """Main execution function"""
    logger.info("=" * 60)
    logger.info("Starting Drop All Tables Script")
    logger.info("=" * 60)
    
    spark = None
    try:
        # Create Spark session
        spark = create_spark_session()
        
        # Verify catalog and database exist
        database_exists = verify_catalog_and_database(spark)
        
        if not database_exists:
            logger.info("=" * 60)
            logger.info("🎉 SUCCESS: No chainalytics.db database found - nothing to drop!")
            logger.info("=" * 60)
            return
        
        # Drop all tables
        tables_dropped = drop_all_tables(spark)
        
        # Optionally drop the database itself (uncomment if you want to drop the entire database)
        # logger.info("Dropping the entire chainalytics.db database...")
        # database_dropped = drop_database(spark)
        
        if tables_dropped:
            logger.info("=" * 60)
            logger.info("🎉 SUCCESS: All tables dropped from chainalytics.db database!")
            logger.info("Database 'chainalytics.db' is now empty")
            logger.info("=" * 60)
        else:
            logger.error("=" * 60)
            logger.error("⚠️ PARTIAL SUCCESS: Some tables could not be dropped")
            logger.error("=" * 60)
        
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