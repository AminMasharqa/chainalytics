#!/usr/bin/env python3
"""
Create Iceberg Bronze Tables in MinIO: warehouse.catalog -> "chainalytics" database
"""

import logging
import sys
from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session():
    try:
        logger.info("Starting Spark session with Iceberg + MinIO...")
        spark = SparkSession.builder \
            .appName("Iceberg-Bronze-Tables") \
            .config("spark.sql.catalog.warehouse", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.warehouse.type", "hadoop") \
            .config("spark.sql.catalog.warehouse.warehouse", "s3a://warehouse/") \
            .config("spark.sql.catalog.warehouse.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://chainalytics-minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .getOrCreate()
        logger.info("✓ Spark session created.")
        return spark
    except Exception as e:
        logger.error(f"✗ Spark session creation failed: {e}")
        raise

def create_database_if_not_exists(spark):
    try:
        logger.info('Creating database "chainalytics" in Iceberg catalog...')
        spark.sql("""
            CREATE DATABASE IF NOT EXISTS chainalytics
            LOCATION 's3a://warehouse/chainalytics'
        """)
        logger.info("✓ Database created or already exists at s3a://warehouse/chainalytics")
    except Exception as e:
        logger.error(f"✗ Failed to create database: {e}")
        raise

def create_iceberg_tables(spark):
    try:
        logger.info("Creating Iceberg bronze tables...")

        tables = {
            "bronze_user_events": """
                event_id STRING,
                user_id STRING,
                event_type STRING,
                product_id STRING,
                event_timestamp TIMESTAMP,
                ingestion_timestamp TIMESTAMP
            """,
            "bronze_weather_data": """
                location_id STRING,
                weather_condition STRING,
                temperature DOUBLE,
                wind_speed DOUBLE,
                data_delay_hours INT,
                observation_time TIMESTAMP,
                ingestion_timestamp TIMESTAMP
            """,
            "bronze_products": """
                product_id INT,
                title STRING,
                price DOUBLE,
                category STRING,
                rating_score DOUBLE,
                rating_count INT,
                ingestion_date DATE,
                ingestion_timestamp TIMESTAMP
            """,
            "bronze_api_logs": """
                log_id STRING,
                api_source STRING,
                response_time_ms INT,
                success_flag BOOLEAN,
                call_timestamp TIMESTAMP,
                ingestion_timestamp TIMESTAMP
            """,
            "bronze_user_posts": """
                post_id INT,
                user_id INT,
                title STRING,
                created_timestamp TIMESTAMP,
                ingestion_timestamp TIMESTAMP
            """
        }

        for table_name, schema in tables.items():
            fqtn = f"`chainalytics`.{table_name}"
            logger.info(f"Creating table: {fqtn}")
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {fqtn} (
                    {schema}
                ) USING ICEBERG
                TBLPROPERTIES ('write.format.default'='orc')

            """)
            logger.info(f"✓ Created {fqtn}")
    except Exception as e:
        logger.error(f"✗ Failed to create bronze tables: {e}")
        raise

def verify_tables(spark):
    try:
        logger.info("Verifying tables in `chainalytics`...")
        tables = spark.sql("SHOW TABLES IN `chainalytics`").collect()
        for row in tables:
            logger.info(f"  - {row.tableName}")
        logger.info("✓ Verification complete.")
    except Exception as e:
        logger.error(f"✗ Table verification failed: {e}")
        raise

def main():
    logger.info("=" * 60)
    logger.info("🧊 Iceberg Bronze Table Creation Script")
    logger.info("=" * 60)

    spark = None
    try:
        spark = create_spark_session()
        create_database_if_not_exists(spark)
        create_iceberg_tables(spark)
        verify_tables(spark)
        logger.info("✅ All bronze tables created successfully in warehouse.catalog → `chainalytics`")
    except Exception as e:
        logger.error(f"💥 Script failed: {e}")
        sys.exit(1)
    finally:
        if spark:
            logger.info("Stopping Spark session...")
            spark.stop()
            logger.info("✓ Spark session stopped.")

if __name__ == "__main__":
    main()
