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
        logger.info("Creating Iceberg silver tables...")

        tables = {
            "silver_user_behavior": """
                user_id STRING,
                total_events INT,
                unique_products_viewed INT,
                avg_session_duration DOUBLE,
                last_activity_date DATE,
                user_segment STRING,
                ingestion_timestamp TIMESTAMP
            """,
            "silver_product_analytics": """
                product_id INT,
                product_name STRING,
                category STRING,
                current_price DOUBLE,
                avg_rating DOUBLE,
                total_views INT,
                total_purchases INT,
                conversion_rate DOUBLE,
                ingestion_timestamp TIMESTAMP
            """,
            "silver_weather_impact": """
                location_id STRING,
                date DATE,
                avg_temperature DOUBLE,
                weather_category STRING,
                delivery_delay_hours INT,
                impact_score DOUBLE,
                ingestion_timestamp TIMESTAMP
            """,
            "silver_api_performance": """
                api_source STRING,
                date DATE,
                total_calls INT,
                avg_response_time_ms DOUBLE,
                success_rate DOUBLE,
                error_count INT,
                performance_grade STRING,
                ingestion_timestamp TIMESTAMP
            """,
            "silver_product_performance": """
                product_key STRING,
                product_name STRING,
                price_tier STRING,
                category_std STRING,
                popularity_score DOUBLE,
                quality_rating STRING,
                last_updated DATE,
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
        logger.error(f"✗ Failed to create tables: {e}")
        raise

def initialize_silver_api_performance_table(spark):
    try:
        logger.info("Initializing silver_api_performance table with a dummy row...")
        dummy_data = [
            Row(
                api_source="dummy_api",
                date=datetime.today().date(),
                total_calls=0,
                avg_response_time_ms=0.0,
                success_rate=0.0,
                error_count=0,
                performance_grade="N/A",
                ingestion_timestamp=datetime.now()
            )
        ]
        df = spark.createDataFrame(dummy_data)
        # Note the 'warehouse' catalog prefix here!
        df.writeTo("warehouse.chainalytics.silver_api_performance").append()
        logger.info("✓ silver_api_performance table initialized with dummy data.")
    except Exception as e:
        logger.error(f"✗ Failed to initialize silver_api_performance table: {e}")
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
    logger.info("🧊 Iceberg Silver Table Creation Script")
    logger.info("=" * 60)

    spark = None
    try:
        spark = create_spark_session()
        # logger.info("========================================================================================")

        # spark.sql("SHOW TABLES IN chainalytics.db").show()        # tables in default catalog

        # spark.sql("SHOW TABLES IN warehouse.chainalytics.db").show()  # tables in warehouse catalog
        # logger.info("========================================================================================")

        create_database_if_not_exists(spark)
        create_iceberg_tables(spark)
        # initialize_silver_api_performance_table(spark)  # <- New call to insert dummy row


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
