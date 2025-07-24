#!/usr/bin/env python3
"""
Show the first N rows of every table in the Iceberg "chainalytics" database on MinIO.
"""

import logging
from pyspark.sql import SparkSession

# Configure logging
testing_level = logging.INFO
logging.basicConfig(level=testing_level, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def create_spark_session():
    """
    Initialize a SparkSession configured for Iceberg and MinIO.
    """
    try:
        logger.info("Starting Spark session with Iceberg + MinIO configuration...")
        spark = (
            SparkSession.builder
                .appName("Show-Tables-Content")
                .config("spark.sql.catalog.warehouse", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.warehouse.type", "hadoop")
                .config("spark.sql.catalog.warehouse.warehouse", "s3a://warehouse/")
                .config("spark.sql.catalog.warehouse.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
                .config("spark.hadoop.fs.s3a.endpoint", "http://chainalytics-minio:9000")
                .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
                .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .getOrCreate()
        )
        logger.info("✓ Spark session created.")
        return spark
    except Exception as e:
        logger.error(f"✗ Spark session creation failed: {e}")
        raise


def show_tables_content(spark, database: str = "chainalytics", limit: int = 10):
    """
    List all tables in the specified Iceberg database and show up to `limit` rows for each.
    """
    try:
        logger.info(f"Listing tables in database '{database}'...")
        tables_df = spark.sql(f"SHOW TABLES IN `warehouse``.`{database}")
        table_rows = tables_df.select("tableName").collect()
        if not table_rows:
            logger.warning(f"No tables found in database '{database}'.")
            return
        for row in table_rows:
            table_name = row["tableName"]
            logger.info(f"Showing up to {limit} rows from table {database}.{table_name}")
            # Use catalog reference, avoid direct path concatenation
            spark.table(f"{database}.{table_name}").limit(limit).show(truncate=False)
    except Exception as e:
        logger.error(f"✗ Error while showing tables content: {e}")
        raise


if __name__ == "__main__":
    spark = create_spark_session()
    show_tables_content(spark)
