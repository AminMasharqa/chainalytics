#!/usr/bin/env python3
"""
ETL from Silver to Gold for chainalytics
Reads Iceberg silver tables in MinIO, bootstraps Iceberg gold tables with backtick-qualified identifiers
and explicit LOCATIONs, applies simple aggregations/computations, and writes into gold tables.
"""

import logging
import sys
from datetime import date, datetime
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import (
    col, avg, sum as spark_sum, row_number, when, lit,
    to_date, current_timestamp, expr, concat_ws
)
from pyspark.sql.window import Window

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create Spark session configured for Iceberg + MinIO"""
    try:
        spark = (SparkSession.builder
            .appName("ETL-Silver-to-Gold")
            .config("spark.sql.catalog.warehouse", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.warehouse.type", "hadoop")
            .config("spark.sql.catalog.warehouse.warehouse", "s3a://warehouse/")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.hadoop.fs.s3a.endpoint", "http://chainalytics-minio:9000")
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .getOrCreate())
        logger.info("✓ Spark session created.")
        return spark
    except Exception as e:
        logger.error(f"✗ Spark session creation failed: {e}")
        raise

def bootstrap_gold_tables(spark):
    """Ensure all gold tables exist with explicit LOCATIONs"""
    logger.info("Bootstrapping Iceberg gold tables…")
    spark.sql("""
      CREATE TABLE IF NOT EXISTS `warehouse`.`chainalytics`.`gold_customer_analytics` (
        customer_id STRING,
        customer_name STRING,
        total_orders BIGINT,
        total_spent DOUBLE,
        avg_order_value DOUBLE,
        favorite_category STRING,
        last_order_date DATE,
        customer_tier STRING,
        lifetime_value DOUBLE,
        churn_risk_score DOUBLE,
        ingestion_timestamp TIMESTAMP
      ) USING ICEBERG
      LOCATION 's3a://warehouse/chainalytics/gold_customer_analytics/'
    """)

    spark.sql("""
      CREATE TABLE IF NOT EXISTS `warehouse`.`chainalytics`.`gold_daily_summary` (
        business_date DATE,
        total_revenue DOUBLE,
        total_orders BIGINT,
        active_customers BIGINT,
        new_customers BIGINT,
        avg_order_value DOUBLE,
        top_selling_category STRING,
        weather_impact_score DOUBLE,
        api_performance_score DOUBLE,
        overall_health_score DOUBLE,
        ingestion_timestamp TIMESTAMP
      ) USING ICEBERG
      LOCATION 's3a://warehouse/chainalytics/gold_daily_summary/'
    """)

    spark.sql("""
      CREATE TABLE IF NOT EXISTS `warehouse`.`chainalytics`.`gold_product_performance` (
        product_id INT,
        product_name STRING,
        category STRING,
        revenue_rank INT,
        conversion_rank INT,
        customer_satisfaction DOUBLE,
        inventory_status STRING,
        price_optimization_score DOUBLE,
        recommended_action STRING,
        ingestion_timestamp TIMESTAMP
      ) USING ICEBERG
      LOCATION 's3a://warehouse/chainalytics/gold_product_performance/'
    """)

    spark.sql("""
      CREATE TABLE IF NOT EXISTS `warehouse`.`chainalytics`.`gold_executive_dashboard` (
        kpi_date DATE,
        revenue_growth_rate DOUBLE,
        customer_acquisition_rate DOUBLE,
        customer_retention_rate DOUBLE,
        average_customer_lifetime_value DOUBLE,
        profit_margin DOUBLE,
        operational_efficiency_score DOUBLE,
        customer_satisfaction_index DOUBLE,
        market_share_estimate DOUBLE,
        ingestion_timestamp TIMESTAMP
      ) USING ICEBERG
      LOCATION 's3a://warehouse/chainalytics/gold_executive_dashboard/'
    """)

    spark.sql("""
      CREATE TABLE IF NOT EXISTS `warehouse`.`chainalytics`.`gold_weather_correlation` (
        correlation_id STRING,
        location_id STRING,
        weather_condition STRING,
        avg_temperature DOUBLE,
        sales_impact_percentage DOUBLE,
        optimal_weather_score DOUBLE,
        business_date DATE,
        ingestion_timestamp TIMESTAMP
      ) USING ICEBERG
      LOCATION 's3a://warehouse/chainalytics/gold_weather_correlation/'
    """)
    logger.info("✓ Gold tables ensured.")

def etl_customer_analytics(spark):
    logger.info("ETL → gold_customer_analytics")
    df = spark.table("`warehouse`.`chainalytics`.`silver_user_behavior`")
    out = (df
        .withColumnRenamed("user_id","customer_id")
        .withColumn("customer_name", lit("unknown"))
        .withColumn("total_orders", col("total_events"))
        .withColumn("total_spent", col("total_events") * lit(10.0))
        .withColumn("avg_order_value", when(col("total_orders")==0, lit(0.0))
                    .otherwise(col("total_spent")/col("total_orders")))
        .withColumn("favorite_category", lit("N/A"))
        .withColumn("last_order_date", col("last_activity_date"))
        .withColumn("customer_tier",
            when(col("total_orders")>20, lit("Gold"))
            .when(col("total_orders")>10, lit("Silver"))
            .otherwise(lit("Bronze")))
        .withColumn("lifetime_value", col("total_spent"))
        .withColumn("churn_risk_score", when(expr("datediff(current_date(), last_order_date)")>30, lit(0.8)).otherwise(lit(0.2)))
        .withColumn("ingestion_timestamp", current_timestamp())
        .select(
          "customer_id","customer_name","total_orders","total_spent",
          "avg_order_value","favorite_category","last_order_date",
          "customer_tier","lifetime_value","churn_risk_score","ingestion_timestamp"
        )
    )
    out.writeTo("`warehouse`.`chainalytics`.`gold_customer_analytics`").append()
    logger.info(f"✓ gold_customer_analytics loaded ({out.count()} rows)")

def etl_daily_summary(spark):
    logger.info("ETL → gold_daily_summary")
    api = spark.table("`warehouse`.`chainalytics`.`silver_api_performance`")
    weather = spark.table("`warehouse`.`chainalytics`.`silver_weather_impact`")
    weatheragg = weather.groupBy("date").agg(avg("impact_score").alias("weather_impact_score"))
    agg = (api.groupBy("date").agg(
            spark_sum("total_calls").alias("total_orders"),
            avg("success_rate").alias("api_performance_score")
        )
        .join(weatheragg, "date", "left")
        .select(
          col("date").alias("business_date"),
          lit(0.0).alias("total_revenue"),
          col("total_orders"),
          lit(0).cast("bigint").alias("active_customers"),
          lit(0).cast("bigint").alias("new_customers"),
          lit(0.0).alias("avg_order_value"),
          lit("N/A").alias("top_selling_category"),
          col("weather_impact_score"),
          col("api_performance_score"),
          ((col("weather_impact_score")+col("api_performance_score"))/2).alias("overall_health_score"),
          current_timestamp().alias("ingestion_timestamp")
        )
    )
    agg.writeTo("`warehouse`.`chainalytics`.`gold_daily_summary`").append()
    logger.info(f"✓ gold_daily_summary loaded ({agg.count()} rows)")

def etl_product_performance(spark):
    logger.info("ETL → gold_product_performance")
    df = spark.table("`warehouse`.`chainalytics`.`silver_product_analytics`")
    w1 = Window.orderBy(col("total_views").desc())
    w2 = Window.orderBy(col("conversion_rate").desc())
    out = (df
        .withColumn("revenue_rank", row_number().over(w1))
        .withColumn("conversion_rank", row_number().over(w2))
        .withColumnRenamed("avg_rating","customer_satisfaction")
        .withColumn("inventory_status", lit("InStock"))
        .withColumn("price_optimization_score", lit(0.0))
        .withColumn("recommended_action", when(col("conversion_rate")<0.1, lit("Discount")).otherwise(lit("Keep")))
        .withColumn("ingestion_timestamp", current_timestamp())
        .select(
          "product_id","product_name","category","revenue_rank",
          "conversion_rank","customer_satisfaction","inventory_status",
          "price_optimization_score","recommended_action","ingestion_timestamp"
        )
    )
    out.writeTo("`warehouse`.`chainalytics`.`gold_product_performance`").append()
    logger.info(f"✓ gold_product_performance loaded ({out.count()} rows)")

def etl_executive_dashboard(spark):
    logger.info("ETL → gold_executive_dashboard")
    row = Row(
      kpi_date=date.today(),
      revenue_growth_rate=0.05,
      customer_acquisition_rate=0.02,
      customer_retention_rate=0.90,
      average_customer_lifetime_value=1000.0,
      profit_margin=0.20,
      operational_efficiency_score=0.85,
      customer_satisfaction_index=0.90,
      market_share_estimate=0.10,
      ingestion_timestamp=datetime.now()
    )
    df = spark.createDataFrame([row])
    df.writeTo("`warehouse`.`chainalytics`.`gold_executive_dashboard`").append()
    logger.info("✓ gold_executive_dashboard loaded (1 row)")

def etl_weather_correlation(spark):
    logger.info("ETL → gold_weather_correlation")
    df = spark.table("`warehouse`.`chainalytics`.`silver_weather_impact`")
    out = (df
        .withColumn("correlation_id", concat_ws("_", col("location_id"), col("date")))
        .withColumnRenamed("weather_category","weather_condition")
        .withColumn("sales_impact_percentage", col("impact_score"))
        .withColumn("optimal_weather_score", expr("100 - impact_score"))
        .withColumn("business_date", col("date"))
        .withColumn("ingestion_timestamp", current_timestamp())
        .select(
          "correlation_id","location_id","weather_condition","avg_temperature",
          "sales_impact_percentage","optimal_weather_score","business_date",
          "ingestion_timestamp"
        )
    )
    out.writeTo("`warehouse`.`chainalytics`.`gold_weather_correlation`").append()
    logger.info(f"✓ gold_weather_correlation loaded ({out.count()} rows)")

def main():
    logger.info("="*60)
    logger.info("▶️  ETL: Silver → Gold")
    logger.info("="*60)
    try:
        spark = create_spark_session()
        bootstrap_gold_tables(spark)
        etl_customer_analytics(spark)
        etl_daily_summary(spark)
        etl_product_performance(spark)
        etl_executive_dashboard(spark)
        etl_weather_correlation(spark)
        logger.info("🎉 ETL complete: all gold tables updated.")
    except Exception as e:
        logger.error(f"💥 ETL failed: {e}")
        sys.exit(1)
    finally:
        if 'spark' in locals():
            spark.stop()
            logger.info("✓ Spark session stopped.")

if __name__ == "__main__":
    main()
