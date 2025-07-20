#!/usr/bin/env python3
"""
Data Quality Checks - Simple but comprehensive validation
Validates data across bronze, silver, and gold layers
"""

import sys
sys.path.append('/opt/spark/config')
sys.path.append('/opt/spark/utils')

from spark_config import spark_config
from pyspark.sql.functions import *
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def data_quality_checks():
    """Run comprehensive data quality checks"""
    
    logger.info("Starting data quality checks")
    
    try:
        spark = spark_config.create_spark_session()
        
        quality_results = []
        
        # Check 1: Bronze layer completeness
        tables_to_check = [
            "bronze_product_performance",
            "bronze_user_sessions", 
            "bronze_customer_behavior"
        ]
        
        for table in tables_to_check:
            try:
                df = spark.table(f"iceberg_catalog.chainalytics.{table}")
                row_count = df.count()
                null_count = df.filter(col("ingestion_timestamp").isNull()).count()
                
                quality_results.append({
                    "table_name": table,
                    "check_type": "completeness",
                    "metric": "row_count",
                    "value": row_count,
                    "status": "PASS" if row_count > 0 else "FAIL"
                })
                
                quality_results.append({
                    "table_name": table,
                    "check_type": "completeness", 
                    "metric": "null_timestamps",
                    "value": null_count,
                    "status": "PASS" if null_count == 0 else "FAIL"
                })
                
            except Exception as e:
                quality_results.append({
                    "table_name": table,
                    "check_type": "availability",
                    "metric": "table_exists",
                    "value": 0,
                    "status": "FAIL"
                })
        
        # Check 2: Silver layer data quality
        try:
            df_silver_products = spark.table("iceberg_catalog.chainalytics.silver_product_performance")
            
            # Check for valid price tiers
            valid_tiers = df_silver_products.filter(col("price_tier").isin("Low", "Medium", "High")).count()
            total_products = df_silver_products.count()
            
            quality_results.append({
                "table_name": "silver_product_performance",
                "check_type": "validity",
                "metric": "valid_price_tiers",
                "value": valid_tiers,
                "status": "PASS" if valid_tiers == total_products else "FAIL"
            })
            
            # Check for valid quality ratings
            valid_ratings = df_silver_products.filter(col("quality_rating").isin("Excellent", "Good", "Average", "Poor")).count()
            
            quality_results.append({
                "table_name": "silver_product_performance",
                "check_type": "validity",
                "metric": "valid_quality_ratings", 
                "value": valid_ratings,
                "status": "PASS" if valid_ratings == total_products else "FAIL"
            })
            
        except Exception as e:
            quality_results.append({
                "table_name": "silver_product_performance",
                "check_type": "availability",
                "metric": "table_exists",
                "value": 0,
                "status": "FAIL"
            })
        
        # Check 3: Gold layer business logic
        try:
            df_business_summary = spark.table("iceberg_catalog.chainalytics.gold_daily_business_summary")
            
            # Check for reasonable business metrics
            unreasonable_conversion = df_business_summary.filter(col("conversion_rate") > 1.0).count()
            negative_revenue = df_business_summary.filter(col("total_revenue") < 0).count()
            
            quality_results.append({
                "table_name": "gold_daily_business_summary",
                "check_type": "business_logic",
                "metric": "conversion_rate_valid",
                "value": unreasonable_conversion,
                "status": "PASS" if unreasonable_conversion == 0 else "FAIL"
            })
            
            quality_results.append({
                "table_name": "gold_daily_business_summary", 
                "check_type": "business_logic",
                "metric": "revenue_positive",
                "value": negative_revenue,
                "status": "PASS" if negative_revenue == 0 else "FAIL"
            })
            
        except Exception as e:
            quality_results.append({
                "table_name": "gold_daily_business_summary",
                "check_type": "availability",
                "metric": "table_exists",
                "value": 0,
                "status": "FAIL"
            })
        
        # Check 4: SCD Type 2 validation
        try:
            df_customer_dim = spark.table("iceberg_catalog.chainalytics.dim_customer")
            
            # Check for overlapping date ranges (should not exist in proper SCD Type 2)
            total_customers = df_customer_dim.count()
            current_customers = df_customer_dim.filter(col("valid_to").isNull()).count()
            
            quality_results.append({
                "table_name": "dim_customer",
                "check_type": "scd_validation",
                "metric": "current_records_exist",
                "value": current_customers,
                "status": "PASS" if current_customers > 0 else "FAIL"
            })
            
            # Check that we have historical records (more than current)
            has_history = total_customers > current_customers
            
            quality_results.append({
                "table_name": "dim_customer",
                "check_type": "scd_validation", 
                "metric": "has_historical_records",
                "value": total_customers - current_customers,
                "status": "PASS" if has_history else "FAIL"
            })
            
        except Exception as e:
            quality_results.append({
                "table_name": "dim_customer",
                "check_type": "availability",
                "metric": "table_exists",
                "value": 0,
                "status": "FAIL"
            })
        
        # Create quality report DataFrame
        quality_schema = StructType([
            StructField("table_name", StringType(), True),
            StructField("check_type", StringType(), True),
            StructField("metric", StringType(), True),
            StructField("value", IntegerType(), True),
            StructField("status", StringType(), True)
        ])
        
        quality_data = [(r["table_name"], r["check_type"], r["metric"], int(r["value"]), r["status"]) 
                       for r in quality_results]
        
        df_quality = spark.createDataFrame(quality_data, quality_schema) \
            .withColumn("check_timestamp", current_timestamp()) \
            .withColumn("check_date", current_date())
        
        # Save quality report
        df_quality.writeTo("iceberg_catalog.chainalytics.data_quality_report").createOrReplace()
        
        # Print summary
        total_checks = len(quality_results)
        passed_checks = len([r for r in quality_results if r["status"] == "PASS"])
        failed_checks = total_checks - passed_checks
        
        logger.info(f"📊 Data Quality Summary:")
        logger.info(f"   Total Checks: {total_checks}")
        logger.info(f"   ✅ Passed: {passed_checks}")
        logger.info(f"   ❌ Failed: {failed_checks}")
        logger.info(f"   Success Rate: {passed_checks/total_checks*100:.1f}%")
        
        # Show detailed results
        df_quality.select("table_name", "check_type", "metric", "status").show(truncate=False)
        
        logger.info("Data quality checks completed successfully")
        
    except Exception as e:
        logger.error(f"Error in data quality checks: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    data_quality_checks()