#!/usr/bin/env python3
"""
Verify Table Creation - Check all tables were created successfully
Location: jobs/setup/verify_table_creation.py
"""

import sys
sys.path.append('/opt/spark/jobs/utils')

from spark_config import spark_config
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def verify_table_creation():
    """Verify all bronze, silver, and gold tables were created successfully"""
    
    logger.info("Starting table verification")
    
    try:
        spark = spark_config.create_spark_session("TableVerification")
        
        # Expected tables by layer
        expected_tables = {
            'bronze': [
                'bronze_product_performance',
                'bronze_user_sessions', 
                'bronze_customer_behavior',
                'bronze_streaming_events',
                'bronze_streaming_events_late'
            ],
            'silver': [
                'silver_product_performance',
                'silver_user_sessions',
                'silver_logistics_impact', 
                'silver_customer_behavior',
                'silver_system_health'
            ],
            'gold': [
                'dim_date',
                'dim_product',
                'dim_customer',
                'fact_customer_activity',
                'fact_operational_metrics',
                'gold_customer_conversion_features',
                'gold_daily_business_summary'
            ]
        }
        
        verification_results = {}
        total_tables = 0
        successful_tables = 0
        
        # Check each layer
        for layer, tables in expected_tables.items():
            logger.info(f"\n🔍 Verifying {layer.upper()} layer tables:")
            layer_results = {}
            
            for table_name in tables:
                total_tables += 1
                full_table_name = f"iceberg_catalog.chainalytics.{table_name}"
                
                try:
                    # Try to describe the table
                    df = spark.table(full_table_name)
                    column_count = len(df.columns)
                    row_count = df.count()
                    
                    logger.info(f"  ✅ {table_name}: {column_count} columns, {row_count} rows")
                    layer_results[table_name] = {
                        'exists': True,
                        'columns': column_count,
                        'rows': row_count,
                        'status': 'SUCCESS'
                    }
                    successful_tables += 1
                    
                except Exception as e:
                    logger.error(f"  ❌ {table_name}: {str(e)}")
                    layer_results[table_name] = {
                        'exists': False,
                        'error': str(e),
                        'status': 'FAILED'
                    }
            
            verification_results[layer] = layer_results
        
        # Summary report
        logger.info("\n" + "="*60)
        logger.info("📊 TABLE VERIFICATION SUMMARY")
        logger.info("="*60)
        
        for layer, results in verification_results.items():
            success_count = sum(1 for r in results.values() if r['status'] == 'SUCCESS')
            total_count = len(results)
            success_rate = (success_count / total_count) * 100
            
            logger.info(f"{layer.upper()} Layer: {success_count}/{total_count} tables ({success_rate:.1f}%)")
        
        logger.info(f"\nOVERALL: {successful_tables}/{total_tables} tables created successfully")
        
        # Show all tables in database
        logger.info("\n📋 All tables in iceberg_catalog.chainalytics:")
        all_tables = spark.sql("SHOW TABLES IN iceberg_catalog.chainalytics")
        all_tables.show(truncate=False)
        
        # Database summary
        logger.info("\n💾 Database summary:")
        spark.sql("DESCRIBE DATABASE iceberg_catalog.chainalytics").show()
        
        # Final status
        if successful_tables == total_tables:
            logger.info("🎉 ALL TABLES CREATED SUCCESSFULLY!")
            return True
        else:
            logger.warning(f"⚠️  {total_tables - successful_tables} tables failed to create")
            return False
            
    except Exception as e:
        logger.error(f"Error during table verification: {str(e)}")
        raise
    finally:
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    success = verify_table_creation()
    exit(0 if success else 1)