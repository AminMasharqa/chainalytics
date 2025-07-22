#!/usr/bin/env python3
"""
Simple Spark Config - Creates warehouse bucket and chainalytics database
Matches the entrypoint.sh setup with iceberg catalog
"""

from pyspark.sql import SparkSession
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def setup_warehouse():
    """Creates warehouse bucket and chainalytics database"""
    # Get existing Spark session (created by spark-submit)
    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    
    try:
        # Debug: Print the current warehouse configuration
        warehouse_location = spark.conf.get("spark.sql.catalog.iceberg.warehouse")
        logger.info(f"🔍 Warehouse location: {warehouse_location}")
        
        # First test basic S3 connectivity with a simple operation
        logger.info("🔍 Testing basic S3 connectivity...")
        try:
            # Try to create a simple DataFrame and write to S3 as a test
            test_df = spark.createDataFrame([(1, "test")], ["id", "message"])
            test_df.write.mode("overwrite").parquet("s3a://warehouse/test_connectivity/")
            logger.info("✅ S3 connectivity works!")
            
        except Exception as e:
            logger.error(f"❌ S3 connectivity failed: {e}")
            raise
        
        # Now try Iceberg operations - matching your entrypoint.sh catalog name
        logger.info("🔍 Testing Iceberg catalog...")
        try:
            # Test if iceberg catalog is accessible (matches your --conf spark.sql.catalog.iceberg)
            spark.sql("SHOW NAMESPACES IN iceberg").show()
            logger.info("✅ Iceberg catalog accessible!")
        except Exception as e:
            logger.info(f"ℹ️ No existing namespaces yet: {e}")
        
        # Create chainalytics namespace using the iceberg catalog
        logger.info("📁 Creating Iceberg namespace: chainalytics")
        spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.chainalytics")
        logger.info("✅ Created namespace: iceberg.chainalytics")
        
        # Verify namespace was created
        logger.info("🔍 Checking namespaces in iceberg catalog...")
        try:
            namespaces = spark.sql("SHOW NAMESPACES IN iceberg").collect()
            logger.info("📋 Available namespaces:")
            for ns in namespaces:
                logger.info(f"  - {ns.namespace}")
        except Exception as e:
            logger.warning(f"Could not list namespaces: {e}")
        
        # Create a table in the iceberg.chainalytics namespace
        logger.info("📋 Creating init table in iceberg.chainalytics...")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS iceberg.chainalytics.init_table (
                id INT,
                status STRING,
                created_at TIMESTAMP
            ) USING iceberg
            PARTITIONED BY (created_at)
        """)
        
        # Insert test data
        logger.info("💾 Inserting test data...")
        spark.sql("""
            INSERT INTO iceberg.chainalytics.init_table 
            VALUES (1, 'warehouse_ready', current_timestamp())
        """)
        
        # Verify the table was created and has data
        logger.info("🔍 Verifying table creation...")
        result = spark.sql("SELECT * FROM iceberg.chainalytics.init_table").collect()
        logger.info(f"📊 Table has {len(result)} rows")
        for row in result:
            logger.info(f"  - ID: {row.id}, Status: {row.status}")
        
        # Show tables in the namespace
        logger.info("📋 Tables in iceberg.chainalytics:")
        spark.sql("SHOW TABLES IN iceberg.chainalytics").show()
        
        logger.info("🎉 SUCCESS: Warehouse and ChainAnalytics database created!")
        logger.info("📍 MinIO UI: http://localhost:9001")
        logger.info("📁 Check bucket: warehouse")
        logger.info("📂 Check folder: chainalytics/init_table/")
        
    except Exception as e:
        logger.error(f"❌ Setup failed: {e}")
        logger.error(f"❌ Exception type: {type(e)}")
        import traceback
        logger.error(f"❌ Full traceback: {traceback.format_exc()}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    setup_warehouse()