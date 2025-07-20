#!/usr/bin/env python3
"""
Streaming Job: Kafka to Bronze
Simple streaming implementation for real-time data ingestion
"""

import sys
sys.path.append('/opt/spark/config')
sys.path.append('/opt/spark/utils')

from spark_config import spark_config
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def kafka_to_bronze_streaming():
    """Stream data from Kafka to bronze tables"""
    
    logger.info("Starting Kafka to Bronze streaming")
    
    try:
        spark = spark_config.create_spark_session()
        
        # Correct schema matching actual Kafka data format
        event_schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("event_timestamp", StringType(), True),      # ✅ FIXED - was "timestamp"
            StructField("ingestion_timestamp", StringType(), True)   # ✅ FIXED - was "value"
        ])
        
        # Read from Kafka with correct bootstrap servers
        kafka_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:29092") \
            .option("subscribe", "user-events") \
            .option("startingOffsets", "latest") \
            .load()
        
        # Parse the JSON messages
        parsed_df = kafka_df.select(
            from_json(col("value").cast("string"), event_schema).alias("data"),
            col("timestamp").alias("kafka_timestamp")  # ✅ FIXED - use Kafka's timestamp field
        ).select("data.*", "kafka_timestamp") \
         .withColumn("processing_timestamp", current_timestamp()) \
         .withColumn("source_system", lit("kafka_stream"))
        
        # Write to bronze table (append mode for streaming)
        query = parsed_df.writeStream \
            .format("iceberg") \
            .outputMode("append") \
            .option("path", "s3a://chainalytics/warehouse/chainalytics/bronze_streaming_events") \
            .option("checkpointLocation", "s3a://chainalytics/checkpoints/kafka_to_bronze") \
            .trigger(processingTime='30 seconds') \
            .start()
        
        logger.info("✅ Kafka streaming started successfully")
        logger.info("🔄 Streaming will run continuously...")
        
        # Run continuously - removed timeout for production streaming
        query.awaitTermination()  # ✅ FIXED - removed 300 second timeout
        
    except Exception as e:
        logger.error(f"❌ Error in Kafka streaming: {str(e)}")
        import traceback
        logger.error(f"📊 Full traceback: {traceback.format_exc()}")
        raise
    finally:
        if 'spark' in locals():
            spark.stop()
            logger.info("🛑 Spark session stopped")

if __name__ == "__main__":
    kafka_to_bronze_streaming()