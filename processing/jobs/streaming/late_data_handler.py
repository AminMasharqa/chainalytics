#!/usr/bin/env python3
"""
Late Data Handler - Handle data arriving up to 48 hours after event time
Implements watermarking and reprocessing logic for out-of-order data
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

def late_data_handler():
    """Handle late-arriving data with 48-hour watermark"""
    
    logger.info("Starting late data handler with 48-hour watermark")
    
    try:
        spark = spark_config.create_spark_session()
        
        # Schema for streaming events with event time
        event_schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("event_timestamp", StringType(), True),  # Original event time
            StructField("value", DoubleType(), True),
            StructField("session_id", StringType(), True)
        ])
        
        # Read from Kafka with late data
        kafka_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("subscribe", "user_events,late_events") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Parse JSON and extract event timestamp
        parsed_df = kafka_df.select(
            from_json(col("value").cast("string"), event_schema).alias("data"),
            col("timestamp").alias("kafka_timestamp"),
            col("topic").alias("source_topic")
        ).select("data.*", "kafka_timestamp", "source_topic")
        
        # Convert event timestamp to proper timestamp type
        df_with_event_time = parsed_df \
            .withColumn("event_timestamp", to_timestamp(col("event_timestamp"), "yyyy-MM-dd HH:mm:ss")) \
            .withColumn("processing_timestamp", current_timestamp()) \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .filter(col("event_timestamp").isNotNull())
        
        # Add late data detection logic
        df_with_lateness = df_with_event_time \
            .withColumn("arrival_delay_hours", 
                (unix_timestamp(col("processing_timestamp")) - unix_timestamp(col("event_timestamp"))) / 3600) \
            .withColumn("is_late_data", 
                col("arrival_delay_hours") > 1.0) \
            .withColumn("late_data_category",
                when(col("arrival_delay_hours") <= 1, "On-time")
                .when(col("arrival_delay_hours") <= 6, "Slightly Late")
                .when(col("arrival_delay_hours") <= 24, "Late")
                .when(col("arrival_delay_hours") <= 48, "Very Late")
                .otherwise("Too Late")) \
            .withColumn("data_quality_score",
                when(col("arrival_delay_hours") <= 1, 100.0)
                .when(col("arrival_delay_hours") <= 6, 90.0)
                .when(col("arrival_delay_hours") <= 24, 75.0)
                .when(col("arrival_delay_hours") <= 48, 50.0)
                .otherwise(0.0))
        
        # Apply 48-hour watermark for late data handling
        df_with_watermark = df_with_lateness \
            .withWatermark("event_timestamp", "48 hours")
        
        # ==========================================
        # STRATEGY 1: Direct Append to Bronze Layer
        # ==========================================
        
        def write_to_bronze_with_late_handling(df, epoch_id):
            """Custom function to handle late data in bronze layer"""
            
            try:
                # Separate on-time vs late data
                df_ontime = df.filter(col("arrival_delay_hours") <= 1.0)
                df_late = df.filter((col("arrival_delay_hours") > 1.0) & (col("arrival_delay_hours") <= 48.0))
                df_too_late = df.filter(col("arrival_delay_hours") > 48.0)
                
                batch_timestamp = current_timestamp()
                
                # Write on-time data to main bronze table
                if df_ontime.count() > 0:
                    df_ontime_final = df_ontime \
                        .withColumn("batch_id", lit(epoch_id)) \
                        .withColumn("batch_timestamp", batch_timestamp) \
                        .withColumn("table_partition", date_format(col("event_timestamp"), "yyyy-MM-dd"))
                    
                    df_ontime_final.write \
                        .format("iceberg") \
                        .mode("append") \
                        .option("path", "s3a://iceberg-warehouse/bronze/streaming_events") \
                        .save()
                    
                    logger.info(f"✅ Batch {epoch_id}: Wrote {df_ontime.count()} on-time records to bronze")
                
                # Write late data to bronze with special handling
                if df_late.count() > 0:
                    df_late_final = df_late \
                        .withColumn("batch_id", lit(epoch_id)) \
                        .withColumn("batch_timestamp", batch_timestamp) \
                        .withColumn("table_partition", date_format(col("event_timestamp"), "yyyy-MM-dd")) \
                        .withColumn("requires_reprocessing", lit(True))
                    
                    df_late_final.write \
                        .format("iceberg") \
                        .mode("append") \
                        .option("path", "s3a://iceberg-warehouse/bronze/streaming_events_late") \
                        .save()
                    
                    logger.info(f"🔄 Batch {epoch_id}: Wrote {df_late.count()} late records to bronze_late")
                    
                    # Trigger reprocessing of affected downstream tables
                    trigger_downstream_reprocessing(df_late, epoch_id)
                
                # Log too-late data (beyond 48 hours)
                if df_too_late.count() > 0:
                    df_too_late_final = df_too_late \
                        .withColumn("batch_id", lit(epoch_id)) \
                        .withColumn("batch_timestamp", batch_timestamp) \
                        .withColumn("rejection_reason", lit("Beyond 48-hour window"))
                    
                    df_too_late_final.write \
                        .format("iceberg") \
                        .mode("append") \
                        .option("path", "s3a://iceberg-warehouse/monitoring/rejected_late_data") \
                        .save()
                    
                    logger.warning(f"❌ Batch {epoch_id}: Rejected {df_too_late.count()} records (too late)")
                
            except Exception as e:
                logger.error(f"Error in batch {epoch_id}: {str(e)}")
                raise
        
        def trigger_downstream_reprocessing(late_data_df, epoch_id):
            """Trigger reprocessing of downstream silver/gold tables for late data"""
            
            try:
                # Get unique dates that need reprocessing
                affected_dates = late_data_df.select("event_timestamp") \
                    .withColumn("event_date", date_format(col("event_timestamp"), "yyyy-MM-dd")) \
                    .select("event_date").distinct().collect()
                
                for row in affected_dates:
                    event_date = row["event_date"]
                    logger.info(f"🔄 Triggering reprocessing for date: {event_date}")
                    
                    # Mark date partition for reprocessing
                    reprocess_record = spark.createDataFrame([
                        (event_date, epoch_id, current_timestamp(), "late_data_detected", False)
                    ], ["event_date", "trigger_batch_id", "trigger_timestamp", "reason", "processed"])
                    
                    reprocess_record.write \
                        .format("iceberg") \
                        .mode("append") \
                        .option("path", "s3a://iceberg-warehouse/monitoring/reprocessing_queue") \
                        .save()
                
            except Exception as e:
                logger.error(f"Error triggering reprocessing: {str(e)}")
        
        # ==========================================
        # STRATEGY 2: Windowed Aggregations with Late Data
        # ==========================================
        
        # Real-time session aggregations with late data tolerance
        session_aggregations = df_with_watermark \
            .groupBy(
                window(col("event_timestamp"), "10 minutes", "5 minutes"),
                col("user_id"),
                col("session_id")
            ).agg(
                count("*").alias("event_count"),
                sum("value").alias("total_value"),
                max("event_timestamp").alias("last_event_time"),
                min("event_timestamp").alias("first_event_time"),
                avg("arrival_delay_hours").alias("avg_delay_hours"),
                max("is_late_data").alias("contains_late_data"),
                collect_list("late_data_category").alias("lateness_distribution")
            ) \
            .withColumn("session_duration_minutes", 
                (unix_timestamp(col("last_event_time")) - unix_timestamp(col("first_event_time"))) / 60) \
            .withColumn("window_start", col("window.start")) \
            .withColumn("window_end", col("window.end")) \
            .withColumn("processing_timestamp", current_timestamp())
        
        # ==========================================
        # OUTPUT STREAMS
        # ==========================================
        
        # Stream 1: Write raw events with late data handling
        query_raw = df_with_watermark.writeStream \
            .foreachBatch(write_to_bronze_with_late_handling) \
            .outputMode("append") \
            .option("checkpointLocation", "s3a://checkpoints/late_data_raw") \
            .trigger(processingTime='30 seconds') \
            .queryName("late_data_raw_events") \
            .start()
        
        # Stream 2: Write session aggregations (handles late data automatically)
        query_sessions = session_aggregations.writeStream \
            .format("iceberg") \
            .outputMode("append") \
            .option("path", "s3a://iceberg-warehouse/silver/streaming_session_metrics") \
            .option("checkpointLocation", "s3a://checkpoints/late_data_sessions") \
            .trigger(processingTime='60 seconds') \
            .queryName("late_data_session_aggregations") \
            .start()
        
        # Stream 3: Late data monitoring stream
        late_data_monitoring = df_with_watermark \
            .filter(col("is_late_data") == True) \
            .select(
                col("event_id"),
                col("user_id"), 
                col("event_timestamp"),
                col("processing_timestamp"),
                col("arrival_delay_hours"),
                col("late_data_category"),
                col("data_quality_score")
            ) \
            .withColumn("monitoring_timestamp", current_timestamp())
        
        query_monitoring = late_data_monitoring.writeStream \
            .format("iceberg") \
            .outputMode("append") \
            .option("path", "s3a://iceberg-warehouse/monitoring/late_data_alerts") \
            .option("checkpointLocation", "s3a://checkpoints/late_data_monitoring") \
            .trigger(processingTime='120 seconds') \
            .queryName("late_data_monitoring") \
            .start()
        
        # ==========================================
        # REPROCESSING LOGIC FOR SILVER/GOLD LAYERS
        # ==========================================
        
        def reprocess_affected_partitions():
            """Batch job to reprocess silver/gold layers when late data arrives"""
            
            # Read reprocessing queue
            reprocess_queue = spark.read \
                .format("iceberg") \
                .option("path", "s3a://iceberg-warehouse/monitoring/reprocessing_queue") \
                .load() \
                .filter(col("processed") == False)
            
            if reprocess_queue.count() > 0:
                logger.info("🔄 Found partitions that need reprocessing due to late data")
                
                for row in reprocess_queue.collect():
                    event_date = row["event_date"]
                    
                    # Reprocess silver layer for this date
                    logger.info(f"🔄 Reprocessing silver layer for date: {event_date}")
                    
                    # Read ALL data for this date (including newly arrived late data)
                    all_data_for_date = spark.read \
                        .format("iceberg") \
                        .option("path", "s3a://iceberg-warehouse/bronze/streaming_events") \
                        .load() \
                        .filter(date_format(col("event_timestamp"), "yyyy-MM-dd") == event_date) \
                        .union(
                            spark.read \
                                .format("iceberg") \
                                .option("path", "s3a://iceberg-warehouse/bronze/streaming_events_late") \
                                .load() \
                                .filter(date_format(col("event_timestamp"), "yyyy-MM-dd") == event_date)
                        )
                    
                    # Recompute silver metrics
                    silver_recomputed = all_data_for_date \
                        .groupBy("user_id", "session_id") \
                        .agg(
                            count("*").alias("total_events"),
                            sum("value").alias("session_value"),
                            max("event_timestamp").alias("session_end"),
                            min("event_timestamp").alias("session_start")
                        ) \
                        .withColumn("session_duration_min", 
                            (unix_timestamp(col("session_end")) - unix_timestamp(col("session_start"))) / 60) \
                        .withColumn("reprocessed_timestamp", current_timestamp()) \
                        .withColumn("reprocessing_reason", lit("late_data_arrival"))
                    
                    # Overwrite the partition in silver layer
                    silver_recomputed.write \
                        .format("iceberg") \
                        .mode("overwrite") \
                        .option("path", "s3a://iceberg-warehouse/silver/session_metrics_reprocessed") \
                        .save()
                    
                    logger.info(f"✅ Reprocessed silver layer for {event_date}: {silver_recomputed.count()} sessions")
                    
                    # Mark as processed
                    spark.sql(f"""
                        UPDATE iceberg_catalog.monitoring.reprocessing_queue
                        SET processed = true, processed_timestamp = current_timestamp()
                        WHERE event_date = '{event_date}' AND processed = false
                    """)
        
        logger.info("✅ Late data handler streams started successfully")
        logger.info("📊 Monitoring:")
        logger.info("   - Raw events with 48-hour watermark")
        logger.info("   - Session aggregations with late data tolerance") 
        logger.info("   - Late data alerts and monitoring")
        logger.info("   - Automatic reprocessing queue")
        
        # Run for demo (5 minutes), then trigger reprocessing
        query_raw.awaitTermination(300)
        
        # Run reprocessing logic
        reprocess_affected_partitions()
        
    except Exception as e:
        logger.error(f"Error in late data handler: {str(e)}")
        raise
    finally:
        # Stop all queries
        for query in spark.streams.active:
            query.stop()
        spark.stop()

if __name__ == "__main__":
    late_data_handler()