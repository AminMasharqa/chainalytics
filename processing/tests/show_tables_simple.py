#!/usr/bin/env python3
from pyspark.sql import SparkSession
import logging

# Suppress all Spark logs
logging.getLogger("pyspark").setLevel(logging.ERROR)
logging.getLogger("py4j").setLevel(logging.ERROR)
logging.getLogger("org").setLevel(logging.ERROR)
logging.getLogger("akka").setLevel(logging.ERROR)

# Create Spark session
spark = SparkSession.builder \
    .appName("ShowTableData") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://chainalytics-minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Discover all tables in the warehouse
warehouse_path = "s3a://warehouse/chainalytics/"
hadoop_conf = spark._jsc.hadoopConfiguration()
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
    spark._jvm.java.net.URI(warehouse_path), hadoop_conf)
path = spark._jvm.org.apache.hadoop.fs.Path(warehouse_path)

print("📊 SHOWING 10 ROWS FROM EACH TABLE")
print("=" * 50)

# Get all directories (tables) in the warehouse
file_statuses = fs.listStatus(path)
for status in file_statuses:
    if status.isDirectory():
        table_path = str(status.getPath()) + "/"
        table_name = status.getPath().getName()
        print(f"\n📋 {table_name.upper()}")
        print("-" * 40)
        
        try:
            df = spark.read.parquet(table_path)
            df.show(10, truncate=False)
            
        except Exception as e:
            print(f"❌ Could not read table: {e}")

print("\n✅ Done!")
spark.stop()