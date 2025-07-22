#!/usr/bin/env python3
from pyspark.sql import SparkSession
import logging

# Suppress Spark logs - only show ERROR level and above
logging.getLogger("pyspark").setLevel(logging.ERROR)
logging.getLogger("py4j").setLevel(logging.ERROR)
logging.getLogger("org").setLevel(logging.ERROR)
logging.getLogger("akka").setLevel(logging.ERROR)

# Create Spark session with corrected S3 configuration
spark = SparkSession.builder \
    .appName("ReadMinIOParquetData") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://chainalytics-minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

# Set Spark log level to ERROR (this suppresses INFO/WARN logs)
spark.sparkContext.setLogLevel("ERROR")

print("📊 READING PARQUET DATA FROM MINIO")
print("=" * 50)

# List of tables discovered from your output
table_paths = [
    "s3a://warehouse/chainalytics.db/bronze_api_logs/",
    "s3a://warehouse/chainalytics.db/bronze_products/", 
    "s3a://warehouse/chainalytics.db/bronze_user_events/",
    "s3a://warehouse/chainalytics.db/bronze_user_posts/",
    "s3a://warehouse/chainalytics.db/bronze_weather_data/"
]

def explore_table(table_path, table_name):
    """Show raw data from table without statistics"""
    print(f"\n📋 TABLE: {table_name}")
    print("=" * 60)
    
    try:
        # Read the parquet files directly
        df = spark.read.parquet(table_path)
        
        # Show column names
        print(f"📝 Columns: {', '.join(df.columns)}")
        print(f"📊 Total rows: {df.count()}")
        
        # Show raw data - increase rows shown and don't truncate
        print(f"\n🔍 RAW DATA:")
        df.show(50, truncate=False)
        
        return True
        
    except Exception as e:
        print(f"❌ Failed to read {table_name}: {e}")
        
        # Try alternative approaches
        try:
            print("🔄 Trying to list files in path...")
            hadoop_conf = spark._jsc.hadoopConfiguration()
            fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                spark._jvm.java.net.URI(table_path), hadoop_conf)
            path = spark._jvm.org.apache.hadoop.fs.Path(table_path)
            
            if fs.exists(path):
                file_statuses = fs.listStatus(path)
                print(f"📁 Files found in {table_path}:")
                for status in file_statuses:
                    file_name = status.getPath().getName()
                    file_size = status.getLen()
                    print(f"  - {file_name} ({file_size} bytes)")
                    
                    # Try to read individual parquet files
                    if file_name.endswith('.parquet'):
                        try:
                            individual_path = f"{table_path.rstrip('/')}/{file_name}"
                            df_individual = spark.read.parquet(individual_path)
                            print(f"  ✅ Successfully read {file_name}")
                            print(f"     📝 Columns: {', '.join(df_individual.columns)}")
                            print(f"     📊 Rows: {df_individual.count()}")
                            print(f"     🔍 RAW DATA:")
                            df_individual.show(50, truncate=False)
                            return True
                        except Exception as ie:
                            print(f"  ❌ Failed to read {file_name}: {ie}")
            else:
                print(f"❌ Path doesn't exist: {table_path}")
                
        except Exception as list_error:
            print(f"❌ Failed to list files: {list_error}")
        
        return False

# Method 1: Show raw data from all tables
print("\n📊 RAW DATA FROM ALL TABLES")
print("=" * 80)
for i, table_path in enumerate(table_paths):
    table_name = table_path.split('/')[-2]  # Extract table name from path
    success = explore_table(table_path, table_name)
    
    # Add separator between tables
    if i < len(table_paths) - 1:
        print("\n" + "🔸" * 60)

# # Method 2: Try reading with wildcard patterns
# print("\n2️⃣ METHOD 2: Reading with wildcard patterns")
# for table_path in table_paths:
#     table_name = table_path.split('/')[-2]
#     wildcard_path = f"{table_path.rstrip('/')}/*.parquet"
    
#     print(f"\n🔍 Trying wildcard: {wildcard_path}")
#     try:
#         df = spark.read.parquet(wildcard_path)
#         print(f"✅ Success! {table_name} - Rows: {df.count()}, Columns: {len(df.columns)}")
#         df.show(5)
#     except Exception as e:
#         print(f"❌ Wildcard failed for {table_name}: {e}")

# # Method 3: Try as Delta tables (if they're Delta format)
# print("\n3️⃣ METHOD 3: Trying as Delta tables")
# for table_path in table_paths:
#     table_name = table_path.split('/')[-2]
    
#     try:
#         df = spark.read.format("delta").load(table_path)
#         print(f"✅ Delta success! {table_name} - Rows: {df.count()}")
#         df.show(5)
#     except Exception as e:
#         print(f"❌ Delta failed for {table_name}: {e}")

# # Method 4: Raw S3 exploration
# print("\n4️⃣ METHOD 4: Raw S3 bucket exploration")
# try:
#     hadoop_conf = spark._jsc.hadoopConfiguration()
#     fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
#         spark._jvm.java.net.URI("s3a://warehouse/"), hadoop_conf)
    
#     def list_directory_recursive(path_str, depth=0):
#         if depth > 3:  # Prevent infinite recursion
#             return
            
#         path = spark._jvm.org.apache.hadoop.fs.Path(path_str)
#         if fs.exists(path) and fs.isDirectory(path):
#             file_statuses = fs.listStatus(path)
#             for status in file_statuses:
#                 indent = "  " * depth
#                 file_name = status.getPath().getName()
                
#                 if status.isDirectory():
#                     print(f"{indent}📁 {file_name}/")
#                     list_directory_recursive(str(status.getPath()), depth + 1)
#                 else:
#                     file_size = status.getLen()
#                     print(f"{indent}📄 {file_name} ({file_size} bytes)")
                    
#                     # If it's a parquet file, try to read it
#                     if file_name.endswith('.parquet') and depth <= 2:
#                         try:
#                             df = spark.read.parquet(str(status.getPath()))
#                             print(f"{indent}   ✅ Readable parquet: {df.count()} rows, {len(df.columns)} cols")
#                         except Exception as pe:
#                             print(f"{indent}   ❌ Parquet read error: {pe}")
    
#     print("\n🗂️ Complete S3 warehouse structure:")
#     list_directory_recursive("s3a://warehouse/")
    
# except Exception as e:
#     print(f"❌ S3 exploration error: {e}")

print("\n✅ Data exploration complete!")
spark.stop()