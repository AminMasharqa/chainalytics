#!/usr/bin/env python3
from pyspark.sql import SparkSession

# Create Spark session with Iceberg support
spark = SparkSession.builder \
    .appName("ConvertToIceberg") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://chainalytics-minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg_catalog.type", "hadoop") \
    .config("spark.sql.catalog.iceberg_catalog.warehouse", "s3a://warehouse/iceberg/") \
    .getOrCreate()

print("🧊 CONVERTING PARQUET TO ICEBERG TABLES")
print("=" * 50)

# Your existing parquet tables
parquet_tables = [
    ("s3a://warehouse/chainalytics.db/bronze_products/*.parquet", "products"),
    ("s3a://warehouse/chainalytics.db/bronze_user_events/*.parquet", "user_events"),
    ("s3a://warehouse/chainalytics.db/bronze_user_posts/*.parquet", "user_posts"),
    ("s3a://warehouse/chainalytics.db/bronze_weather_data/*.parquet", "weather_data"),
]

def convert_parquet_to_iceberg(parquet_path, table_name):
    """Convert existing Parquet files to Iceberg table"""
    print(f"\n🔄 Converting {table_name}...")
    
    try:
        # Read existing Parquet data
        df = spark.read.parquet(parquet_path)
        print(f"✅ Read {df.count()} rows from {parquet_path}")
        
        # Create Iceberg table
        iceberg_path = f"s3a://warehouse/iceberg/{table_name}"
        
        # Write as Iceberg table
        df.write \
            .format("iceberg") \
            .mode("overwrite") \
            .option("path", iceberg_path) \
            .saveAsTable(f"iceberg_catalog.{table_name}")
        
        print(f"✅ Created Iceberg table: iceberg_catalog.{table_name}")
        print(f"📍 Location: {iceberg_path}")
        
        # Verify the conversion
        iceberg_df = spark.read.format("iceberg").load(iceberg_path)
        print(f"✅ Verified: {iceberg_df.count()} rows in Iceberg table")
        
        # Show sample data
        print(f"\n📊 Sample data from Iceberg table:")
        iceberg_df.show(5, False)
        
        return True
        
    except Exception as e:
        print(f"❌ Failed to convert {table_name}: {e}")
        return False

def show_iceberg_features(table_name):
    """Demonstrate Iceberg-specific features"""
    print(f"\n🧊 ICEBERG FEATURES for {table_name}")
    print("-" * 40)
    
    try:
        # Show table history
        print("📜 Table History:")
        spark.sql(f"SELECT * FROM iceberg_catalog.{table_name}.history").show(5, False)
        
        # Show table snapshots
        print("\n📸 Table Snapshots:")
        spark.sql(f"SELECT * FROM iceberg_catalog.{table_name}.snapshots").show(5, False)
        
        # Show table files
        print(f"\n📁 Table Files:")
        spark.sql(f"SELECT * FROM iceberg_catalog.{table_name}.files LIMIT 5").show(5, False)
        
    except Exception as e:
        print(f"⚠️ Could not show advanced features: {e}")

# Convert all tables
print("🚀 Starting conversion process...")
successful_conversions = []

for parquet_path, table_name in parquet_tables:
    success = convert_parquet_to_iceberg(parquet_path, table_name)
    if success:
        successful_conversions.append(table_name)

print(f"\n🎯 CONVERSION SUMMARY")
print("=" * 50)
print(f"✅ Successfully converted {len(successful_conversions)} tables:")
for table in successful_conversions:
    print(f"   - iceberg_catalog.{table}")

if successful_conversions:
    print(f"\n🧊 ICEBERG TABLE FEATURES DEMO")
    print("=" * 50)
    
    # Demo Iceberg features on first successful table
    demo_table = successful_conversions[0]
    show_iceberg_features(demo_table)
    
    print(f"\n💡 ICEBERG ADVANTAGES NOW AVAILABLE:")
    print("✅ Time travel queries")
    print("✅ Schema evolution")
    print("✅ Hidden partitioning")
    print("✅ Snapshot isolation")
    print("✅ Efficient deletes/updates")
    
    print(f"\n🔍 TRY THESE ICEBERG QUERIES:")
    print(f"-- Time travel")
    print(f"SELECT * FROM iceberg_catalog.{demo_table} TIMESTAMP AS OF '2025-07-23 10:00:00'")
    print(f"")
    print(f"-- Schema evolution")
    print(f"ALTER TABLE iceberg_catalog.{demo_table} ADD COLUMN new_column STRING")
    print(f"")
    print(f"-- Efficient updates")
    print(f"UPDATE iceberg_catalog.{demo_table} SET column_name = 'new_value' WHERE condition")

    print(f"\n📍 YOUR ICEBERG TABLES ARE NOW AT:")
    for table in successful_conversions:
        print(f"   s3a://warehouse/iceberg/{table}/")

print(f"\n✅ Conversion complete!")
spark.stop()