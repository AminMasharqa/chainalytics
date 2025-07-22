# 1. Create the file
cat > show_tables_simple.py << 'EOF'
from pyspark.sql import SparkSession

# Create Spark session without Hive (avoids Derby issues)
spark = SparkSession.builder \
    .appName("ShowIcebergTables") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "s3a://warehouse") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.catalogImplementation", "in-memory") \
    .getOrCreate()

print("🔍 SHOWING ICEBERG TABLES")
print("=" * 50)

tables = [
    "local.chainalytics.bronze_api_logs",
    "local.chainalytics.bronze_products", 
    "local.chainalytics.bronze_user_events",
    "local.chainalytics.bronze_user_posts",
    "local.chainalytics.bronze_weather_data"
]

for table in tables:
    print(f"\n📊 {table}")
    print("-" * 40)
    
    try:
        count_df = spark.sql(f"SELECT COUNT(*) as count FROM {table}")
        count = count_df.collect()[0]['count']
        print(f"Total rows: {count:,}")
        
        print("\nSchema:")
        spark.sql(f"DESCRIBE {table}").show()
        
        print(f"\nSample data (first 10 rows):")
        spark.sql(f"SELECT * FROM {table} LIMIT 10").show(truncate=False)
        
    except Exception as e:
        print(f"❌ Error: {e}")

print("\n✅ Demo complete!")
spark.stop()
EOF

# 2. Copy to container and run
docker cp show_tables_simple.py chainalytics-spark-master:/tmp/
docker exec chainalytics-spark-master /opt/spark/bin/spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  /tmp/show_tables_simple.py