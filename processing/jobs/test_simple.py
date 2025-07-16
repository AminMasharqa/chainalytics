from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Simple Test") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hadoop") \
    .config("spark.sql.catalog.iceberg.warehouse", "s3a://warehouse/") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

print("=== Testing Spark + Iceberg + MinIO ===")

# Test 1: Read CSV from MinIO bucket
print("\n1. Reading sensor data CSV from MinIO bucket...")
sensor_df = spark.read.option("header", "true").option("inferSchema", "true").csv("s3a://test-bucket/sensor_data.csv")
sensor_df.show(5)

# Test 2: Create Iceberg namespace
print("\n2. Creating Iceberg namespace...")
spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.test")

# Test 3: Create Iceberg table for sensor data
print("\n3. Creating Iceberg table...")
spark.sql("""
    CREATE TABLE IF NOT EXISTS iceberg.test.sensor_data (
        timestamp TIMESTAMP,
        `living room temperature` DOUBLE,
        `living room humidity` DOUBLE,
        `living room motion` BOOLEAN,
        spaceId BOOLEAN -- adjust type if needed
    ) USING iceberg
""")

# Test 4: Write data to Iceberg table
print("\n4. Writing sensor data to Iceberg table...")
sensor_df.writeTo("iceberg.test.sensor_data").overwritePartitions()

# Test 5: Read back from Iceberg table
print("\n5. Reading back data from Iceberg table...")
result = spark.sql("SELECT * FROM iceberg.test.sensor_data")
result.show(5)

print("\n✅ All tests passed! Iceberg is working with MinIO and your sensor data.")

spark.stop()
