from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder \
    .appName("InitializeIcebergTable") \
    .config("spark.sql.catalog.warehouse", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.warehouse.type", "hadoop") \
    .config("spark.sql.catalog.warehouse.warehouse", "s3a://warehouse/") \
    .config("spark.sql.catalog.warehouse.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://chainalytics-minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .getOrCreate()

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType
from pyspark.sql import Row

dummy_data = [Row(
    api_source="dummy_api",
    date=datetime.today().date(),
    total_calls=0,
    avg_response_time_ms=0.0,
    success_rate=0.0,
    error_count=0,
    performance_grade="N/A",
    ingestion_timestamp=datetime.now()
)]

df = spark.createDataFrame(dummy_data)

df.writeTo("warehouse.chainalytics.silver_api_performance").append()

spark.stop()
