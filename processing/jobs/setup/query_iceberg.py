from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("QueryIceberg") \
    .config("spark.sql.catalog.warehouse", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.warehouse.type", "hadoop") \
    .config("spark.sql.catalog.warehouse.warehouse", "s3a://warehouse/") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://chainalytics-minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()


spark.sql("USE chainalytics.db")
spark.sql("SHOW TABLES").show()
