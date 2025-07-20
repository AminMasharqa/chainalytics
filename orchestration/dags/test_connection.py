#!/usr/bin/env python3
"""Simple Connection Test"""

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'test',
    'start_date': datetime(2025, 7, 20),
    'retries': 0,
}

DOCKER_CONFIG = {
    'image': 'chainalytics-spark-image',
    'network_mode': 'chainalytics-network',
    'auto_remove': 'success',
    'environment': {
        'SPARK_MASTER': 'spark://chainalytics-spark-master:7077',
        'MINIO_ENDPOINT': 'http://chainalytics-minio:9000',
        'MINIO_ACCESS_KEY': 'minioadmin',
        'MINIO_SECRET_KEY': 'minioadmin',
    },
}

dag = DAG('test_connections', default_args=default_args, schedule=None, catchup=False)

test_spark = DockerOperator(
    task_id='test_spark',
    command='''
python -c "
print('🔍 Testing Spark...')
from pyspark.sql import SparkSession

try:
    spark = SparkSession.builder \\
        .appName('Test') \\
        .master('spark://chainalytics-spark-master:7077') \\
        .getOrCreate()
    
    print('✅ Spark connected successfully!')
    print(f'Spark version: {spark.version}')
    
    # Test basic SQL
    spark.sql('SELECT 1 as test').show()
    spark.stop()
    
except Exception as e:
    print(f'❌ Spark failed: {e}')
    import traceback
    traceback.print_exc()
"
    ''',
    dag=dag,
    **DOCKER_CONFIG
)

test_minio = DockerOperator(
    task_id='test_minio',
    command='''
python -c "
print('🔍 Testing MinIO...')

try:
    import boto3
    from botocore.client import Config
    
    s3 = boto3.client(
        \"s3\",
        endpoint_url=\"http://chainalytics-minio:9000\",
        aws_access_key_id=\"minioadmin\",
        aws_secret_access_key=\"minioadmin\",
        config=Config(signature_version=\"s3v4\")
    )
    
    buckets = s3.list_buckets()
    print('✅ MinIO connected successfully!')
    print('Buckets:')
    for bucket in buckets[\"Buckets\"]:
        print(f\"  - {bucket[\"Name\"]}\")
    
    # Try to create chainalytics bucket if it doesn't exist
    bucket_names = [b[\"Name\"] for b in buckets[\"Buckets\"]]
    if \"chainalytics\" not in bucket_names:
        s3.create_bucket(Bucket=\"chainalytics\")
        print('✅ Created chainalytics bucket')
    else:
        print('✅ chainalytics bucket already exists')
        
except Exception as e:
    print(f'❌ MinIO failed: {e}')
    import traceback
    traceback.print_exc()
"
    ''',
    dag=dag,
    **DOCKER_CONFIG
)

test_spark >> test_minio